from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from datetime import datetime, timedelta
import pydoop.hdfs as hdfs
from pyhive import hive
import json
import os
import re
import subprocess
import threading
from functools import lru_cache
from typing import Set, Tuple, List
from dotenv import load_dotenv
from fastapi import WebSocket, WebSocketDisconnect
import asyncio
from weakref import WeakValueDictionary



# Load environment variables from .env file
load_dotenv()

# Database configuration from .env
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://hudi_user:password@localhost/hudi_bootstrap_db")

# Hive and HDFS configuration from .env
HIVE_HOST = os.getenv("HIVE_HOST", "localhost")
HIVE_PORT = int(os.getenv("HIVE_PORT", 10000))

# Initialize the database engine and sessionmaker
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Model for Hudi Transactions
class HudiTransaction(Base):
    __tablename__ = "hudi_transactions"
    
    id = Column(Integer, primary_key=True, index=True)
    transaction_id = Column(String, unique=True, index=True)
    status = Column(String, default="PENDING")  # PENDING, FAILED, SUCCESS
    transaction_data = Column(Text, nullable=False)
    start_time = Column(DateTime, default=datetime.utcnow)
    end_time = Column(DateTime, nullable=True)
    app_id = Column(String, nullable=True)
    error_log = Column(Text, nullable=True)  # Store error log if any

Base.metadata.create_all(bind=engine)

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# FastAPI app initialization
app = FastAPI()

# CORS Middleware setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Consider specifying allowed origins for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

TRANSACTION_TIMEOUT_MINUTES = 60

# Runaway transaction error message
RUNAWAY_ERROR_MESSAGE = "Transaction timeout or runaway process."

# Add an event handler for startup to check and update runaway transactions
@app.on_event("startup")
async def check_runaway_transactions():
    try:
        # Get the current time
        current_time = datetime.utcnow()

        # Open a database session
        with SessionLocal() as db:
            # Query for all pending transactions that have been running for too long
            pending_transactions = db.query(HudiTransaction).filter(
                HudiTransaction.status == "PENDING",
                HudiTransaction.start_time <= current_time - timedelta(minutes=TRANSACTION_TIMEOUT_MINUTES)
            ).all()

            # Loop through the pending transactions and update their status to 'FAILED'
            for transaction in pending_transactions:
                transaction.status = "FAILED"
                transaction.error_log = RUNAWAY_ERROR_MESSAGE

                # Save the transaction status update in the database
                db.add(transaction)
            db.commit()  # Commit all changes at once

        # Log the action
        print(f"Updated {len(pending_transactions)} runaway transactions to FAILED.")

    except Exception as e:
        print(f"Error during startup processing runaway transactions: {str(e)}")


# Pydantic model for request validation
class HudiBootstrapRequest(BaseModel):
    data_file_path: str
    hudi_table_name: str
    key_field: str
    precombine_field: str
    partition_field: Optional[str] = None
    hudi_table_type: str  # COPY_ON_WRITE or MERGE_ON_READ
    output_path: str
    spark_config: Optional[dict] = None  # Optional spark configurations
    bootstrap_type: str  # FULL_RECORD or METADATA_ONLY
    partition_regex: Optional[str] = None  # Optional regex for partitions

# Run spark-submit in a separate thread to bootstrap the Hudi table
def run_spark_submit(request: HudiBootstrapRequest, transaction: HudiTransaction):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    pyspark_script_path = os.path.join(current_directory, "pyspark_script.py")
    log_file_path = os.path.join(current_directory, f"pyspark_script_{transaction.transaction_id}.log")

    # Build the spark-submit command
    spark_submit_command = [
        "spark-submit",
        "--master", "local"  # Use SPARK_MASTER from .env if needed
    ]
    
    # Add Spark configurations
    if request.spark_config:
        for key, value in request.spark_config.items():
            spark_submit_command.append(f"--conf")
            spark_submit_command.append(f"{key}={value}")
    
    spark_submit_command.append(pyspark_script_path)
    spark_submit_command.append(f"--data-file-path={request.data_file_path}")
    spark_submit_command.append(f"--hudi-table-name={request.hudi_table_name}")
    spark_submit_command.append(f"--key-field={request.key_field}")
    spark_submit_command.append(f"--precombine-field={request.precombine_field}")
    if request.partition_field:
        spark_submit_command.append(f"--partition-field={request.partition_field}")
    spark_submit_command.append(f"--hudi-table-type={request.hudi_table_type}")
    spark_submit_command.append(f"--output-path={request.output_path}")
    spark_submit_command.append(f"--bootstrap-type={request.bootstrap_type}")

    if request.partition_regex:
        spark_submit_command.append(f"--partition-regex={request.partition_regex}")

    spark_submit_command.append(f"--log-file={log_file_path}")

    # Call Spark-submit
    try:
        process = subprocess.Popen(spark_submit_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
    except Exception as e:
        print(f"Error running spark-submit: {e}")
        transaction.status = "FAILED"
        transaction.error_log = str(e)
        transaction.end_time = datetime.utcnow()
        asyncio.run(save_transaction(transaction))
        return

    # Read log file after process completion
    with open(log_file_path, "r") as log_file:
        error_log = log_file.read()
    
    # Handle the process result and update transaction status
        if process.returncode != 0:
            transaction.status = "FAILED"
            transaction.error_log = error_log
        else:
            transaction.status = "SUCCESS"
            transaction.error_log = error_log

    # Clean up the log file
    os.remove(log_file_path)

    # Update transaction in the database and send WebSocket updates
    transaction.end_time = datetime.utcnow()
    asyncio.run(save_transaction(transaction))


async def save_transaction(transaction: HudiTransaction):
    try:
        with SessionLocal() as db:
            db.add(transaction)
            db.commit()
            db.refresh(transaction)
            
            # Send updates via WebSocket (only for failed or successful status)
            if transaction.status in ["FAILED", "SUCCESS"]:
                # Make sure we're in an async event loop
                await send_transaction_status_update(transaction.transaction_id, transaction.status, transaction.error_log)
    except Exception as e:
        print(f"Error saving transaction: {e}")

# Endpoint to start the Hudi bootstrap process
@app.post("/bootstrap_hudi/")
async def bootstrap_hudi(request: HudiBootstrapRequest, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    transaction_id = f"{request.hudi_table_name}-{int(datetime.utcnow().timestamp())}"
    transaction = HudiTransaction(
        transaction_id=transaction_id,
        status="PENDING",
        transaction_data=json.dumps(request.dict()),
        start_time=datetime.utcnow(),
    )
    background_tasks.add_task(save_transaction, transaction)

    # Start the Spark submit task in a separate thread using a background task (or thread pool)
    def run_spark_task():
        run_spark_submit(request, transaction)
    
    # Run the Spark submit in the background (threaded)
    background_tasks.add_task(run_spark_task)

    return {"transaction_id": transaction.transaction_id, "message": "Bootstrapping started."}

# Endpoint to get bootstrap history
@app.get("/bootstrap_history/")
async def get_bootstrap_history(start_date: Optional[str] = None, end_date: Optional[str] = None, transaction_id: Optional[str] = None, db: Session = Depends(get_db)):
    query = db.query(HudiTransaction)
    
    if transaction_id:
        query = query.filter(HudiTransaction.transaction_id.like(f"%{transaction_id}%"))
    
    if start_date:
        query = query.filter(HudiTransaction.start_time >= datetime.fromisoformat(start_date))
    
    if end_date:
        end_date_obj = datetime.fromisoformat(end_date)
        end_time = end_date_obj + timedelta(days=1)
        query = query.filter(HudiTransaction.start_time < end_time)
    
    transactions = query.order_by(HudiTransaction.start_time.desc()).all()
    return transactions

active_connections = WeakValueDictionary()

# WebSocket endpoint to listen for real-time status updates
@app.websocket("/ws/{transaction_id}/")
async def websocket_endpoint(websocket: WebSocket, transaction_id: str):
    """WebSocket endpoint to listen for real-time status updates."""
    await websocket.accept()
    
    # Add the connection to the active connections dictionary
    active_connections[transaction_id] = websocket

    try:
        # Listen for messages from the client (if any)
        while True:
            message = await websocket.receive_text()
            print(f"Received message: {message}")
            # Handle any messages if needed
    except WebSocketDisconnect:
        # Remove the connection from the active connections when it disconnects
        del active_connections[transaction_id]
        print(f"Client disconnected: {transaction_id}")
    except Exception as e:
        # Handle any unexpected exceptions
        print(f"Error with WebSocket: {e}")
        del active_connections[transaction_id]
        await websocket.close()


async def send_transaction_status_update(transaction_id: str, status: str, error_log: str = None):
    """Send a real-time update via WebSocket to the client."""
    # Check if there is an active WebSocket connection for this transaction
    if transaction_id in active_connections:
        websocket = active_connections[transaction_id]
        error_message = None
        record_counts = {"input_count": None, "hudi_count": None}

        # Parse the error_log if available
        if error_log:
            error_message = parse_error_log(error_log)
            record_counts = extract_record_counts_from_log(error_log)

        await websocket.send_json({
            "transaction_id": transaction_id,
            "status": status,
            "error_log": error_log,
            "error_message": error_message,
            "record_counts": record_counts
        })



# Helper function to parse error logs
def parse_error_log(error_log: str) -> str:
    """Parse error log for meaningful messages."""
    if "Configuration Error:" in error_log:
        return "Configuration Error: " + error_log.split("Configuration Error:")[1].strip().split("\n")[0]
    elif "Permission Denied:" in error_log:
        return "Access Permission Error: " + error_log.split("Permission Denied:")[1].strip().split("\n")[0]
    elif "Unsupported file format:" in error_log:
        return "Unsupported File Format: Only .parquet and .orc files are supported."
    else:
        return "An Unexpected error occurred during Hudi table Bootstrap"

def extract_record_counts_from_log(error_log: str) -> dict:
    """Extract the record counts from the error_log."""
    
    if error_log is None:
        # Optionally log a warning here or return a default value
        return {"input_count": None, "hudi_count": None}

    if not isinstance(error_log, str):
        raise ValueError("Expected error_log to be a string, but got {0}".format(type(error_log)))

    record_counts = {"input_count": None, "hudi_count": None}

    # Regular expressions to extract the record counts from the error log
    input_count_match = re.search(r"Total records in Input DataFrame: (\d+)", error_log)
    hudi_count_match = re.search(r"Total records in Hudi table: (\d+)", error_log)

    if input_count_match:
        record_counts["input_count"] = int(input_count_match.group(1))

    if hudi_count_match:
        record_counts["hudi_count"] = int(hudi_count_match.group(1))

    return record_counts

# Pydantic model to check path or table
class PathOrTableCheck(BaseModel):
    hudi_table_name: str

hive_conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT)

@app.post("/check_path_or_table/")
async def check_path_or_table(data: PathOrTableCheck, db: Session = Depends(get_db)):
    path_or_table = data.hudi_table_name

    if path_or_table.startswith("hdfs://") or path_or_table.startswith("file://"):
        # Validate HDFS path and get partition fields
        return check_hdfs(path_or_table)
    else:
        # Check if it's a Hive table and extract partition fields
        return check_hive(path_or_table)


def check_hive(table_name: str) -> dict:
    """Check if the given Hive table exists, and get partition info."""
    table_name = check_hive_table(table_name)
    if table_name:
        hdfs_location = get_hdfs_location_from_hive_table(table_name)
        if hdfs_location:
            is_partitioned, partition_fields, _, _ = check_hdfs(hdfs_location)
            return {
                "isPartitioned": is_partitioned,
                "partitionFields": partition_fields,
                "tableName": table_name,
                "hdfsLocation": hdfs_location
            }
        else:
            raise HTTPException(status_code=404, detail="HDFS location for the Hive table not found.")
    else:
        raise HTTPException(status_code=404, detail="Hive table not found.")


def check_hdfs(hdfs_path: str) -> dict:
    """Check if the HDFS path contains partitioned data."""
    is_partitioned, partition_fields = scan_hdfs_directory(hdfs_path)
    return {
        "isPartitioned": is_partitioned,
        "partitionFields": partition_fields,
        "tableName": None,
        "hdfsLocation": hdfs_path
    }


def check_hive_table(table_name: str) -> Optional[str]:
    """Check if the specified Hive table exists."""
    try:
        with hive_conn.cursor() as cursor:
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            tables = cursor.fetchall()

        if tables and table_name in [t[0] for t in tables]:
            return table_name
        return None
    except Exception as e:
        print("Error checking Hive table:", e)
        return None


def get_hdfs_location_from_hive_table(table_name: str) -> Optional[str]:
    """Retrieve the HDFS location of the specified Hive table."""
    try:
        with hive_conn.cursor() as cursor:
            cursor.execute(f"DESCRIBE FORMATTED {table_name}")
            rows = cursor.fetchall()

            for row in rows:
                if isinstance(row, tuple) and row:
                    # Check for 'Location:' first
                    if "Location:" in row[0]:
                        return row[1].strip()
                    # Also check for 'path' under Storage Desc Params
                    if "path" in row[0].lower():
                        return row[1].strip()
        return None
    except Exception as e:
        print("Error retrieving HDFS location from Hive table:", e)
        return None


def scan_hdfs_directory(hdfs_path: str) -> Tuple[bool, str]:
    """Scan the HDFS directory to find partition fields."""
    VALID_FORMATS = {'.parquet', '.orc'}

    @lru_cache(maxsize=128)
    def is_valid_file(filename: str) -> bool:
        """Check if a file has a valid format."""
        return any(filename.endswith(fmt) for fmt in VALID_FORMATS)

    def extract_partition_fields_from_path(path: str) -> List[str]:
        """Extract partition fields from a directory path (like year=2020/month=01)."""
        pattern = r"([^/]+)=([^/]+)"
        return [key for key, _ in re.findall(pattern, path)]

    def scan_directory(path: str) -> Tuple[bool, List[str]]:
        """Recursively scan directories to find partition fields."""
        try:
            contents = hdfs.ls(path)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error reading HDFS directory {path}: {str(e)}")

        has_partitions = False
        partition_fields = []  # Use list to maintain order

        for item in contents:
            item_path = hdfs.path.join(path, item)
            relative_path = item_path[len(hdfs_path):].lstrip('/')

            if hdfs.path.isdir(item_path):
                # Check if it's a partition directory by examining its structure
                partition_field = extract_partition_fields_from_path(relative_path)
                if partition_field:
                    has_partitions = True
                    for field in partition_field:
                        if field not in partition_fields:  # Avoid duplicates
                            partition_fields.append(field)
                # Recursively check subdirectories
                sub_partitions, sub_partition_fields = scan_directory(item_path)
                has_partitions = has_partitions or sub_partitions
                # Add partition fields found in subdirectories
                for field in sub_partition_fields:
                    if field not in partition_fields:
                        partition_fields.append(field)

            elif hdfs.path.isfile(item_path) and is_valid_file(item):
                partition_field = extract_partition_fields_from_path(relative_path)
                if partition_field:
                    has_partitions = True
                    for field in partition_field:
                        if field not in partition_fields:
                            partition_fields.append(field)

        return has_partitions, partition_fields

    try:
        if not hdfs.path.exists(hdfs_path):
            raise HTTPException(status_code=404, detail=f"HDFS path does not exist: {hdfs_path}")

        has_partitions, partition_fields = scan_directory(hdfs_path)
        return has_partitions, ",".join(partition_fields) if has_partitions else None
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


def get_partition_fields_from_hive_table(table_name: str) -> Optional[str]:
    """Get partition fields for a Hive table."""
    try:
        with hive_conn.cursor() as cursor:
            cursor.execute(f"DESCRIBE FORMATTED {table_name}")
            rows = cursor.fetchall()

        partition_found = False
        partition_fields = []  # Use list to maintain order

        for row in rows:
            if isinstance(row, tuple) and row:
                row_content = row[0].strip()

                if "Partition Information" in row_content:
                    partition_found = True
                elif partition_found:
                    if not row_content:
                        break
                    if not row_content.startswith("#") and row_content:
                        partition_fields.append(row_content.split()[0])

        return partition_fields if partition_fields else None
    except Exception as e:
        print(f"Error retrieving partition fields for Hive table {table_name}: {e}")
        return None


