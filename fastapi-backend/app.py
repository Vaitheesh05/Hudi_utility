from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from datetime import datetime, timedelta
import json
import os
import re
import subprocess

# Database configuration
DATABASE_URL = "postgresql://hudi_user:password@localhost/hudi_bootstrap_db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class HudiTransaction(Base):
    __tablename__ = "hudi_transactions"
    
    id = Column(Integer, primary_key=True, index=True)
    transaction_id = Column(String, unique=True, index=True)
    status = Column(String, default="PENDING")  # PENDING, FAILED, SUCCESS
    transaction_data = Column(Text, nullable=False)
    start_time = Column(DateTime, default=datetime.utcnow)
    end_time = Column(DateTime, nullable=True)
    app_id = Column(String, nullable=True)

Base.metadata.create_all(bind=engine)

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Consider specifying allowed origins for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Define input schema
class HudiBootstrapRequest(BaseModel):
    data_file_path: str
    hudi_table_name: str
    key_field: str
    precombine_field: str
    partition_field: Optional[str] = None
    hudi_table_type: str  # COPY_ON_WRITE or MERGE_ON_READ
    write_operation: str  # insert or upsert
    output_path: str
    spark_config: Optional[dict] = None  # Optional spark configurations
    bootstrap_type: str  # FULL_RECORD or METADATA_ONLY
    partition_regex: Optional[str] = None  # Optional regex for partitions

@app.post("/bootstrap_hudi/")
def bootstrap_hudi(request: HudiBootstrapRequest, db: Session = Depends(get_db)):
    transaction_id = f"{request.hudi_table_name}-{int(datetime.utcnow().timestamp())}"
    transaction = HudiTransaction(
        transaction_id=transaction_id,
        status="PENDING",
        transaction_data=json.dumps(request.dict()),
        start_time=datetime.utcnow(),
    )
    db.add(transaction)
    db.commit()
    db.refresh(transaction)

    current_directory = os.path.dirname(os.path.abspath(__file__))
    pyspark_script_path = os.path.join(current_directory, "pyspark_script.py")

    try:
        # Build the spark-submit command
        spark_submit_command = [
            "spark-submit",
            "--master", "local"
        ]
        
        # Add Spark configurations
        if request.spark_config:
            for key, value in request.spark_config.items():
                spark_submit_command.append(f"--conf")
                spark_submit_command.append(f"{key}={value}")
        
        spark_submit_command.append(pyspark_script_path)
        # Append additional parameters
        spark_submit_command.append(f"--data-file-path={request.data_file_path}")
        spark_submit_command.append(f"--hudi-table-name={request.hudi_table_name}")
        spark_submit_command.append(f"--key-field={request.key_field}")
        spark_submit_command.append(f"--precombine-field={request.precombine_field}")
        if request.partition_field:
            spark_submit_command.append(f"--partition-field={request.partition_field}")

        spark_submit_command.append(f"--hudi-table-type={request.hudi_table_type}")
        spark_submit_command.append(f"--write-operation={request.write_operation}")
        spark_submit_command.append(f"--output-path={request.output_path}")
        spark_submit_command.append(f"--bootstrap-type={request.bootstrap_type}")

        if request.partition_regex:
            spark_submit_command.append(f"--partition-regex={request.partition_regex}")

        # Call Spark-submit
        result = subprocess.run(spark_submit_command, capture_output=True, text=True)
        
        app_id_match = re.search(r'local-\d{13}', result.stderr)
        if app_id_match:
            app_id = app_id_match.group(0)
            transaction.app_id = app_id
            db.commit()

        # Modify the error handling section in the bootstrap_hudi function
        if result.returncode != 0:
            if os.path.exists("pyspark_script.log"):
                with open("pyspark_script.log", "r") as f:
                    error_log = f.read()
                os.remove("pyspark_script.log")
            else:
                error_log = ""

            error_message = parse_error_log(error_log)
            transaction.status = "FAILED"
            transaction.end_time = datetime.utcnow()
            db.commit()
            return JSONResponse(status_code=500, content={"message": error_message, "detail": error_log})

        if os.path.exists("pyspark_script.log"):
            os.remove("pyspark_script.log")

        transaction.end_time = datetime.utcnow()
        transaction.status = "SUCCESS"
        db.commit()

        return {"message": "Hudi table bootstrapped successfully."}
    
    except Exception as e:
        transaction.end_time = datetime.utcnow()
        transaction.status = "FAILED"
        db.commit()
        
        return JSONResponse(status_code=500, content={"message": "Error while Running Spark Submit", "detail": str(e)})

@app.get("/bootstrap_history/")
def get_bootstrap_history(start_date: Optional[str] = None, end_date: Optional[str] = None, transaction_id: Optional[str] = None, db: Session = Depends(get_db)):
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

def check_hive_table(table_name: str) -> Optional[str]:
    """Check if the specified Hive table exists and return its name."""
    try:
        command = f'hive -e "SHOW TABLES LIKE \'{table_name}\';"'
        result = subprocess.run(command, shell=True, capture_output=True, text=True)

        if result.returncode != 0:
            raise Exception(f"Hive command failed: {result.stderr}")

        if table_name in result.stdout.splitlines():
            return table_name
        else:
            return None
    except Exception as e:
        print("Error checking Hive table:", e)
        return None

def get_hdfs_location_from_hive_table(table_name: str) -> Optional[str]:
    """Retrieve the HDFS location of the specified Hive table."""
    try:
        command = f'hive -e "DESCRIBE FORMATTED {table_name};"'
        result = subprocess.run(command, shell=True, capture_output=True, text=True)

        if result.returncode != 0:
            raise Exception(f"Hive command failed: {result.stderr}")

        # Parse the output to find the location
        for line in result.stdout.splitlines():
            if "Location:" in line:
                return line.split(":", 1)[1].strip()  # Return the HDFS location

        return None  # Location not found
    except Exception as e:
        print("Error retrieving HDFS location from Hive table:", e)
        return None

def check_hdfs_partition(hdfs_path: str) -> bool:
    """Check if the specified HDFS path is partitioned by looking for subdirectories."""
    try:
        # List the contents of the HDFS directory
        result = subprocess.run(['hdfs', 'dfs', '-ls', hdfs_path], capture_output=True, text=True)

        if result.returncode != 0:
            raise Exception(f"HDFS command failed: {result.stderr}")

        # Check for directories
        has_partitions = False
        for line in result.stdout.splitlines():
            # Each line typically has a format of:
            # permissions | number of replicas | owner | group | size | date | time | path
            parts = line.split()
            if len(parts) > 0:
                # The path is always the last part
                path = parts[-1]
                # Check if it's a directory by looking for the presence of 'd' at the start of the permissions
                if parts[0].startswith('d'):
                    has_partitions = True
                    break

        return has_partitions  # Return True if any directories are found
    except Exception as e:
        print("Error checking HDFS partition:", e)
        return False

class PathOrTableCheck(BaseModel):
    hudi_table_name: str

@app.post("/check_path_or_table/")
async def check_path_or_table(data: PathOrTableCheck, db: Session = Depends(get_db)):
    """Check if the provided path is a HDFS path or a Hive table, and determine partitioning."""
    path_or_table = data.hudi_table_name  # Assuming this is the table name

    if path_or_table.startswith("hdfs://"):
        is_partitioned = check_hdfs_partition(path_or_table)
        return {"isPartitioned": is_partitioned, "tableName": None,"hdfsLocation": path_or_table}
    else:
        table_name = check_hive_table(path_or_table)
        if table_name:
            hdfs_location = get_hdfs_location_from_hive_table(table_name)
            is_partitioned = check_hdfs_partition(hdfs_location) if hdfs_location else False
            return {"isPartitioned": is_partitioned, "tableName": table_name,"hdfsLocation": hdfs_location}
        else:
            raise HTTPException(status_code=404, detail="Hive table not found.")


# Run the FastAPI app using `uvicorn`
# Uncomment the next line to run the server locally.
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)
