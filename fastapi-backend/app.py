from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import subprocess
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from datetime import datetime
import uuid
import json


# Database configuration
DATABASE_URL = "postgresql://hudi_user:password@localhost/hudi_bootstrap_db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class HudiTransaction(Base):
    __tablename__ = "hudi_transactions"
    
    id = Column(Integer, primary_key=True, index=True)
    transaction_id = Column(String, default=lambda: str(uuid.uuid4()), unique=True, index=True)
    status = Column(String, default="PENDING")  # PENDING, FAILED, SUCCESS
    log = Column(Text, nullable=True)
    transaction_data = Column(Text, nullable=False)
    start_time = Column(DateTime, default=datetime.utcnow)
    end_time = Column(DateTime, nullable=True)

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
    allow_origins=["*"],
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
    partition_field: str
    hudi_table_type: str  # COPY_ON_WRITE or MERGE_ON_READ
    write_operation: str  # insert or upsert
    output_path: str
    spark_config: Optional[dict] = None  # Optional spark configurations
    schema_validation: bool = False  # Changed to boolean
    dry_run: bool = False  # Changed to boolean
    bootstrap_type: str  # FULL_RECORD or METADATA_ONLY
    partition_regex: Optional[str] = None  # Optional regex for partitions

# Define the API endpoint
@app.post("/bootstrap_hudi/")
def bootstrap_hudi(request: HudiBootstrapRequest,db: Session = Depends(get_db)):
    try:
        # Build the spark-submit command
        transaction = HudiTransaction(
            status="PENDING",
            transaction_data=json.dumps(request.dict()),
            start_time=datetime.utcnow(),
        )
        db.add(transaction)
        db.commit()
        db.refresh(transaction)
        spark_submit_command = [
            "spark-submit",
            "--master", "local",  # or the appropriate cluster manager URL
            "--conf", f"spark.executor.memory={request.spark_config.get('spark.executor.memory', '2g') if request.spark_config else '2g'}",
            "/home/labuser/Desktop/Persistant_Folder/utility/fastapi-backend/pyspark_script.py",  # The path to your PySpark script
            f"--data-file-path={request.data_file_path}",
            f"--hudi-table-name={request.hudi_table_name}",
            f"--key-field={request.key_field}",
            f"--precombine-field={request.precombine_field}",
            f"--partition-field={request.partition_field}",
            f"--hudi-table-type={request.hudi_table_type}",
            f"--write-operation={request.write_operation}",
            f"--output-path={request.output_path}",
            f"--bootstrap-type={request.bootstrap_type}",
            f"--partition-regex={request.partition_regex}" if request.partition_regex else ""
        ]
	
        if request.schema_validation:
            spark_submit_command.append("--enable-schema-validation")

        if request.dry_run:
            spark_submit_command.append("--dry-run")

        # Call Spark-submit
        result = subprocess.run(spark_submit_command, capture_output=True, text=True)

        if result.returncode != 0:
            raise Exception(result.stderr)  # Raise error if subprocess fails

        transaction.end_time = datetime.utcnow()
        if result.returncode == 0:
            transaction.status = "SUCCESS"
            transaction.log = result.stdout
        else:
            transaction.status = "FAILED"
            transaction.log = result.stderr
        
        db.commit()

        return {"message": "Hudi table bootstrapped successfully."}
    
    except Exception as e:
        transaction.end_time = datetime.utcnow()
        transaction.status = "FAILED"
        transaction.log = str(e)
        db.commit()
        raise HTTPException(status_code=500, detail=str(e))

# Add any other endpoints as needed
@app.get("/bootstrap_history/")
def get_bootstrap_history(db: Session = Depends(get_db)):
    transactions = db.query(HudiTransaction).order_by(HudiTransaction.start_time.desc()).all()
    return transactions

