from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import subprocess
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins, modify if needed to be more restrictive
    allow_credentials=True,
    allow_methods=["*"],  # Allows all HTTP methods (POST, GET, OPTIONS, etc.)
    allow_headers=["*"],  # Allows all headers
)

# Define input schema
class HudiBootstrapRequest(BaseModel):
    parquet_file_path: str
    hudi_table_name: str
    key_field: str
    precombine_field: str
    partition_field: str
    hudi_table_type: str  # COPY_ON_WRITE or MERGE_ON_READ
    write_operation: str  # insert or upsert
    output_path: str
    spark_config: dict = None  # Optional spark configurations
    schema_validation: str = "no"  # yes or no
    dry_run: str = "no"  # yes or no
    bootstrap_type: str  # FULL_RECORD or METADATA_ONLY
    partition_regex: str  # regex for partitions

# Define the API endpoint
@app.post("/bootstrap_hudi/")
def bootstrap_hudi(request: HudiBootstrapRequest):
    try:
        # Build the spark-submit command
        spark_submit_command = [
            "spark-submit",
            "--class", "org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer",
            "--master", "local",
            "--conf", f"spark.executor.memory={request.spark_config.get('spark.executor.memory', '2g')}",
            f"--source-class {request.parquet_file_path}",
            f"--target-table {request.hudi_table_name}",
            f"--key-generator-class {request.key_field}",
            f"--precombine-key {request.precombine_field}",
            f"--partition-path-field {request.partition_field}",
            f"--table-type {request.hudi_table_type}",
            f"--operation {request.write_operation}",
            f"--target-base-path {request.output_path}",
            f"--hoodie.bootstrap.mode {request.bootstrap_type}",
            f"--hoodie.bootstrap.partition.regex {request.partition_regex}"
        ]

        if request.schema_validation == "yes":
            spark_submit_command.append("--enable-schema-validation")

        if request.dry_run == "yes":
            spark_submit_command.append("--dry-run")

        # Call Spark-submit
        result = subprocess.run(spark_submit_command, capture_output=True, text=True)

        if result.returncode != 0:
            raise HTTPException(status_code=500, detail=f"Spark-submit failed: {result.stderr}")

        return {"message": "Hudi table bootstrapped successfully", "output": result.stdout}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during bootstrapping: {str(e)}")

# To run FastAPI: `uvicorn filename:app --reload`

