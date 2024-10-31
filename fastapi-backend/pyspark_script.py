from pyspark.sql import SparkSession
import argparse
from pathlib import Path
import pydoop.hdfs as hdfs
import logging
import os
import sys
import traceback

# Set up logging to file with more detailed formatting
logging.basicConfig(
    filename="pyspark_script.log", 
    level=logging.ERROR, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def validate_input_arguments(args):
    """Validate input arguments before processing."""
    errors = []
    
    # Comprehensive input validation
    try:
        # Check data file path
        if not args.data_file_path:
            errors.append("Data file path is missing or empty.")
        elif not hdfs.path.exists(args.data_file_path):
            # Additional diagnostics for HDFS path issues
            try:
                hdfs.ls(args.data_file_path)
            except Exception as e:
                errors.append(f"HDFS access error: {str(e)}")
            errors.append(f"HDFS path does not exist: {args.data_file_path}")
        
        # More robust validation checks
        if not args.hudi_table_name or not args.hudi_table_name.strip():
            errors.append("Hudi table name is required and cannot be empty.")
        
        if not args.key_field or not args.key_field.strip():
            errors.append("Key field is required for record identification and cannot be empty.")
        
        # Validate table type with more context
        valid_table_types = ["COPY_ON_WRITE", "MERGE_ON_READ"]
        if args.hudi_table_type not in valid_table_types:
            errors.append(f"Invalid Hudi table type. Supported types are: {', '.join(valid_table_types)}")
        
        # Write operation validation
        valid_write_ops = ["insert", "upsert"]
        if args.write_operation not in valid_write_ops:
            errors.append(f"Invalid write operation. Supported operations are: {', '.join(valid_write_ops)}")
        
        # Output path validation
        if not args.output_path or not args.output_path.strip():
            errors.append("Output path is required and cannot be empty.")
        
        # Bootstrap type validation
        valid_bootstrap_types = ["FULL_RECORD", "METADATA_ONLY"]
        if args.bootstrap_type not in valid_bootstrap_types:
            errors.append(f"Invalid bootstrap type. Supported types are: {', '.join(valid_bootstrap_types)}")
    
    except Exception as general_error:
        errors.append(f"Unexpected validation error: {str(general_error)}")
    
    if errors:
        # Raise a detailed validation error
        raise ValueError("\n".join(errors))

def get_first_file_extension(hdfs_path):
    try:
        # Check if path exists before attempting to list
        if not hdfs.path.exists(hdfs_path):
            raise ValueError(f"HDFS path does not exist: {hdfs_path}")
        
        files = hdfs.ls(hdfs_path)
        
        # Handle empty directory case
        if not files:
            raise ValueError(f"No files found in the specified HDFS path: {hdfs_path}")
        
        for item in files:
            if hdfs.path.isfile(item):
                ext = Path(item).suffix.lower()
                if ext:
                    return ext
            elif hdfs.path.isdir(item):
                try:
                    file_extension = get_first_file_extension(item)
                    if file_extension:
                        return file_extension
                except ValueError:
                    # Skip directories that cause issues
                    continue
        
        # If no extensions found
        raise ValueError(f"No supported file extensions found in path: {hdfs_path}")
    
    except Exception as e:
        # Log the full traceback for debugging
        logging.error(f"Error in get_first_file_extension: {traceback.format_exc()}")
        raise ValueError(f"Error processing HDFS path {hdfs_path}: {str(e)}")

# Parse the command-line arguments
parser = argparse.ArgumentParser(description="Bootstrap Hudi Table using DataSource Writer")
parser.add_argument("--data-file-path", required=True)
parser.add_argument("--hudi-table-name", required=True)
parser.add_argument("--key-field", required=True)
parser.add_argument("--precombine-field", required=True)
parser.add_argument("--partition-field", required=True)
parser.add_argument("--hudi-table-type", required=True)
parser.add_argument("--write-operation", required=True)
parser.add_argument("--output-path", required=True)
parser.add_argument("--bootstrap-type", required=True)
parser.add_argument("--partition-regex", required=True)
args = parser.parse_args()

spark = None
try:
    # Validate input arguments first
    validate_input_arguments(args)

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Hudi Bootstrap") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    # Determine file extension and read accordingly
    try:
        file_extension = get_first_file_extension(args.data_file_path)
    except ValueError as ext_error:
        logging.error(f"File extension detection error: {str(ext_error)}")
        raise
    
    # Specific file format handling with more robust error management
    try:
        if file_extension == ".parquet":
            input_df = spark.read.option("mergeSchema", "true").format("parquet").load(args.data_file_path)
        elif file_extension == ".orc":
            input_df = spark.read.option("mergeSchema", "true").format("orc").load(args.data_file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}. Only .parquet and .orc are supported.")
    except Exception as read_error:
        logging.error(f"Error reading input file: {str(read_error)}")
        raise ValueError(f"Failed to read input file: {str(read_error)}")

    # Verify input dataframe
    record_count = input_df.count()
    if record_count == 0:
        raise ValueError("Input dataframe is empty. No records to process.")

    # Detailed logging of input dataframe characteristics
    logging.info(f"Input dataframe characteristics:")
    logging.info(f"Record count: {record_count}")
    logging.info(f"Schema: {input_df.schema}")

    # Write to Hudi with comprehensive configuration
    input_df.write.format("hudi") \
        .option("hoodie.datasource.write.operation", args.write_operation) \
        .option("hoodie.datasource.write.table.type", args.hudi_table_type) \
        .option("hoodie.datasource.write.recordkey.field", args.key_field) \
        .option("hoodie.datasource.write.precombine.field", args.precombine_field) \
        .option("hoodie.datasource.write.partitionpath.field", args.partition_field) \
        .option("hoodie.bootstrap.mode", args.bootstrap_type) \
        .option("hoodie.bootstrap.partition.regex", args.partition_regex) \
        .option("hoodie.table.name", args.hudi_table_name) \
        .option("hoodie.schema.on.read.enable", "true") \
        .option("hoodie.upsert.shuffle.parallelism", 2) \
        .mode("Overwrite") \
        .save(args.output_path)

    # Log success message with more details
    logging.info(f"Hudi table bootstrapped successfully.")
    logging.info(f"Table Name: {args.hudi_table_name}")
    logging.info(f"Output Path: {args.output_path}")
    print("Hudi table bootstrapped successfully.")

except ValueError as ve:
    # Detailed logging for validation and configuration errors
    error_message = f"Configuration Error: {str(ve)}"
    logging.error(error_message)
    print(error_message)
    sys.exit(1)
except PermissionError as pe:
    # Specific handling for permission-related errors
    error_message = f"Permission Denied: {str(pe)}"
    logging.error(error_message)
    print(error_message)
    sys.exit(1)
except Exception as e:
    # Comprehensive error logging for unexpected errors
    error_message = f"Unexpected Error: {str(e)}"
    logging.error(error_message)
    logging.error(f"Full Traceback: {traceback.format_exc()}")
    print(error_message)
    sys.exit(1)
finally:
    # Ensure Spark session is stopped
    if spark:
        spark.stop()

