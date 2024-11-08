from pyspark.sql import SparkSession
import argparse
from pathlib import Path
import pydoop.hdfs as hdfs
import logging
import os
import sys
import traceback
import re

# Set up logging to file with more detailed formatting
def setup_logging(log_file):
    """Set up logging to a specified file with detailed formatting."""
    logging.basicConfig(
        filename=log_file,
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

def validate_fields_in_schema(input_df, args):
    """Validate key_field, precombine_field, and partition_field against the input DataFrame schema."""
    errors = []
    schema_fields = input_df.schema.fieldNames()
    
    # Validate key fields (multiple fields separated by commas)
    key_fields = args.key_field.split(',')
    missing_key_fields = [field for field in key_fields if field not in schema_fields]
    if missing_key_fields:
        errors.append(f"Key fields missing in schema: {', '.join(missing_key_fields)}")

    # Validate precombine field
    if args.precombine_field not in schema_fields:
        errors.append(f"Precombine field '{args.precombine_field}' not found in schema.")

    # Validate partition field only if it's provided
    if args.partition_field and args.partition_field not in schema_fields:
        key_fields = args.partition_field.split(',')
        missing_key_fields = [field for field in key_fields if field not in schema_fields]
        if missing_key_fields:
            errors.append(f"Partition field '{args.partition_field}' not found in schema.")
    
    if errors:
        raise ValueError("\n".join(errors))

def validate_post_bootstrap(spark, output_path, input_df, bootstrap_type, partition_field=None, partition_regex=None):
    """Validate the Hudi table written to the output path against the input DataFrame."""
    try:
        # Read the Hudi table from the output path
        hudi_df = spark.read.format("hudi").load(output_path)
        
        # Extract schemas and columns
        input_columns = {field.name: field.dataType for field in input_df.schema.fields}
        hudi_columns = {field.name: field.dataType for field in hudi_df.schema.fields if not field.name.startswith("_")}
        
        # Validate schema: Check if the input DataFrame columns exist in the Hudi table
        missing_columns = [col for col in input_columns if col not in hudi_columns]
        if missing_columns:
            raise ValueError(f"Columns {', '.join(missing_columns)} are missing in Hudi table.")
        
        # Validate data types for matching columns
        for column_name, input_type in input_columns.items():
            if column_name in hudi_columns and input_type != hudi_columns[column_name]:
                raise ValueError(f"Data type mismatch for column '{column_name}': "
                                 f"Input type '{input_type}' vs Hudi type '{hudi_columns[column_name]}'.")
        
        # Perform record count validation based on bootstrap type
        if bootstrap_type == "FULL_RECORD":
            # Validate partition regex if specified
            if partition_field and partition_regex:
                # Compile regex once
                try:
                    regex_compiled = re.compile(partition_regex)
                except re.error as e:
                    raise ValueError(f"Invalid regex pattern: {e}")

                # Get distinct partitions and check count for each partition
                input_partitions = input_df.select(partition_field).distinct().collect()
                
                total_input_count = 0
                total_hudi_count = 0
                for partition in input_partitions:
                    partition_value = partition[0]
                    
                    # Match partition value with regex
                    if regex_compiled.match(str(partition_value)):
                        partition_input_df = input_df.filter(input_df[partition_field] == partition_value)
                        partition_hudi_df = hudi_df.filter(hudi_df[partition_field] == partition_value)
                        
                        # Count records for this partition
                        partition_input_count = partition_input_df.count()
                        partition_hudi_count = partition_hudi_df.count()
                        
                        total_input_count += partition_input_count
                        total_hudi_count += partition_hudi_count
                        
                        # Check for record count mismatch
                        if partition_input_count != partition_hudi_count:
                            raise ValueError(f"Record count mismatch for partition '{partition_value}': "
                                             f"Input has {partition_input_count} records, Hudi table has {partition_hudi_count} records.")

                # Compare total record counts for all matched partitions
                logging.error(f"Total records in Input DataFrame: {total_input_count}")
                logging.error(f"Total records in Hudi table: {total_hudi_count}")
                if total_input_count != total_hudi_count:
                    raise ValueError(f"Total record count mismatch: Input has {total_input_count} records, "
                                     f"Hudi table has {total_hudi_count} records.")
                
                #logging.info(f"Full record count validation passed for partitions matching regex '{partition_regex}'.")
            
            else:
                # If no partition regex, compare total record counts
                input_count = input_df.count()
                hudi_count = hudi_df.count()
                logging.error(f"Total records in Input DataFrame: {input_count}")
                logging.error(f"Total records in Hudi table: {hudi_count}")
                if input_count != hudi_count:
                    raise ValueError(f"Record count mismatch: Input has {input_count} records, "
                                     f"Hudi table has {hudi_count} records.")
                

    
    except Exception as e:
        # Raise exception so the main script can handle it
        raise ValueError(f"ERROR - Post-bootstrap validation failed: {str(e)}")




# Parse the command-line arguments
parser = argparse.ArgumentParser(description="Bootstrap Hudi Table using DataSource Writer")
parser.add_argument("--data-file-path", required=True)
parser.add_argument("--hudi-table-name", required=True)
parser.add_argument("--key-field", required=True)
parser.add_argument("--precombine-field", required=True)
parser.add_argument("--partition-field", required=False)  # Make optional
parser.add_argument("--hudi-table-type", required=True)
parser.add_argument("--output-path", required=True)
parser.add_argument("--bootstrap-type", required=True)
parser.add_argument("--partition-regex", required=False)  # Make optional
parser.add_argument("--log-file", required=True) 
args = parser.parse_args()

setup_logging(args.log_file)

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

    validate_fields_in_schema(input_df, args)

    # Verify input dataframe
    record_count = input_df.count()
    if record_count == 0:
        raise ValueError("Input dataframe is empty. No records to process.")

    # Write to Hudi with comprehensive configuration
    write_config = {
        "hoodie.datasource.write.table.type": args.hudi_table_type,
        "hoodie.datasource.write.recordkey.field": args.key_field,
        "hoodie.datasource.write.precombine.field": args.precombine_field,
        "hoodie.table.name": args.hudi_table_name,
        "hoodie.schema.on.read.enable": "true",
        "hoodie.upsert.shuffle.parallelism": 2,
        "hoodie.bootstrap.mode": args.bootstrap_type
    }

    # Add optional parameters if provided
    if args.partition_field:
        write_config["hoodie.datasource.write.partitionpath.field"] = args.partition_field
    if args.partition_regex:
        write_config["hoodie.bootstrap.partition.regex"] = args.partition_regex

    input_df.write.format("hudi") \
        .options(**write_config) \
        .mode("Overwrite") \
        .save(args.output_path)

    if args.partition_regex:
        validate_post_bootstrap(spark, args.output_path, input_df, args.bootstrap_type, args.partition_field, args.partition_regex)
    else:
        validate_post_bootstrap(spark, args.output_path, input_df, args.bootstrap_type)

    # Log success message with more details

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
