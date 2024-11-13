from pyspark.sql import SparkSession
import argparse
from pathlib import Path
import pydoop.hdfs as hdfs
import logging
import os
import sys
import traceback
import re

def setup_logging(log_file):
    """Set up logging to a specified file with detailed formatting for INFO and ERROR levels."""
    # Create a logger
    logger = logging.getLogger()
    
    # Set the minimum level to INFO to capture both INFO and ERROR messages
    logger.setLevel(logging.INFO)
    
    # Create a file handler to log messages to a file
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)  # Will capture INFO and ERROR messages
    
    # Create a console handler to log ERROR messages to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.ERROR)  # Will capture only ERROR messages for console output
    
    # Create a formatter with detailed message format
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s - %(funcName)s")
    
    # Apply the formatter to the handlers
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Optionally log an info message indicating logging is set up
    logger.info("Logging setup complete")

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

                # Split partition_field into individual columns (if composite)
                partition_columns = [col.strip() for col in partition_field.split(',')]

                # Get distinct combinations of partition column values
                partition_values = input_df.select(*partition_columns).distinct().collect()
                
                total_input_count = 0
                total_hudi_count = 0
                for partition in partition_values:
                    # Create composite partition key by joining partition column values
                    partition_value = "_".join(str(p) for p in partition)
                    
                    # Match partition value with regex
                    if regex_compiled.match(partition_value):
                        # Filter input and Hudi dataframes by this composite partition
                        partition_input_df = input_df
                        for col, val in zip(partition_columns, partition):
                            partition_input_df = partition_input_df.filter(input_df[col] == val)

                        partition_hudi_df = hudi_df
                        for col, val in zip(partition_columns, partition):
                            partition_hudi_df = partition_hudi_df.filter(hudi_df[col] == val)

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
                logging.info(f"Total records in Input DataFrame: {total_input_count}")
                logging.info(f"Total records in Hudi table: {total_hudi_count}")
                if total_input_count != total_hudi_count:
                    raise ValueError(f"Total record count mismatch: Input has {total_input_count} records, "
                                     f"Hudi table has {total_hudi_count} records.")
                
            else:
                # If no partition regex, compare total record counts
                input_count = input_df.count()
                hudi_count = hudi_df.count()
                logging.info(f"Total records in Input DataFrame: {input_count}")
                logging.info(f"Total records in Hudi table: {hudi_count}")
                if input_count != hudi_count:
                    raise ValueError(f"Record count mismatch: Input has {input_count} records, "
                                     f"Hudi table has {hudi_count} records.")
    
    except Exception as e:
        # Raise exception so the main script can handle it
        raise ValueError(f"ERROR - Post-bootstrap validation failed: {str(e)}")

def get_existing_partitions(hdfs_path):
    """Check existing partitions in the output Hudi table."""
    try:
        if not hdfs.path.exists(hdfs_path):
            return None
        
        # Get list of existing partitions
        partitions = hdfs.ls(hdfs_path)
        existing_partitions = [
            p.split('/')[-1]  # Extract partition value (e.g., '2024-10-06') from full path
            for p in partitions if hdfs.path.isdir(p) and p != '.hoodie'
        ]
        logging.info(f"Found Partitions : {existing_partitions}")
        return existing_partitions
    except Exception as e:
        logging.error(f"Error fetching existing partitions: {traceback.format_exc()}")
        raise ValueError(f"Error fetching partitions from {hdfs_path}: {str(e)}")


def is_partition_complete(input_df, hudi_df, partition_value, partition_field):
    """Check if a partition is complete by comparing record counts."""
    try:
        # Filter input DataFrame for the partition value
        input_partition_df = input_df.filter(input_df[partition_field] == partition_value)
        input_partition_count = input_partition_df.count()

        # Filter Hudi DataFrame for the partition value
        hudi_partition_df = hudi_df.filter(hudi_df[partition_field] == partition_value)
        hudi_partition_count = hudi_partition_df.count()

        # Compare the counts
        if input_partition_count != hudi_partition_count:
            logging.info(f"Partition {partition_value} is incomplete. "
                         f"Input records: {input_partition_count}, Hudi records: {hudi_partition_count}")
            return False  # Partition is incomplete
        else:
            logging.info(f"Partition {partition_value} is complete. "
                         f"Input records: {input_partition_count}, Hudi records: {hudi_partition_count}")
            return True  # Partition is complete

    except Exception as e:
        logging.error(f"Error checking partition completeness for {partition_value}: {str(e)}")
        return False  # In case of error, consider the partition incomplete


def get_missing_and_incomplete_partitions(input_df, hudi_df, existing_partitions, partition_field):
    """Identify missing and incomplete partitions based on record counts."""
    missing_partitions = []
    incomplete_partitions = []

    # Get distinct partition values from the input DataFrame
    partition_column_values = input_df.select(partition_field).distinct().collect()

    # Create a set of partition values from the input data (converting datetime to string)
    partition_values_set = set(str(partition_value[partition_field]) for partition_value in partition_column_values)

    for partition_value in partition_values_set:
        # Check if partition exists in the Hudi table (compare partition values)
        if partition_value not in existing_partitions:
            missing_partitions.append(partition_value)
        else:
            # Check if the partition is complete by comparing record counts
            if not is_partition_complete(input_df, hudi_df, partition_value, partition_field):
                incomplete_partitions.append(partition_value)
    logging.info(f"Missing Partitions : {missing_partitions}")
    logging.info(f"Incomplete Partitions : {incomplete_partitions}")
    return missing_partitions, incomplete_partitions

def write_missing_and_incomplete_partitions(input_df, missing_partitions, incomplete_partitions, write_config, partition_field, output_path):
    """Write the missing and incomplete partitions to Hudi."""
    partitions_to_write = set(missing_partitions + incomplete_partitions)
    partitions_to_write_str = [str(p) for p in partitions_to_write]
    
    if partitions_to_write:
        # Filter the input DataFrame to include only records from missing or incomplete partitions
        df_to_write = input_df.filter(input_df[partition_field].isin(partitions_to_write))
        
        # Ensure no duplicates on key-field before writing
        key_fields = [field.strip() for field in args.key_field.split(',')]

        # Apply dropDuplicates on the list of key fields
        df_to_write = df_to_write.dropDuplicates(key_fields)
        
        try:
            # Write the missing or incomplete partitions to Hudi with Append mode
            df_to_write.write.format("hudi") \
                .options(**write_config) \
                .mode("Append") \
                .save(output_path)
            
            logging.info(f"Successfully wrote missing and incomplete partitions: {', '.join(partitions_to_write_str)}")
        except Exception as write_error:
            logging.error(f"Error while writing missing and incomplete partitions: {str(write_error)}")
            raise ValueError(f"Failed to write missing or incomplete partitions: {str(write_error)}")


def write_full_data(input_df, write_config):
    """Write the entire DataFrame if no partitions exist."""
    logging.info("No existing partitions found. Writing the full DataFrame.")
    input_df.write.format("hudi") \
        .options(**write_config) \
        .mode("Overwrite") \
        .save(args.output_path)
    logging.info("Full DataFrame written to Hudi.")


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

    # Check if output path exists and fetch partitions accordingly
    if hdfs.path.exists(args.output_path):
        # Read the Hudi DataFrame if output path exists
        hudi_df = spark.read.format("hudi").load(args.output_path)
        existing_partitions = get_existing_partitions(args.output_path)
    else:
        # Handle case when output path doesn't exist
        hudi_df = None
        existing_partitions = None

    # Write to Hudi with comprehensive configuration
    write_config = {
        "hoodie.datasource.write.table.type": args.hudi_table_type,
        "hoodie.datasource.write.recordkey.field": args.key_field,
        "hoodie.datasource.write.precombine.field": args.precombine_field,
        "hoodie.table.name": args.hudi_table_name,
        "hoodie.schema.on.read.enable": "true",
        "hoodie.bootstrap.mode": args.bootstrap_type
    }

    # Add optional parameters if provided
    if args.partition_field:
        write_config["hoodie.datasource.write.partitionpath.field"] = args.partition_field
    if args.partition_regex:
        write_config["hoodie.bootstrap.partition.regex"] = args.partition_regex

    if not existing_partitions:
        # If there are no existing partitions, write the full DataFrame
        write_full_data(input_df, write_config)
    else:
        # Identify missing partitions
        missing_partitions, incomplete_partitions = get_missing_and_incomplete_partitions(input_df, hudi_df, existing_partitions, args.partition_field)

        if missing_partitions or incomplete_partitions:
        # Write the missing and incomplete partitions
            write_missing_and_incomplete_partitions(input_df, missing_partitions, incomplete_partitions, write_config, args.partition_field, args.output_path)
        else:
        # If no partitions need writing, log and exit
            logging.info("No missing or incomplete partitions found. No data written.")
    
    logging.info("Hudi bootstrap process completed successfully.")

    if args.partition_regex:
        validate_post_bootstrap(spark, args.output_path, input_df, args.bootstrap_type, args.partition_field, args.partition_regex)
    else:
        validate_post_bootstrap(spark, args.output_path, input_df, args.bootstrap_type)

except ValueError as ve:
    # Detailed logging for validation and configuration errors
    error_message = f"Configuration Error: {str(ve)}"
    logging.error(error_message)
    sys.exit(1)
except PermissionError as pe:
    # Specific handling for permission-related errors
    error_message = f"Permission Denied: {str(pe)}"
    logging.error(error_message)
    sys.exit(1)
except Exception as e:
    # Comprehensive error logging for unexpected errors
    error_message = f"Unexpected Error: {str(e)}"
    logging.error(error_message)
    logging.error(f"Full Traceback: {traceback.format_exc()}")
    sys.exit(1)
finally:
    # Ensure Spark session is stopped
    if spark:
        spark.stop()

