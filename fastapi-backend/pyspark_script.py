from pyspark.sql import SparkSession
import argparse
from pathlib import Path
import pydoop.hdfs as hdfs
import os
import re

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

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Hudi Bootstrap") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

def get_first_file_extension(hdfs_path):
    """Retrieve the extension of the first file found in HDFS."""
    files = hdfs.ls(hdfs_path)
    for item in files:
        if hdfs.path.isfile(item):
            return Path(item).suffix.lower()
        elif hdfs.path.isdir(item):
            file_extension = get_first_file_extension(item)
            if file_extension:
                return file_extension
    return None

# Get the file extension of the first file
file_extension = get_first_file_extension(args.data_file_path)

if file_extension == ".parquet":
# Try reading the file as a Parquet file
	input_df = spark.read.option("mergeSchema", "true").format("parquet").load(args.data_file_path)
elif file_extension == ".orc":
	input_df = spark.read.option("mergeSchema", "true").format("orc").load(args.data_file_path)
else:
    	raise ValueError("Unsupported file format. Please provide a .parquet or .orc file.")
   

# Write to Hudi using the DataSource API for bootstrapping
input_df.write.format("hudi") \
    .option("hoodie.datasource.write.operation", args.write_operation) \
    .option("hoodie.datasource.write.table.type", args.hudi_table_type) \
    .option("hoodie.datasource.write.recordkey.field", args.key_field) \
    .option("hoodie.datasource.write.precombine.field", args.precombine_field) \
    .option("hoodie.datasource.write.partitionpath.field", args.partition_field) \
    .option("hoodie.bootstrap.mode", args.bootstrap_type) \
    .option("hoodie.bootstrap.partition.regex", args.partition_regex) \
    .option("hoodie.table.name", args.hudi_table_name) \
    .option("hoodie.schema.on.read.enable","true") \
    .option('hoodie.upsert.shuffle.parallelism', 2) \
    .mode("Overwrite") \
    .save(args.output_path)

spark.stop()
