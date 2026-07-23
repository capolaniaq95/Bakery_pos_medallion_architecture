from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Bronze Layer: Raw Data Ingestion
# Uses Auto Loader to incrementally ingest JSON files from S3
# This is the entry point of the medallion architecture (Bronze → Silver → Gold)

@dp.table(
    comment="Bronze layer: Raw bakery POS transaction data ingested from S3 using Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bakery_pos_raw():
    """
    Streams raw bakery POS data from S3 using Auto Loader (cloudFiles).
    
    Auto Loader Configuration:
    - Format: JSON with automatic type inference
    - Schema: Inferred automatically from JSON files
    - Mode: Incremental processing of new files only
    - Checkpoint and schema location: Managed automatically by Databricks
    
    Metadata columns added:
    - file_name: Source file name for lineage tracking
    - file_path: Full S3 path for debugging
    - file_modification_time: File timestamp for ordering/troubleshooting
    - processing_timestamp: When the record was ingested into the pipeline
    """
    return (
        spark.readStream
        .format("cloudFiles")  # Auto Loader for incremental ingestion
        .option("cloudFiles.format", "json")  # Source file format
        .option("cloudFiles.inferColumnTypes", "true")  # Infer proper data types (not just STRING)
        .load("s3://bakery-pos-bk-978430483777-us-east-2-an/bakery-pos/2026/06/25")
        .select(
            "*",  # All inferred columns from JSON
            F.col("_metadata.file_name").alias("file_name"),
            F.col("_metadata.file_path").alias("file_path"),
            F.col("_metadata.file_modification_time").alias("file_modification_time"),
            F.current_timestamp().alias("processing_timestamp")
        ).writeStream
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable("my_catalog.my_schema.raw_events")    
    )
