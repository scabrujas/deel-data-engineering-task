"""
Main module for the Spark streaming application.
"""
import os
import time
from datetime import datetime
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, current_timestamp,
    expr, from_json, struct, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, IntegerType, BooleanType,
    TimestampType, DateType, DecimalType
)

from src import config
from src.simple_cdc import SimpleCDCConnector
from src.db_utils import execute_query, check_table_exists, create_schema_if_not_exists

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Spark session
def create_spark_session():
    """
    Create and configure a Spark session.
    """
    logger.info("Initializing Spark session...")

    # Create Spark session
    spark = (SparkSession.builder
        .appName("PostgreSQL CDC Stream")
        .master(config.SPARK_MASTER)
        .config("spark.jars.packages",
                "org.postgresql:postgresql:42.6.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.streaming.checkpointLocation", config.CHECKPOINT_LOCATION)
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate())

    # Set log level
    spark.sparkContext.setLogLevel("WARN")

    return spark

def process_table_stream(spark, table_config):
    """
    Process a single table stream from source to target database.
    """
    schema_name = table_config["schema"]
    table_name = table_config["table"]
    target_table = table_config["target_table"]

    logger.info(f"Processing stream for table: {schema_name}.{table_name}")

    # Define source table schema based on table name
    schema = get_table_schema(table_name)

    # Read from source database table
    df = (spark.read
        .format("jdbc")
        .option("url", config.get_source_jdbc_url())
        .option("dbtable", f"{schema_name}.{table_name}")
        .option("user", config.SOURCE_DB_USER)
        .option("password", config.SOURCE_DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load())

    # Ensure date fields are properly handled for orders table
    if table_name == "orders":
        logger.info(f"Ensuring proper date format for orders table")
        # Force proper date parsing if needed
        df = df.withColumn("order_date", col("order_date").cast(DateType()))
        df = df.withColumn("delivery_date", col("delivery_date").cast(DateType()))

    # Add operation_type, effective_from, and is_current columns
    df = df.withColumn("operation_type", lit("INSERT"))
    df = df.withColumn("effective_from", current_timestamp())
    df = df.withColumn("effective_to", lit(None).cast(TimestampType()))
    df = df.withColumn("is_current", lit(True))

    # Rename columns to match target schema
    df = rename_columns_for_target(df, table_name)

    # Write to target database
    (df.write
        .format("jdbc")
        .option("url", config.get_target_jdbc_url())
        .option("dbtable", target_table)
        .option("user", config.TARGET_DB_USER)
        .option("password", config.TARGET_DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save())

    logger.info(f"Initial load completed for {schema_name}.{table_name}")

    # Set up streaming job to capture changes
    return setup_cdc_stream(spark, schema_name, table_name, target_table, schema)

def setup_cdc_stream(spark, schema_name, table_name, target_table, schema):
    """
    Set up CDC streaming from PostgreSQL.
    This function uses our PostgreSQLCDCConnector for CDC.
    """
    logger.info(f"Setting up CDC stream for {schema_name}.{table_name}")

    # Create a CDC connector for this table
    connector = SimpleCDCConnector(spark, schema_name, table_name, target_table)

    # Start the connector
    connector.start()

    return connector

def process_changes(current_df, target_df, schema_name, table_name, target_table):
    """
    Process changes between current and previous state of the table.
    """
    # Get primary key column for this table
    pk_column = get_primary_key_column(table_name)
    source_pk = f"{pk_column}"
    target_pk = f"{table_name}_id"

    # Register temporary views
    current_df.createOrReplaceTempView("current_state")
    target_df.createOrReplaceTempView("previous_state")

    # Identify changes (this is a simplified approach)
    spark = current_df.sparkSession

    # New records (in current but not in previous)
    new_records = spark.sql(f"""
        SELECT c.*, 'INSERT' as operation_type,
               current_timestamp() as effective_from,
               NULL as effective_to,
               true as is_current
        FROM current_state c
        LEFT JOIN previous_state p
        ON c.{source_pk} = p.{target_pk}
        WHERE p.{target_pk} IS NULL
    """)

    # Changed records (in both but different values)
    # This is simplified and would need more complex logic to detect actual changes
    changed_records = spark.sql(f"""
        SELECT c.*, 'UPDATE' as operation_type,
               current_timestamp() as effective_from,
               NULL as effective_to,
               true as is_current
        FROM current_state c
        JOIN previous_state p
        ON c.{source_pk} = p.{target_pk}
        WHERE c.updated_at > p.source_updated_at
    """)

    # Deleted records (in previous but not in current)
    # Note: In a real CDC solution, we'd have explicit delete events
    deleted_records = spark.sql(f"""
        SELECT p.*, 'DELETE' as operation_type,
               current_timestamp() as effective_from,
               NULL as effective_to,
               false as is_current
        FROM previous_state p
        LEFT JOIN current_state c
        ON p.{target_pk} = c.{source_pk}
        WHERE c.{source_pk} IS NULL
    """)

    # Combine all changes
    all_changes = new_records.union(changed_records).union(deleted_records)

    if all_changes.count() > 0:
        logger.info(f"Found {all_changes.count()} changes for {schema_name}.{table_name}")

        # For updates, mark the previous current record as not current
        if changed_records.count() > 0:
            update_previous_records(spark, changed_records, target_table, target_pk)

        # For deletes, mark the record as not current
        if deleted_records.count() > 0:
            update_deleted_records(spark, deleted_records, target_table, target_pk)

        # Write all new/changed records to target
        all_changes = rename_columns_for_target(all_changes, table_name)

        # Write to target database
        (all_changes.write
            .format("jdbc")
            .option("url", config.get_target_jdbc_url())
            .option("dbtable", target_table)
            .option("user", config.TARGET_DB_USER)
            .option("password", config.TARGET_DB_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save())
    else:
        logger.info(f"No changes detected for {schema_name}.{table_name}")

def update_previous_records(spark, changed_records, target_table, pk_column):
    """
    Update previous current records to set effective_to and is_current.
    """
    # Get the list of primary key values that have changed
    pk_values = changed_records.select(pk_column).collect()
    pk_list = [str(row[pk_column]) for row in pk_values]

    if not pk_list:
        return

    # Create a comma-separated string of values for the SQL IN clause
    pk_values_str = "(" + ",".join(pk_list) + ")"

    # Execute SQL to update previous records
    update_sql = f"""
    UPDATE {target_table}
    SET effective_to = current_timestamp(),
        is_current = false
    WHERE {pk_column} IN {pk_values_str}
    AND is_current = true
    """

    # Execute the update using JDBC
    try:
        spark._jsparkSession.execute(update_sql)
    except Exception as e:
        logger.error(f"Error updating previous records: {str(e)}")
        # Fall back to using db_utils
        from src.db_utils import execute_query
        execute_query(update_sql)

def update_deleted_records(spark, deleted_records, target_table, pk_column):
    """
    Update records that are deleted to set effective_to and is_current.
    """
    # Similar to update_previous_records but for deleted records
    # Implementation would be very similar
    pass

def rename_columns_for_target(df, table_name):
    """
    Rename source columns to match target schema.
    """
    # Common renames for all tables
    df = df.withColumnRenamed("updated_at", "source_updated_at")
    df = df.withColumnRenamed("updated_by", "source_updated_by")
    df = df.withColumnRenamed("created_at", "source_created_at")
    df = df.withColumnRenamed("created_by", "source_created_by")

    # Table-specific renames
    if table_name == "customers":
        df = df.withColumnRenamed("customer_id", "customer_id")
    elif table_name == "products":
        df = df.withColumnRenamed("product_id", "product_id")
    elif table_name == "orders":
        df = df.withColumnRenamed("order_id", "order_id")
    elif table_name == "order_items":
        df = df.withColumnRenamed("order_item_id", "order_item_id")
        df = df.withColumnRenamed("quanity", "quantity")  # Fix typo in source schema

    return df

def get_primary_key_column(table_name):
    """
    Get the primary key column name for a table.
    """
    pk_mapping = {
        "customers": "customer_id",
        "products": "product_id",
        "orders": "order_id",
        "order_items": "order_item_id"
    }

    return pk_mapping.get(table_name)

def get_table_schema(table_name):
    """
    Get the schema for a specific table.
    """
    if table_name == "customers":
        return StructType([
            StructField("customer_id", LongType(), False),
            StructField("customer_name", StringType(), False),
            StructField("is_active", BooleanType(), False),
            StructField("customer_address", StringType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("updated_by", LongType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", LongType(), True)
        ])
    elif table_name == "products":
        return StructType([
            StructField("product_id", LongType(), False),
            StructField("product_name", StringType(), False),
            StructField("barcode", StringType(), False),
            StructField("unity_price", DecimalType(10, 2), False),
            StructField("is_active", BooleanType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("updated_by", LongType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", LongType(), True)
        ])
    elif table_name == "orders":
        return StructType([
            StructField("order_id", LongType(), False),
            StructField("order_date", DateType(), True),
            StructField("delivery_date", DateType(), True),
            StructField("customer_id", LongType(), True),
            StructField("status", StringType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("updated_by", LongType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", LongType(), True)
        ])
    elif table_name == "order_items":
        return StructType([
            StructField("order_item_id", LongType(), False),
            StructField("order_id", LongType(), True),
            StructField("product_id", LongType(), True),
            StructField("quanity", IntegerType(), True),  # Note: typo in source schema
            StructField("updated_at", TimestampType(), True),
            StructField("updated_by", LongType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", LongType(), True)
        ])
    else:
        raise ValueError(f"Unknown table name: {table_name}")

def main():
    """
    Main entry point for the application.
    """
    try:
        logger.info("Starting Spark streaming application...")

        # Wait for databases to be available
        time.sleep(10)

        # Create Spark session
        spark = create_spark_session()

        # Ensure the target schema exists
        create_schema_if_not_exists("analytics")

        # List to store CDC connectors
        cdc_connectors = []

        # Process each table
        for table_config in config.TABLES:
            connector = process_table_stream(spark, table_config)
            cdc_connectors.append(connector)

        # Keep the application running
        # In a real application, we would properly manage the lifecycle of the connectors
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping CDC connectors...")
            for connector in cdc_connectors:
                if connector:
                    connector.stop()

    except Exception as e:
        logger.error(f"Error in main application: {str(e)}")
        raise

if __name__ == "__main__":
    main()
