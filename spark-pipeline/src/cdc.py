"""
Module for handling Change Data Capture (CDC) from PostgreSQL.
This implementation uses Debezium + Kafka for proper CDC.
"""
import json
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, current_timestamp,
    expr, from_json, struct, to_timestamp,
    from_unixtime, substring, udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, IntegerType, BooleanType,
    TimestampType, DateType, DecimalType
)

from src import config
from src.db_utils import execute_query

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PostgreSQLCDCConnector:
    """
    Connector for PostgreSQL CDC using Debezium and Kafka.
    """
    def __init__(self, spark_session, schema_name, table_name, target_table):
        """
        Initialize the CDC connector.

        Args:
            spark_session: The Spark session
            schema_name: Source schema name
            table_name: Source table name
            target_table: Target table name
        """
        self.spark = spark_session
        self.schema_name = schema_name
        self.table_name = table_name
        self.target_table = target_table
        self.stream_query = None
        self.running = False

    def start(self):
        """
        Start the CDC connector by creating a Spark streaming query
        that reads from Kafka and writes to the target database.
        """
        if self.running:
            logger.warning(f"CDC connector for {self.schema_name}.{self.table_name} already running")
            return

        logger.info(f"Starting CDC connector for {self.schema_name}.{self.table_name}")

        # Get the Kafka topic for this table
        topic = config.get_kafka_topic(self.schema_name, self.table_name)

        # Get the schema for this table
        value_schema = self._get_table_schema()

        try:
            # Read from Kafka
            df = (self.spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load())

            # Parse the value column as JSON using a simpler approach
            parsed_df = (df
                .selectExpr("CAST(value AS STRING) as json_value", "CAST(key AS STRING) as key_value")
                .select(
                    from_json(col("json_value"), value_schema).alias("data"),
                    col("key_value")
                )
                .select("data.payload.*", "key_value"))

            # Add operation_type, effective_from, and is_current columns
            transformed_df = self._transform_data(parsed_df)

            # Write to target database
            self.stream_query = (transformed_df
                .writeStream
                .foreachBatch(self._process_batch)
                .option("checkpointLocation", f"{config.CHECKPOINT_LOCATION}/{self.schema_name}_{self.table_name}")
                .start())

            self.running = True

            logger.info(f"CDC connector for {self.schema_name}.{self.table_name} started")

            return self

        except Exception as e:
            logger.error(f"Error starting CDC connector: {str(e)}")
            raise

    def stop(self):
        """
        Stop the CDC connector.
        """
        if not self.running:
            return

        logger.info(f"Stopping CDC connector for {self.schema_name}.{self.table_name}")

        if self.stream_query:
            self.stream_query.stop()

        self.running = False

    def _get_table_schema(self):
        """
        Get the schema for this table.
        This should match the payload structure in Debezium messages.
        """
        # Define the payload schema structure based on table
        if self.table_name == "customers":
            payload_schema = StructType([
                StructField("customer_id", LongType(), True),
                StructField("customer_name", StringType(), True),
                StructField("is_active", BooleanType(), True),
                StructField("customer_address", StringType(), True),
                StructField("updated_at", LongType(), True),  # Debezium timestamp in ms
                StructField("updated_by", LongType(), True),
                StructField("created_at", LongType(), True),  # Debezium timestamp in ms
                StructField("created_by", LongType(), True),
                StructField("__op", StringType(), True),
                StructField("__ts_ms", LongType(), True),
                StructField("__deleted", StringType(), True)
            ])
        elif self.table_name == "products":
            payload_schema = StructType([
                StructField("product_id", LongType(), True),
                StructField("product_name", StringType(), True),
                StructField("barcode", StringType(), True),
                StructField("unity_price", DecimalType(10, 2), True),
                StructField("is_active", BooleanType(), True),
                StructField("updated_at", LongType(), True),  # Debezium timestamp in ms
                StructField("updated_by", LongType(), True),
                StructField("created_at", LongType(), True),  # Debezium timestamp in ms
                StructField("created_by", LongType(), True),
                StructField("__op", StringType(), True),
                StructField("__ts_ms", LongType(), True),
                StructField("__deleted", StringType(), True)
            ])
        elif self.table_name == "orders":
            payload_schema = StructType([
                StructField("order_id", LongType(), True),
                StructField("order_date", DateType(), True),
                StructField("delivery_date", DateType(), True),
                StructField("customer_id", LongType(), True),
                StructField("status", StringType(), True),
                StructField("updated_at", LongType(), True),  # Debezium timestamp in ms
                StructField("updated_by", LongType(), True),
                StructField("created_at", LongType(), True),  # Debezium timestamp in ms
                StructField("created_by", LongType(), True),
                StructField("__op", StringType(), True),
                StructField("__ts_ms", LongType(), True),
                StructField("__deleted", StringType(), True)
            ])
        elif self.table_name == "order_items":
            payload_schema = StructType([
                StructField("order_item_id", LongType(), True),
                StructField("order_id", LongType(), True),
                StructField("product_id", LongType(), True),
                StructField("quanity", IntegerType(), True),  # Note: typo in source schema
                StructField("updated_at", LongType(), True),  # Debezium timestamp in ms
                StructField("updated_by", LongType(), True),
                StructField("created_at", LongType(), True),  # Debezium timestamp in ms
                StructField("created_by", LongType(), True),
                StructField("__op", StringType(), True),
                StructField("__ts_ms", LongType(), True),
                StructField("__deleted", StringType(), True)
            ])
        else:
            raise ValueError(f"Unknown table name: {self.table_name}")

        # Return the full Debezium message schema
        return StructType([
            StructField("payload", payload_schema, True)
        ])

    def _transform_data(self, df):
        """
        Transform the data before writing to the target database.
        """
        # Map Debezium operation types to our operation types
        df = df.withColumn(
            "operation_type",
            when(col("__op") == "c", "INSERT")
            .when(col("__op") == "u", "UPDATE")
            .when(col("__op") == "d", "DELETE")
            .when(col("__op") == "r", "INSERT")  # 'r' is for read/snapshot
            .otherwise("UNKNOWN")
        )

        # Set effective_from based on the timestamp from Debezium
        df = df.withColumn("effective_from",
                          from_unixtime(col("__ts_ms") / 1000).cast(TimestampType()))

        # Set effective_to to NULL
        df = df.withColumn("effective_to", lit(None).cast(TimestampType()))

        # Set is_current based on operation type
        df = df.withColumn("is_current",
                          when(col("operation_type") != "DELETE", True)
                          .otherwise(False))

        # Convert timestamp fields from Debezium format (milliseconds) to timestamp
        df = df.withColumn("updated_at",
                          when(col("updated_at").isNotNull(),
                               from_unixtime(col("updated_at") / 1000).cast(TimestampType()))
                          .otherwise(lit(None).cast(TimestampType())))

        df = df.withColumn("created_at",
                          when(col("created_at").isNotNull(),
                               from_unixtime(col("created_at") / 1000).cast(TimestampType()))
                          .otherwise(lit(None).cast(TimestampType())))

        # Rename columns to match target schema
        df = self._rename_columns(df)

        # Drop Debezium metadata columns
        df = df.drop("__op", "__ts_ms", "__deleted", "key_value")

        return df

    def _rename_columns(self, df):
        """
        Rename columns to match target schema.
        """
        # Common renames for all tables
        df = df.withColumnRenamed("updated_at", "source_updated_at")
        df = df.withColumnRenamed("updated_by", "source_updated_by")
        df = df.withColumnRenamed("created_at", "source_created_at")
        df = df.withColumnRenamed("created_by", "source_created_by")

        # Table-specific renames
        if self.table_name == "order_items":
            df = df.withColumnRenamed("quanity", "quantity")  # Fix typo in source schema

        return df

    def _process_batch(self, batch_df, batch_id):
        """
        Process a batch of records.

        Args:
            batch_df: The batch DataFrame
            batch_id: The batch ID
        """
        logger.info(f"Processing batch {batch_id} for {self.schema_name}.{self.table_name}")

        if batch_df.isEmpty():
            logger.info(f"Batch {batch_id} is empty for {self.schema_name}.{self.table_name}")
            return

        try:
            # Get updates and deletes
            updates_df = batch_df.filter(col("operation_type") == "UPDATE")
            deletes_df = batch_df.filter(col("operation_type") == "DELETE")

            # Update previous records for updates and deletes
            self._update_previous_records(updates_df, deletes_df)

            # Write the batch to the target database
            (batch_df
                .write
                .format("jdbc")
                .option("url", config.get_target_jdbc_url())
                .option("dbtable", self.target_table)
                .option("user", config.TARGET_DB_USER)
                .option("password", config.TARGET_DB_PASSWORD)
                .option("driver", "org.postgresql.Driver")
                .mode("append")
                .save())

            logger.info(f"Batch {batch_id} processed for {self.schema_name}.{self.table_name}")

        except Exception as e:
            logger.error(f"Error processing batch {batch_id} for {self.schema_name}.{self.table_name}: {str(e)}")
            raise

    def _update_previous_records(self, updates_df, deletes_df):
        """
        Update previous records to set effective_to and is_current.

        Args:
            updates_df: DataFrame with updated records
            deletes_df: DataFrame with deleted records
        """
        # Get primary key column
        pk_column = get_primary_key_column(self.table_name)
        target_pk = f"{self.table_name}_id"

        # Combine updates and deletes
        combined_df = updates_df.union(deletes_df)

        if combined_df.isEmpty():
            return

        # Get the list of primary keys
        pk_values = combined_df.select(target_pk).collect()
        pk_list = [str(row[target_pk]) for row in pk_values]

        if not pk_list:
            return

        # Create a comma-separated string of values for the SQL IN clause
        pk_values_str = "(" + ",".join(pk_list) + ")"

        # Execute SQL to update previous records
        update_sql = f"""
        UPDATE {self.target_table}
        SET effective_to = current_timestamp(),
            is_current = false
        WHERE {target_pk} IN {pk_values_str}
        AND is_current = true
        """

        # Execute the update
        execute_query(update_sql)

# Helper functions
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
