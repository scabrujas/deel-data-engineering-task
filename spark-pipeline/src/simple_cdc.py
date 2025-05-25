"""
Simplified CDC implementation that works with actual Debezium messages.

This module provides a CDC (Change Data Capture) connector that processes
Debezium messages from Kafka and implements SCD Type 2 (Slowly Changing Dimensions)
for historical data tracking in the target analytics database.

Key Features:
- Processes Debezium CDC events (INSERT, UPDATE, DELETE)
- Implements SCD Type 2 with effective_from/effective_to timestamps
- Handles deduplication within batches
- Supports custom decimal decoding for Debezium VariableScaleDecimal format
- Provides comprehensive error handling and logging

Author: Data Engineering Team
Date: 2025-05-25
"""
import json
import logging
import base64
import struct as python_struct
from datetime import datetime
from decimal import Decimal
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, current_timestamp,
    expr, from_json, struct, to_timestamp,
    from_unixtime, substring, udf,
    get_json_object, coalesce, row_number
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, IntegerType, BooleanType,
    TimestampType, DateType, DecimalType
)
from pyspark.sql.window import Window

from src import config
from src.db_utils import execute_query

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ProcessingMetrics:
    """Metrics for batch processing."""
    batch_id: int
    records_processed: int
    records_written: int
    processing_time_ms: float
    errors: List[str]

class CDCError(Exception):
    """Custom exception for CDC processing errors."""
    pass

class DecimalDecodingError(CDCError):
    """Exception raised when decimal decoding fails."""
    pass

def decode_variable_scale_decimal(json_str: Optional[str]) -> float:
    """
    Decode Debezium VariableScaleDecimal format.

    The format is: {"scale": n, "value": "base64_encoded_bytes"}
    where the bytes represent a big-endian signed integer that needs to be
    divided by 10^scale to get the decimal value.

    Args:
        json_str: JSON string containing scale and base64-encoded value

    Returns:
        Decimal value or 0.0 if parsing fails

    Raises:
        DecimalDecodingError: If decoding fails with invalid data
    """
    if not json_str or json_str == 'null' or json_str.strip() == '':
        return 0.0

    try:
        # Remove quotes if present
        json_str = json_str.strip('"')

        # Parse the JSON
        data = json.loads(json_str)
        if not isinstance(data, dict) or 'scale' not in data or 'value' not in data:
            # If it's not the VariableScaleDecimal format, try to parse as regular number
            try:
                return float(json_str)
            except (ValueError, TypeError):
                logger.warning(f"Could not parse as regular number: {json_str}")
                return 0.0

        scale = data['scale']
        value_b64 = data['value']

        # Validate scale
        if not isinstance(scale, int) or scale < 0:
            raise DecimalDecodingError(f"Invalid scale value: {scale}")

        # Decode base64 to bytes
        try:
            value_bytes = base64.b64decode(value_b64)
        except Exception as e:
            raise DecimalDecodingError(f"Failed to decode base64 value: {value_b64}, error: {str(e)}")

        # Convert bytes to signed integer (big-endian)
        int_value = _bytes_to_signed_int(value_bytes)

        # Apply scale to get decimal value
        decimal_value = Decimal(int_value) / (Decimal(10) ** scale)

        return float(decimal_value)

    except DecimalDecodingError:
        raise
    except Exception as e:
        logger.warning(f"Failed to decode VariableScaleDecimal: {json_str}, error: {str(e)}")
        return 0.0


def _bytes_to_signed_int(value_bytes: bytes) -> int:
    """
    Convert bytes to signed integer with proper handling of variable length.

    Args:
        value_bytes: Bytes to convert

    Returns:
        Signed integer value
    """
    if len(value_bytes) == 0:
        return 0

    # Handle variable length by padding if necessary
    if len(value_bytes) <= 4:
        # Pad to 4 bytes for int32
        if value_bytes[0] & 0x80:  # Check if negative (MSB set)
            value_bytes = b'\xff' * (4 - len(value_bytes)) + value_bytes
        else:
            value_bytes = b'\x00' * (4 - len(value_bytes)) + value_bytes
        return python_struct.unpack('>i', value_bytes)[0]  # Big-endian signed int32
    elif len(value_bytes) <= 8:
        # Pad to 8 bytes for int64
        if value_bytes[0] & 0x80:  # Check if negative (MSB set)
            value_bytes = b'\xff' * (8 - len(value_bytes)) + value_bytes
        else:
            value_bytes = b'\x00' * (8 - len(value_bytes)) + value_bytes
        return python_struct.unpack('>q', value_bytes)[0]  # Big-endian signed int64
    else:
        # For very large numbers, use Python's int.from_bytes
        return int.from_bytes(value_bytes, byteorder='big', signed=True)

# Register UDF
decode_decimal_udf = udf(decode_variable_scale_decimal, DecimalType(10, 2))

class SimpleCDCConnector:
    """
    Simplified CDC connector that handles Debezium messages using JSON path extraction.

    This class implements a Spark streaming application that:
    1. Reads CDC events from Kafka topics
    2. Processes Debezium messages with proper field extraction
    3. Implements SCD Type 2 for historical data tracking
    4. Handles deduplication and error recovery

    Attributes:
        spark: Spark session instance
        schema_name: Source database schema name
        table_name: Source table name
        target_table: Target table name in analytics database
        stream_query: Spark streaming query instance
        running: Flag indicating if the connector is active
        metrics: Processing metrics for monitoring
    """

    def __init__(self, spark_session, schema_name: str, table_name: str, target_table: str):
        """
        Initialize the CDC connector.

        Args:
            spark_session: The Spark session
            schema_name: Source schema name
            table_name: Source table name
            target_table: Target table name

        Raises:
            CDCError: If required parameters are missing
        """
        if not all([spark_session, schema_name, table_name, target_table]):
            raise CDCError("All parameters (spark_session, schema_name, table_name, target_table) are required")

        self.spark = spark_session
        self.schema_name = schema_name
        self.table_name = table_name
        self.target_table = target_table
        self.stream_query = None
        self.running = False
        self.metrics: List[ProcessingMetrics] = []

        logger.info(f"Initialized CDC connector for {schema_name}.{table_name} -> {target_table}")

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

            # Parse the JSON using simple string operations
            parsed_df = self._parse_debezium_message(df)

            # Add CDC metadata columns
            transformed_df = self._transform_data(parsed_df)

            # Write to target database
            self.stream_query = (transformed_df
                .writeStream
                .foreachBatch(self._process_batch)
                .option("checkpointLocation", f"{config.CHECKPOINT_LOCATION}/{self.schema_name}_{self.table_name}")
                .trigger(processingTime='5 seconds')  # Process every 5 seconds
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

    def _parse_debezium_message(self, df):
        """
        Parse Debezium messages using JSON path extraction.
        """
        # Convert value to string
        df = df.selectExpr("CAST(value AS STRING) as json_value", "CAST(key AS STRING) as key_value")

        # Extract common fields for all tables
        df = df.select(
            get_json_object(col("json_value"), "$.payload.__op").alias("__op"),
            get_json_object(col("json_value"), "$.payload.__ts_ms").cast(LongType()).alias("__ts_ms"),
            get_json_object(col("json_value"), "$.payload.__deleted").alias("__deleted"),
            col("key_value"),
            col("json_value")  # Keep the original JSON for table-specific parsing
        )

        # Add table-specific fields
        # Handle both snapshot records ($.payload.field) and change events ($.payload.after.field)
        if self.table_name == "customers":
            df = df.select(
                "*",
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.customer_id"),
                    get_json_object(col("json_value"), "$.payload.customer_id")
                ).cast(LongType()).alias("customer_id"),
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.customer_name"),
                    get_json_object(col("json_value"), "$.payload.customer_name")
                ).alias("customer_name"),
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.is_active"),
                    get_json_object(col("json_value"), "$.payload.is_active")
                ).cast(BooleanType()).alias("is_active"),
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.customer_address"),
                    get_json_object(col("json_value"), "$.payload.customer_address")
                ).alias("customer_address"),
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.updated_at"),
                    get_json_object(col("json_value"), "$.payload.updated_at")
                ).cast(LongType()).alias("updated_at"),
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.updated_by"),
                    get_json_object(col("json_value"), "$.payload.updated_by")
                ).cast(LongType()).alias("updated_by"),
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.created_at"),
                    get_json_object(col("json_value"), "$.payload.created_at")
                ).cast(LongType()).alias("created_at"),
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.created_by"),
                    get_json_object(col("json_value"), "$.payload.created_by")
                ).cast(LongType()).alias("created_by")
            )
        elif self.table_name == "products":
            df = df.select(
                "*",
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.product_id"),
                    get_json_object(col("json_value"), "$.payload.product_id")
                ).cast(LongType()).alias("product_id"),
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.product_name"),
                    get_json_object(col("json_value"), "$.payload.product_name")
                ).alias("product_name"),
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.barcode"),
                    get_json_object(col("json_value"), "$.payload.barcode")
                ).alias("barcode"),
                coalesce(
                    decode_decimal_udf(get_json_object(col("json_value"), "$.payload.after.unity_price")),
                    decode_decimal_udf(get_json_object(col("json_value"), "$.payload.unity_price")),
                    lit(0.0)
                ).alias("unity_price"),
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.is_active"),
                    get_json_object(col("json_value"), "$.payload.is_active")
                ).cast(BooleanType()).alias("is_active"),
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.updated_at"),
                    get_json_object(col("json_value"), "$.payload.updated_at")
                ).cast(LongType()).alias("updated_at"),
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.updated_by"),
                    get_json_object(col("json_value"), "$.payload.updated_by")
                ).cast(LongType()).alias("updated_by"),
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.created_at"),
                    get_json_object(col("json_value"), "$.payload.created_at")
                ).cast(LongType()).alias("created_at"),
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.created_by"),
                    get_json_object(col("json_value"), "$.payload.created_by")
                ).cast(LongType()).alias("created_by")
            )
        elif self.table_name == "orders":
            df = df.select(
                "*",
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.order_id"),
                    get_json_object(col("json_value"), "$.payload.order_id")
                ).cast(LongType()).alias("order_id"),
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.order_date"),
                    get_json_object(col("json_value"), "$.payload.order_date")
                ).alias("order_date_str"),
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.delivery_date"),
                    get_json_object(col("json_value"), "$.payload.delivery_date")
                ).alias("delivery_date_str"),
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.customer_id"),
                    get_json_object(col("json_value"), "$.payload.customer_id")
                ).cast(LongType()).alias("customer_id"),
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.status"),
                    get_json_object(col("json_value"), "$.payload.status")
                ).alias("status"),
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.updated_at"),
                    get_json_object(col("json_value"), "$.payload.updated_at")
                ).cast(LongType()).alias("updated_at"),
                coalesce(
                    get_json_object(col("json_value"), "$.payload.after.updated_by"),
                    get_json_object(col("json_value"), "$.payload.updated_by")
                ).cast(LongType()).alias("updated_by"),
                get_json_object(col("json_value"), "$.payload.after.created_at").cast(LongType()).alias("created_at"),
                get_json_object(col("json_value"), "$.payload.after.created_by").cast(LongType()).alias("created_by")
            )
        elif self.table_name == "order_items":
            df = df.select(
                "*",
                get_json_object(col("json_value"), "$.payload.after.order_item_id").cast(LongType()).alias("order_item_id"),
                get_json_object(col("json_value"), "$.payload.after.order_id").cast(LongType()).alias("order_id"),
                get_json_object(col("json_value"), "$.payload.after.product_id").cast(LongType()).alias("product_id"),
                get_json_object(col("json_value"), "$.payload.after.quanity").cast(IntegerType()).alias("quanity"),  # Note: typo in source
                get_json_object(col("json_value"), "$.payload.after.updated_at").cast(LongType()).alias("updated_at"),
                get_json_object(col("json_value"), "$.payload.after.updated_by").cast(LongType()).alias("updated_by"),
                get_json_object(col("json_value"), "$.payload.after.created_at").cast(LongType()).alias("created_at"),
                get_json_object(col("json_value"), "$.payload.after.created_by").cast(LongType()).alias("created_by")
            )

        # Drop the original JSON column
        df = df.drop("json_value")

        return df

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

        # Set effective_to to NULL for all new records (will be set later for DELETE operations)
        df = df.withColumn("effective_to", lit(None).cast(TimestampType()))

        # Set is_current to true for INSERT/UPDATE operations, false for DELETE operations
        df = df.withColumn("is_current",
                          when(col("operation_type") == "DELETE", False)
                          .otherwise(True))

        # Convert timestamp fields from Debezium format (milliseconds) to timestamp
        df = df.withColumn("updated_at",
                          when(col("updated_at").isNotNull(),
                               from_unixtime(col("updated_at") / 1000).cast(TimestampType()))
                          .otherwise(lit(None).cast(TimestampType())))

        df = df.withColumn("created_at",
                          when(col("created_at").isNotNull(),
                               from_unixtime(col("created_at") / 1000).cast(TimestampType()))
                          .otherwise(lit(None).cast(TimestampType())))

        # Handle date fields correctly for the orders table
        if self.table_name == "orders":
            # Convert string dates to proper date type
            # First, try to parse the date with to_date
            df = df.withColumn("order_date",
                              expr("to_date(order_date_str, 'yyyy-MM-dd')"))

            df = df.withColumn("delivery_date",
                              expr("to_date(delivery_date_str, 'yyyy-MM-dd')"))

            # Drop the string columns
            df = df.drop("order_date_str", "delivery_date_str")

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
        Process a batch of records with proper SCD Type 2 handling.

        Args:
            batch_df: The batch DataFrame
            batch_id: The batch ID
        """
        start_time = datetime.now()
        errors = []
        records_processed = 0
        records_written = 0

        logger.info(f"Processing batch {batch_id} for {self.schema_name}.{self.table_name}")

        try:
            if batch_df.isEmpty():
                logger.info(f"Batch {batch_id} is empty for {self.schema_name}.{self.table_name}")
                return

            records_processed = batch_df.count()

            # Filter out null primary key values
            pk_column = get_primary_key_column(self.table_name)
            if not pk_column:
                error_msg = f"No primary key column found for table {self.table_name}"
                logger.error(error_msg)
                errors.append(error_msg)
                raise CDCError(error_msg)

            initial_count = batch_df.count()
            batch_df = batch_df.filter(col(pk_column).isNotNull())
            filtered_count = batch_df.count()

            if initial_count != filtered_count:
                logger.warning(f"Filtered out {initial_count - filtered_count} records with null primary keys")

            if batch_df.isEmpty():
                logger.info(f"Batch {batch_id} has no valid records after filtering nulls for {self.schema_name}.{self.table_name}")
                return

            # Handle duplicates within the batch by keeping only the latest record per entity
            from pyspark.sql.window import Window
            from pyspark.sql.functions import row_number

            window_spec = Window.partitionBy(pk_column).orderBy(col("effective_from").desc())
            deduplicated_df = (batch_df
                .withColumn("row_num", row_number().over(window_spec))
                .filter(col("row_num") == 1)
                .drop("row_num"))

            dedup_count = deduplicated_df.count()
            if initial_count != dedup_count:
                logger.info(f"Deduplicated {initial_count - dedup_count} records in batch {batch_id}")

            if deduplicated_df.isEmpty():
                logger.info(f"Batch {batch_id} has no records after deduplication for {self.schema_name}.{self.table_name}")
                return

            # Update previous records first - mark existing records as not current
            self._update_previous_records(deduplicated_df)

            # Add processing timestamp to all records
            final_df = deduplicated_df.withColumn("processing_time", current_timestamp())

            # For DELETE operations, set is_current to false and effective_to to the operation time
            final_df = final_df.withColumn(
                "is_current",
                when(col("operation_type") == "DELETE", False)
                .otherwise(True)
            ).withColumn(
                "effective_to",
                when(col("operation_type") == "DELETE", col("effective_from"))
                .otherwise(lit(None).cast(TimestampType()))
            )

            # Write the batch to the target database
            (final_df
                .write
                .format("jdbc")
                .option("url", config.get_target_jdbc_url())
                .option("dbtable", self.target_table)
                .option("user", config.TARGET_DB_USER)
                .option("password", config.TARGET_DB_PASSWORD)
                .option("driver", "org.postgresql.Driver")
                .mode("append")
                .save())

            records_written = final_df.count()
            processing_time = (datetime.now() - start_time).total_seconds() * 1000

            # Store metrics
            metrics = ProcessingMetrics(
                batch_id=batch_id,
                records_processed=records_processed,
                records_written=records_written,
                processing_time_ms=processing_time,
                errors=errors
            )
            self.metrics.append(metrics)

            logger.info(f"Batch {batch_id} processed successfully for {self.schema_name}.{self.table_name} - "
                       f"{records_written} records written in {processing_time:.2f}ms")

        except Exception as e:
            error_msg = f"Error processing batch {batch_id} for {self.schema_name}.{self.table_name}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)

            # Store failed metrics
            processing_time = (datetime.now() - start_time).total_seconds() * 1000
            metrics = ProcessingMetrics(
                batch_id=batch_id,
                records_processed=records_processed,
                records_written=0,
                processing_time_ms=processing_time,
                errors=errors
            )
            self.metrics.append(metrics)
            raise CDCError(error_msg) from e

    def _update_previous_records(self, batch_df):
        """
        Update previous records to set effective_to and is_current for SCD Type 2.
        We want to ensure that only the latest record for each entity has is_current = true.

        Args:
            batch_df: DataFrame with records that need processing
        """
        # Get primary key column
        pk_column = get_primary_key_column(self.table_name)

        if not pk_column or batch_df.isEmpty():
            logger.warning(f"No primary key found for table {self.table_name} or empty batch")
            return

        # Get all primary keys that need updating from current batch
        pk_values = batch_df.select(pk_column).distinct().collect()
        pk_list = [str(row[pk_column]) for row in pk_values if row[pk_column] is not None]

        if not pk_list:
            return

        # Create a comma-separated string of values for the SQL IN clause
        pk_values_str = "(" + ",".join(pk_list) + ")"

        # Execute SQL to update previous records - setting all existing records for these PKs to not current
        # Only update records that are currently marked as current
        update_sql = f"""
        UPDATE {self.target_table}
        SET effective_to = current_timestamp(),
            is_current = false
        WHERE {pk_column} IN {pk_values_str}
        AND is_current = true
        """

        # Execute the update
        try:
            execute_query(update_sql)
            logger.info(f"Updated existing records for {len(pk_list)} entities in {self.table_name}")
        except Exception as e:
            logger.error(f"Error updating previous records: {str(e)}")
            raise CDCError(f"Failed to update previous records: {str(e)}") from e

    def get_metrics(self) -> List[ProcessingMetrics]:
        """Get processing metrics for monitoring."""
        return self.metrics.copy()

    def get_health_status(self) -> Dict[str, Any]:
        """
        Get health status of the CDC connector.

        Returns:
            Dictionary containing health information
        """
        if not self.running:
            return {
                "status": "stopped",
                "table": f"{self.schema_name}.{self.table_name}",
                "target": self.target_table,
                "running": False
            }

        recent_metrics = self.metrics[-10:] if self.metrics else []
        error_count = sum(1 for m in recent_metrics if m.errors)

        return {
            "status": "healthy" if error_count == 0 else "degraded",
            "table": f"{self.schema_name}.{self.table_name}",
            "target": self.target_table,
            "running": True,
            "recent_batches": len(recent_metrics),
            "recent_errors": error_count,
            "avg_processing_time_ms": sum(m.processing_time_ms for m in recent_metrics) / len(recent_metrics) if recent_metrics else 0
        }

# Helper functions
def get_primary_key_column(table_name: str) -> Optional[str]:
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
