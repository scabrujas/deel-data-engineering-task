"""
Configuration module for the Spark streaming application.
"""
import os
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass
from dotenv import load_dotenv

# Configure logging for this module
logger = logging.getLogger(__name__)

# Load environment variables from .env file if exists
load_dotenv()

@dataclass
class DatabaseConfig:
    """Database configuration class."""
    host: str
    port: int
    user: str
    password: str
    database: str

    def get_jdbc_url(self) -> str:
        """Get JDBC URL for this database configuration."""
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    def get_connection_properties(self) -> Dict[str, str]:
        """Get connection properties for JDBC."""
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver"
        }

@dataclass
class KafkaConfig:
    """Kafka configuration class."""
    bootstrap_servers: str
    group_id: str
    topic_prefix: str

    def get_topic_name(self, schema_name: str, table_name: str) -> str:
        """Get Kafka topic name for a table."""
        return f"{self.topic_prefix}.{schema_name}.{table_name}"

@dataclass
class SparkConfig:
    """Spark configuration class."""
    master: str
    checkpoint_location: str

@dataclass
class TableConfig:
    """Table configuration class."""
    schema: str
    table: str
    target_table: str

# Configuration validation
def _get_env_var(key: str, default: Optional[str] = None, required: bool = False) -> str:
    """Get environment variable with validation."""
    value = os.getenv(key, default)
    if required and not value:
        raise ValueError(f"Required environment variable {key} is not set")
    return value

def _get_env_int(key: str, default: int, required: bool = False) -> int:
    """Get integer environment variable with validation."""
    value_str = _get_env_var(key, str(default), required)
    try:
        return int(value_str)
    except ValueError:
        logger.warning(f"Invalid integer value for {key}: {value_str}, using default: {default}")
        return default

# Source Database Configuration
SOURCE_DB_CONFIG = DatabaseConfig(
    host=_get_env_var("SOURCE_DB_HOST", "localhost"),
    port=_get_env_int("SOURCE_DB_PORT", 5432),
    user=_get_env_var("SOURCE_DB_USER", "cdc_user"),
    password=_get_env_var("SOURCE_DB_PASSWORD", "cdc_1234"),
    database=_get_env_var("SOURCE_DB_NAME", "finance_db")
)

# Target Database Configuration
TARGET_DB_CONFIG = DatabaseConfig(
    host=_get_env_var("TARGET_DB_HOST", "localhost"),
    port=_get_env_int("TARGET_DB_PORT", 5432),
    user=_get_env_var("TARGET_DB_USER", "analytics_user"),
    password=_get_env_var("TARGET_DB_PASSWORD", "1234"),
    database=_get_env_var("TARGET_DB_NAME", "analytics_db")
)

# Kafka Configuration
KAFKA_CONFIG = KafkaConfig(
    bootstrap_servers=_get_env_var("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    group_id=_get_env_var("KAFKA_GROUP_ID", "spark-cdc-consumer"),
    topic_prefix=_get_env_var("KAFKA_TOPIC_PREFIX", "finance-db")
)

# Spark Configuration
SPARK_CONFIG = SparkConfig(
    master=_get_env_var("SPARK_MASTER", "local[*]"),
    checkpoint_location=_get_env_var("CHECKPOINT_LOCATION", "/tmp/spark-checkpoints")
)

# Legacy configuration variables (for backward compatibility)
SOURCE_DB_HOST = SOURCE_DB_CONFIG.host
SOURCE_DB_PORT = str(SOURCE_DB_CONFIG.port)
SOURCE_DB_USER = SOURCE_DB_CONFIG.user
SOURCE_DB_PASSWORD = SOURCE_DB_CONFIG.password
SOURCE_DB_NAME = SOURCE_DB_CONFIG.database
SOURCE_REPLICATION_SLOT = _get_env_var("SOURCE_REPLICATION_SLOT", "cdc_pgoutput")
SOURCE_PUBLICATION_NAME = _get_env_var("SOURCE_PUBLICATION_NAME", "cdc_publication")

TARGET_DB_HOST = TARGET_DB_CONFIG.host
TARGET_DB_PORT = str(TARGET_DB_CONFIG.port)
TARGET_DB_USER = TARGET_DB_CONFIG.user
TARGET_DB_PASSWORD = TARGET_DB_CONFIG.password
TARGET_DB_NAME = TARGET_DB_CONFIG.database

KAFKA_BOOTSTRAP_SERVERS = KAFKA_CONFIG.bootstrap_servers
KAFKA_GROUP_ID = KAFKA_CONFIG.group_id
KAFKA_TOPIC_PREFIX = KAFKA_CONFIG.topic_prefix

SPARK_MASTER = SPARK_CONFIG.master
CHECKPOINT_LOCATION = SPARK_CONFIG.checkpoint_location

# Tables to stream
TABLES: List[Dict[str, str]] = [
    {"schema": "operations", "table": "customers", "target_table": "analytics.customers_history"},
    {"schema": "operations", "table": "products", "target_table": "analytics.products_history"},
    {"schema": "operations", "table": "orders", "target_table": "analytics.orders_history"},
    {"schema": "operations", "table": "order_items", "target_table": "analytics.order_items_history"}
]

TABLE_CONFIGS: List[TableConfig] = [
    TableConfig(schema=table["schema"], table=table["table"], target_table=table["target_table"])
    for table in TABLES
]

# Legacy functions (for backward compatibility)
def get_source_jdbc_url() -> str:
    """Get source database JDBC URL."""
    return SOURCE_DB_CONFIG.get_jdbc_url()

def get_target_jdbc_url() -> str:
    """Get target database JDBC URL."""
    return TARGET_DB_CONFIG.get_jdbc_url()

def get_source_db_properties() -> Dict[str, str]:
    """Get source database connection properties."""
    return SOURCE_DB_CONFIG.get_connection_properties()

def get_target_db_properties() -> Dict[str, str]:
    """Get target database connection properties."""
    return TARGET_DB_CONFIG.get_connection_properties()

def get_kafka_topic(schema_name: str, table_name: str) -> str:
    """Get Kafka topic name for a table."""
    return KAFKA_CONFIG.get_topic_name(schema_name, table_name)

def validate_configuration() -> None:
    """Validate all configuration settings."""
    logger.info("Validating configuration...")

    # Validate database configurations
    for db_name, db_config in [("Source", SOURCE_DB_CONFIG), ("Target", TARGET_DB_CONFIG)]:
        if not db_config.host:
            raise ValueError(f"{db_name} database host is not configured")
        if not (1 <= db_config.port <= 65535):
            raise ValueError(f"{db_name} database port {db_config.port} is not valid")
        if not db_config.user:
            raise ValueError(f"{db_name} database user is not configured")
        if not db_config.password:
            logger.warning(f"{db_name} database password is empty")
        if not db_config.database:
            raise ValueError(f"{db_name} database name is not configured")

    # Validate Kafka configuration
    if not KAFKA_CONFIG.bootstrap_servers:
        raise ValueError("Kafka bootstrap servers not configured")
    if not KAFKA_CONFIG.group_id:
        raise ValueError("Kafka group ID not configured")
    if not KAFKA_CONFIG.topic_prefix:
        raise ValueError("Kafka topic prefix not configured")

    # Validate Spark configuration
    if not SPARK_CONFIG.master:
        raise ValueError("Spark master not configured")
    if not SPARK_CONFIG.checkpoint_location:
        raise ValueError("Spark checkpoint location not configured")

    logger.info("Configuration validation completed successfully")

# Validate configuration on module import
try:
    validate_configuration()
except Exception as e:
    logger.error(f"Configuration validation failed: {e}")
    # Don't raise here to allow for graceful degradation
