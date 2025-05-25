"""
Utility functions for database operations.
"""
import logging
import time
from typing import Any, Dict, List, Optional, Tuple, Union
from contextlib import contextmanager
import pg8000
from src import config

# Configure logging
logger = logging.getLogger(__name__)

class DatabaseError(Exception):
    """Custom exception for database-related errors."""
    pass

class ConnectionPool:
    """Simple connection pool for database connections."""

    def __init__(self, max_connections: int = 5):
        self.max_connections = max_connections
        self.connections: List[pg8000.Connection] = []
        self.connection_info = {
            'user': config.TARGET_DB_USER,
            'password': config.TARGET_DB_PASSWORD,
            'host': config.TARGET_DB_HOST,
            'port': int(config.TARGET_DB_PORT),
            'database': config.TARGET_DB_NAME
        }

    def get_connection(self) -> pg8000.Connection:
        """Get a connection from the pool or create a new one."""
        if self.connections:
            return self.connections.pop()
        else:
            try:
                return pg8000.connect(**self.connection_info)
            except Exception as e:
                logger.error(f"Failed to create database connection: {e}")
                raise DatabaseError(f"Connection failed: {e}")

    def return_connection(self, conn: pg8000.Connection) -> None:
        """Return a connection to the pool."""
        if len(self.connections) < self.max_connections:
            try:
                # Test if connection is still valid
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                self.connections.append(conn)
            except Exception:
                # Connection is not valid, close it
                try:
                    conn.close()
                except Exception:
                    pass
        else:
            try:
                conn.close()
            except Exception:
                pass

# Global connection pool
_connection_pool = ConnectionPool()

@contextmanager
def get_db_connection(connection_info: Optional[Dict[str, Any]] = None):
    """
    Context manager for database connections.

    Args:
        connection_info: Optional connection information override

    Yields:
        Database connection
    """
    conn = None
    try:
        if connection_info:
            conn = pg8000.connect(**connection_info)
        else:
            conn = _connection_pool.get_connection()
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise DatabaseError(f"Database operation failed: {e}")
    finally:
        if conn:
            if connection_info:
                try:
                    conn.close()
                except Exception:
                    pass
            else:
                _connection_pool.return_connection(conn)

def execute_query(
    query: str,
    params: Optional[Union[Tuple, List]] = None,
    connection_info: Optional[Dict[str, Any]] = None,
    retry_count: int = 3,
    retry_delay: float = 1.0
) -> Optional[List[Tuple]]:
    """
    Execute a SQL query on the database with retry logic.

    Args:
        query: The SQL query to execute
        params: Query parameters (optional)
        connection_info: Connection information (optional)
        retry_count: Number of retry attempts
        retry_delay: Delay between retries in seconds

    Returns:
        List of rows returned by the query (for SELECT queries)

    Raises:
        DatabaseError: If the query fails after all retries
    """
    if not query or not query.strip():
        raise ValueError("Query cannot be empty")

    # Sanitize query to prevent basic SQL injection
    query = query.strip()

    last_exception = None

    for attempt in range(retry_count):
        try:
            with get_db_connection(connection_info) as conn:
                with conn.cursor() as cursor:
                    logger.debug(f"Executing query: {query[:100]}{'...' if len(query) > 100 else ''}")

                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)

                    # For SELECT queries, fetch results
                    if query.upper().lstrip().startswith('SELECT'):
                        results = cursor.fetchall()
                        logger.debug(f"Query returned {len(results)} rows")
                        return results
                    else:
                        # For INSERT/UPDATE/DELETE, commit the transaction
                        conn.commit()
                        logger.debug("Query executed successfully")
                        return None

        except Exception as e:
            last_exception = e
            logger.warning(f"Query execution attempt {attempt + 1} failed: {e}")

            if attempt < retry_count - 1:
                time.sleep(retry_delay)
            else:
                logger.error(f"Query execution failed after {retry_count} attempts: {e}")

    # If we get here, all retries failed
    raise DatabaseError(f"Query execution failed after {retry_count} attempts: {last_exception}")

def execute_transaction(
    queries: List[Tuple[str, Optional[Union[Tuple, List]]]],
    connection_info: Optional[Dict[str, Any]] = None
) -> None:
    """
    Execute multiple queries in a single transaction.

    Args:
        queries: List of (query, params) tuples
        connection_info: Optional connection information override

    Raises:
        DatabaseError: If any query in the transaction fails
    """
    if not queries:
        raise ValueError("Queries list cannot be empty")

    try:
        with get_db_connection(connection_info) as conn:
            with conn.cursor() as cursor:
                for query, params in queries:
                    if not query or not query.strip():
                        raise ValueError("Query in transaction cannot be empty")

                    logger.debug(f"Executing transaction query: {query[:100]}{'...' if len(query) > 100 else ''}")

                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)

                conn.commit()
                logger.info(f"Transaction with {len(queries)} queries executed successfully")

    except Exception as e:
        logger.error(f"Transaction execution failed: {e}")
        raise DatabaseError(f"Transaction failed: {e}")

def test_connection(connection_info: Optional[Dict[str, Any]] = None) -> bool:
    """
    Test database connectivity.

    Args:
        connection_info: Optional connection information override

    Returns:
        True if connection successful, False otherwise
    """
    try:
        result = execute_query("SELECT 1", connection_info=connection_info)
        return result is not None and len(result) > 0
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False

def get_table_schema(table_name: str, connection_info: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Get schema information for a table.

    Args:
        table_name: Name of the table (can include schema)
        connection_info: Optional connection information override

    Returns:
        List of dictionaries with column information

    Raises:
        DatabaseError: If the query fails
    """
    if not table_name or not table_name.strip():
        raise ValueError("Table name cannot be empty")

    # Parse schema and table name
    if '.' in table_name:
        schema_name, table_name = table_name.split('.', 1)
    else:
        schema_name = 'public'  # Default schema

    query = """
    SELECT
        column_name,
        data_type,
        is_nullable,
        column_default,
        ordinal_position
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    """

    try:
        results = execute_query(query, (schema_name, table_name), connection_info)
        if not results:
            logger.warning(f"No schema information found for table {schema_name}.{table_name}")
            return []

        columns = []
        for row in results:
            columns.append({
                'column_name': row[0],
                'data_type': row[1],
                'is_nullable': row[2] == 'YES',
                'column_default': row[3],
                'ordinal_position': row[4]
            })

        logger.info(f"Retrieved schema for table {schema_name}.{table_name}: {len(columns)} columns")
        return columns

    except Exception as e:
        logger.error(f"Failed to get schema for table {schema_name}.{table_name}: {e}")
        raise DatabaseError(f"Schema query failed: {e}")

        conn.commit()

        return results

    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def check_table_exists(schema, table, connection_info=None):
    """
    Check if a table exists in the database.

    Args:
        schema: Schema name
        table: Table name
        connection_info: Connection information (optional)

    Returns:
        True if the table exists, False otherwise
    """
    query = """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s
        AND table_name = %s
    )
    """

    result = execute_query(query, (schema, table), connection_info)

    return result[0][0] if result else False

def get_column_names(schema, table, connection_info=None):
    """
    Get the column names for a table.

    Args:
        schema: Schema name
        table: Table name
        connection_info: Connection information (optional)

    Returns:
        List of column names
    """
    query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = %s
    AND table_name = %s
    ORDER BY ordinal_position
    """

    result = execute_query(query, (schema, table), connection_info)

    return [row[0] for row in result] if result else []

def create_schema_if_not_exists(schema, connection_info=None):
    """
    Create a schema if it does not exist.

    Args:
        schema: Schema name
        connection_info: Connection information (optional)
    """
    query = f"CREATE SCHEMA IF NOT EXISTS {schema}"

    execute_query(query, connection_info=connection_info)

    logger.info(f"Schema '{schema}' created or already exists")

def drop_table_if_exists(schema, table, connection_info=None):
    """
    Drop a table if it exists.

    Args:
        schema: Schema name
        table: Table name
        connection_info: Connection information (optional)
    """
    query = f"DROP TABLE IF EXISTS {schema}.{table}"

    execute_query(query, connection_info=connection_info)

    logger.info(f"Table '{schema}.{table}' dropped if it existed")
