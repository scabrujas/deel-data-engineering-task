from fastapi import FastAPI, Query, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any
import psycopg2
import psycopg2.extras
import os
from pydantic import BaseModel, Field, validator
import logging
from contextlib import contextmanager
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Finance Analytics API",
    description="API for accessing finance analytics data from the data pipeline",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection parameters with validation
DB_HOST = os.environ.get("TARGET_DB_HOST", "localhost")
DB_PORT = int(os.environ.get("TARGET_DB_PORT", "5433"))
DB_NAME = os.environ.get("TARGET_DB_NAME", "analytics_db")
DB_USER = os.environ.get("TARGET_DB_USER", "analytics_user")
DB_PASS = os.environ.get("TARGET_DB_PASSWORD", "1234")

# Validate database configuration
if not all([DB_HOST, DB_NAME, DB_USER, DB_PASS]):
    logger.error("Missing required database configuration")
    raise ValueError("Database configuration is incomplete")

# Custom exceptions
class DatabaseConnectionError(Exception):
    """Raised when database connection fails."""
    pass

class QueryExecutionError(Exception):
    """Raised when query execution fails."""
    pass

# Pydantic models for response schemas with validation
class Order(BaseModel):
    delivery_date: Optional[str] = Field(None, description="Delivery date in YYYY-MM-DD format")
    status: str = Field(..., description="Order status")
    open_order_count: int = Field(..., ge=0, description="Number of open orders")

    @validator('delivery_date')
    def validate_delivery_date(cls, v):
        if v is not None and v.strip() == '':
            return None
        return v

class TopDeliveryDate(BaseModel):
    delivery_date: Optional[str] = Field(None, description="Delivery date in YYYY-MM-DD format")
    open_order_count: int = Field(..., ge=0, description="Number of open orders")

    @validator('delivery_date')
    def validate_delivery_date(cls, v):
        if v is not None and v.strip() == '':
            return None
        return v

class PendingItem(BaseModel):
    product_id: int = Field(..., gt=0, description="Product ID")
    product_name: str = Field(..., min_length=1, description="Product name")
    pending_item_count: int = Field(..., ge=0, description="Number of pending items")

class Customer(BaseModel):
    customer_id: int = Field(..., gt=0, description="Customer ID")
    customer_name: str = Field(..., min_length=1, description="Customer name")
    pending_order_count: int = Field(..., ge=0, description="Number of pending orders")

class HealthCheck(BaseModel):
    status: str
    timestamp: str
    database_status: str

# Connection pool management
class ConnectionPool:
    def __init__(self, max_connections: int = 5):
        self.max_connections = max_connections
        self.connections = []

    def get_connection(self):
        if self.connections:
            return self.connections.pop()
        return psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )

    def return_connection(self, conn):
        if len(self.connections) < self.max_connections:
            self.connections.append(conn)
        else:
            conn.close()

# Global connection pool
connection_pool = ConnectionPool()

@contextmanager
def get_db_connection():
    """Context manager for database connections with proper error handling."""
    conn = None
    try:
        conn = connection_pool.get_connection()
        yield conn
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {str(e)}")
        raise DatabaseConnectionError(f"Database connection failed: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected database error: {str(e)}")
        raise DatabaseConnectionError(f"Unexpected database error: {str(e)}")
    finally:
        if conn:
            try:
                connection_pool.return_connection(conn)
            except Exception as e:
                logger.error(f"Error returning connection to pool: {str(e)}")

def execute_query(query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """
    Execute SQL query with proper error handling and connection management.

    Args:
        query: SQL query to execute
        params: Query parameters

    Returns:
        List of query results as dictionaries

    Raises:
        QueryExecutionError: If query execution fails
    """
    if not query or not query.strip():
        raise ValueError("Query cannot be empty")

    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                logger.debug(f"Executing query: {query[:100]}{'...' if len(query) > 100 else ''}")

                start_time = time.time()
                cur.execute(query, params)
                execution_time = time.time() - start_time

                results = cur.fetchall()
                logger.debug(f"Query executed in {execution_time:.3f}s, returned {len(results)} rows")

                return [dict(row) for row in results]

    except psycopg2.Error as e:
        logger.error(f"Database query error: {str(e)}")
        raise QueryExecutionError(f"Query execution failed: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected query error: {str(e)}")
        raise QueryExecutionError(f"Unexpected query error: {str(e)}")

# Error handlers
@app.exception_handler(DatabaseConnectionError)
async def database_connection_error_handler(request, exc):
    logger.error(f"Database connection error for {request.url}: {exc}")
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": "Database service unavailable", "error_type": "database_connection"}
    )

@app.exception_handler(QueryExecutionError)
async def query_execution_error_handler(request, exc):
    logger.error(f"Query execution error for {request.url}: {exc}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Query execution failed", "error_type": "query_execution"}
    )

@app.exception_handler(ValueError)
async def value_error_handler(request, exc):
    logger.warning(f"Value error for {request.url}: {exc}")
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": str(exc), "error_type": "validation_error"}
    )

# Health check endpoint
@app.get("/health", response_model=HealthCheck)
async def health_check():
    """Health check endpoint to verify API and database connectivity."""
    from datetime import datetime

    try:
        # Test database connection
        result = execute_query("SELECT 1 as test")
        db_status = "healthy" if result and result[0]["test"] == 1 else "unhealthy"
    except Exception as e:
        logger.error(f"Health check database test failed: {e}")
        db_status = "unhealthy"

    return HealthCheck(
        status="healthy" if db_status == "healthy" else "degraded",
        timestamp=datetime.utcnow().isoformat(),
        database_status=db_status
    )

# Root endpoint
@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Welcome to the Finance Analytics API",
        "version": "1.0.0",
        "docs_url": "/docs",
        "health_check": "/health"
    }

# GET /orders?status=open: To retrieve information about the number of open orders by DELIVERY_DATE and STATUS
@app.get("/orders", response_model=List[Order])
async def get_orders(status: Optional[str] = Query("open", description="Filter orders by status (open, pending, completed, cancelled, all)")):
    """
    Get information about the number of orders by DELIVERY_DATE and STATUS.
    Uses the exact query specified: COUNT(*) of orders grouped by delivery_date and status.

    Parameters:
    - status: Filter orders by status (default: 'open')

    Returns:
    - List of orders with delivery date, status, and count
    """
    # Validate the status parameter
    valid_statuses = ["open", "pending", "completed", "cancelled", "all"]
    if status:
        status = status.lower()

    if status not in valid_statuses:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid status '{status}'. Must be one of: {', '.join(valid_statuses)}"
        )

    try:
        # Use the exact query from the user's specification
        if status == "open":
            # For 'open' status, include both 'OPEN' and 'PENDING' as per requirements
            query = """
                SELECT
                    delivery_date::text,
                    status,
                    COUNT(*) as open_order_count
                FROM
                    analytics.orders_history
                WHERE
                    is_current = true
                    AND status IN ('OPEN', 'PENDING')
                GROUP BY
                    delivery_date, status
                ORDER BY
                    delivery_date, status
            """
            results = execute_query(query)
        elif status == "all":
            # For 'all' status, return all statuses
            query = """
                SELECT
                    delivery_date::text,
                    status,
                    COUNT(*) as open_order_count
                FROM
                    analytics.orders_history
                WHERE
                    is_current = true
                GROUP BY
                    delivery_date, status
                ORDER BY
                    delivery_date, status
            """
            results = execute_query(query)
        else:
            # For specific status
            query = """
                SELECT
                    delivery_date::text,
                    status,
                    COUNT(*) as open_order_count
                FROM
                    analytics.orders_history
                WHERE
                    is_current = true
                    AND status = %s
                GROUP BY
                    delivery_date, status
                ORDER BY
                    delivery_date, status
            """
            results = execute_query(query, (status.upper(),))

        # Convert the results to the expected format
        orders = []
        for row in results:
            orders.append({
                "delivery_date": row["delivery_date"],
                "status": row["status"],
                "open_order_count": row["open_order_count"]
            })

        logger.info(f"Retrieved {len(orders)} order records for status: {status}")
        return orders

    except QueryExecutionError:
        raise  # Re-raise to be handled by the error handler
    except Exception as e:
        logger.error(f"Unexpected error in get_orders: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while fetching orders"
        )

# GET /orders/top?limit=3: To retrieve information about the Top delivery dates with more open orders
@app.get("/orders/top", response_model=List[TopDeliveryDate])
async def get_top_delivery_dates(limit: int = Query(3, description="Number of top delivery dates to return")):
    """
    Get information about the top delivery dates with more open orders.

    Parameters:
    - limit: Number of top delivery dates to return (default: 3)

    Returns:
    - List of top delivery dates with open order count
    """
    # Validate the limit parameter
    if limit <= 0:
        raise HTTPException(status_code=400, detail="Limit must be greater than 0")

    # If limit is 3, we can use the pre-defined view
    if limit == 3:
        query = """
            SELECT delivery_date::text, open_order_count
            FROM analytics.top_delivery_dates_open_orders
        """
        results = execute_query(query)
    else:
        query = """
            SELECT delivery_date::text, COUNT(*) as open_order_count
            FROM analytics.orders_history
            WHERE is_current = true AND status IN ('OPEN', 'PENDING')
            GROUP BY delivery_date
            ORDER BY open_order_count DESC
            LIMIT %s
        """
        results = execute_query(query, (limit,))

    # Convert the results to a list of dictionaries
    top_dates = []
    for row in results:
        top_dates.append({
            "delivery_date": row["delivery_date"],
            "open_order_count": row["open_order_count"]
        })

    return top_dates

# GET /orders/product: To retrieve information about the number of open pending items by PRODUCT_ID
@app.get("/orders/product", response_model=List[PendingItem])
async def get_pending_items_by_product():
    """
    Get information about the number of open pending items by PRODUCT_ID.

    Returns:
    - List of products with pending item count
    """
    # Using the pre-defined view
    query = """
        SELECT product_id, product_name, pending_item_count
        FROM analytics.open_pending_items_by_product
    """
    results = execute_query(query)

    # Convert the results to a list of dictionaries
    pending_items = []
    for row in results:
        pending_items.append({
            "product_id": row["product_id"],
            "product_name": row["product_name"],
            "pending_item_count": row["pending_item_count"]
        })

    return pending_items

# GET /orders/customers/?status=open&limit=3: To retrieve information about top Customers with more pending orders
@app.get("/orders/customers", response_model=List[Customer])
async def get_top_customers(status: str = Query("pending", description="Filter by order status"),
                          limit: int = Query(3, description="Number of top customers to return")):
    """
    Get information about top customers with more orders of a specific status.

    Parameters:
    - status: Filter by order status (default: 'pending')
    - limit: Number of top customers to return (default: 3)

    Returns:
    - List of top customers with order count
    """
    # Validate the limit parameter
    if limit <= 0:
        raise HTTPException(status_code=400, detail="Limit must be greater than 0")

    # Validate the status parameter
    valid_statuses = ["open", "pending", "completed", "cancelled"]
    status = status.lower()

    if status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {', '.join(valid_statuses)}")

    # If status is pending and limit is 3, we can use the pre-defined view
    if status == "pending" and limit == 3:
        query = """
            SELECT customer_id, customer_name, pending_order_count
            FROM analytics.top_customers_with_pending_orders
        """
        results = execute_query(query)
    else:
        query = """
            SELECT c.customer_id, c.customer_name, COUNT(DISTINCT o.order_id) as pending_order_count
            FROM analytics.customers_history c
            JOIN analytics.orders_history o ON c.customer_id = o.customer_id
            WHERE c.is_current = true
            AND o.is_current = true
            AND o.status = %s
            GROUP BY c.customer_id, c.customer_name
            ORDER BY pending_order_count DESC
            LIMIT %s
        """
        results = execute_query(query, (status.upper(), limit))

    # Convert the results to a list of dictionaries
    customers = []
    for row in results:
        customers.append({
            "customer_id": row["customer_id"],
            "customer_name": row["customer_name"],
            "pending_order_count": row["pending_order_count"]
        })

    return customers

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
