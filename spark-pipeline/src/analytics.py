"""
Sample analytics queries module.
"""
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, max, min, desc, asc

from src import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session():
    """
    Create and configure a Spark session for analytics.
    """
    logger.info("Initializing Spark session for analytics...")

    # Create Spark session
    spark = (SparkSession.builder
        .appName("PostgreSQL Analytics")
        .master(config.SPARK_MASTER)
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .getOrCreate())

    # Set log level
    spark.sparkContext.setLogLevel("WARN")

    return spark

def run_customer_order_analysis():
    """
    Run an analysis of customer orders.
    """
    spark = create_spark_session()

    logger.info("Running customer order analysis...")

    # Load current customers
    customers_df = (spark.read
        .format("jdbc")
        .option("url", config.get_target_jdbc_url())
        .option("dbtable", "analytics.customers_history")
        .option("user", config.TARGET_DB_USER)
        .option("password", config.TARGET_DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "(SELECT * FROM analytics.customers_history WHERE is_current = true) AS current_customers")
        .load())

    # Load current orders
    orders_df = (spark.read
        .format("jdbc")
        .option("url", config.get_target_jdbc_url())
        .option("dbtable", "analytics.orders_history")
        .option("user", config.TARGET_DB_USER)
        .option("password", config.TARGET_DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "(SELECT * FROM analytics.orders_history WHERE is_current = true) AS current_orders")
        .load())

    # Load current order items
    order_items_df = (spark.read
        .format("jdbc")
        .option("url", config.get_target_jdbc_url())
        .option("dbtable", "analytics.order_items_history")
        .option("user", config.TARGET_DB_USER)
        .option("password", config.TARGET_DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "(SELECT * FROM analytics.order_items_history WHERE is_current = true) AS current_order_items")
        .load())

    # Load current products
    products_df = (spark.read
        .format("jdbc")
        .option("url", config.get_target_jdbc_url())
        .option("dbtable", "analytics.products_history")
        .option("user", config.TARGET_DB_USER)
        .option("password", config.TARGET_DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "(SELECT * FROM analytics.products_history WHERE is_current = true) AS current_products")
        .load())

    # Register temporary views
    customers_df.createOrReplaceTempView("customers")
    orders_df.createOrReplaceTempView("orders")
    order_items_df.createOrReplaceTempView("order_items")
    products_df.createOrReplaceTempView("products")

    # Top customers by order count
    top_customers_by_orders = spark.sql("""
        SELECT
            c.customer_id,
            c.customer_name,
            COUNT(o.order_id) AS order_count
        FROM
            customers c
        JOIN
            orders o ON c.customer_id = o.customer_id
        GROUP BY
            c.customer_id, c.customer_name
        ORDER BY
            order_count DESC
        LIMIT 10
    """)

    logger.info("Top customers by order count:")
    top_customers_by_orders.show()

    # Top products by quantity ordered
    top_products_by_quantity = spark.sql("""
        SELECT
            p.product_id,
            p.product_name,
            SUM(oi.quantity) AS total_quantity
        FROM
            products p
        JOIN
            order_items oi ON p.product_id = oi.product_id
        GROUP BY
            p.product_id, p.product_name
        ORDER BY
            total_quantity DESC
        LIMIT 10
    """)

    logger.info("Top products by quantity ordered:")
    top_products_by_quantity.show()

    # Orders by status
    orders_by_status = spark.sql("""
        SELECT
            status,
            COUNT(order_id) AS order_count
        FROM
            orders
        GROUP BY
            status
        ORDER BY
            order_count DESC
    """)

    logger.info("Orders by status:")
    orders_by_status.show()

    # Time-based analysis using historical data
    # Get customer count over time (using the history tables)
    customers_over_time = (spark.read
        .format("jdbc")
        .option("url", config.get_target_jdbc_url())
        .option("dbtable", """
            (SELECT
                DATE_TRUNC('day', effective_from) AS day,
                COUNT(DISTINCT customer_id) AS customer_count
             FROM
                analytics.customers_history
             GROUP BY
                DATE_TRUNC('day', effective_from)
             ORDER BY
                day) AS customers_over_time
        """)
        .option("user", config.TARGET_DB_USER)
        .option("password", config.TARGET_DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load())

    logger.info("Customer count over time:")
    customers_over_time.show(truncate=False)

    # Close the Spark session
    spark.stop()

if __name__ == "__main__":
    run_customer_order_analysis()
