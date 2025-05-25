-- Target database initialization script for analytics
CREATE SCHEMA IF NOT EXISTS analytics;

-- Historical customers table with versioning
CREATE TABLE IF NOT EXISTS analytics.customers_history(
    id                  BIGSERIAL NOT NULL PRIMARY KEY,
    customer_id         BIGINT NOT NULL,
    customer_name       VARCHAR(500) NOT NULL,
    is_active           BOOLEAN NOT NULL,
    customer_address    VARCHAR(500),
    source_updated_at   TIMESTAMP(3),
    source_updated_by   BIGINT,
    source_created_at   TIMESTAMP(3),
    source_created_by   BIGINT,
    operation_type      VARCHAR(10), -- INSERT, UPDATE, DELETE
    effective_from      TIMESTAMP(3) NOT NULL,
    effective_to        TIMESTAMP(3), -- NULL means current version
    is_current          BOOLEAN NOT NULL DEFAULT TRUE
);

-- Create index for faster lookups
CREATE INDEX idx_customers_history_customer_id ON analytics.customers_history(customer_id);
CREATE INDEX idx_customers_history_is_current ON analytics.customers_history(is_current);

-- Historical products table with versioning
CREATE TABLE IF NOT EXISTS analytics.products_history(
    id                  BIGSERIAL NOT NULL PRIMARY KEY,
    product_id          BIGINT NOT NULL,
    product_name        VARCHAR(500) NOT NULL,
    barcode             VARCHAR(26) NOT NULL,
    unity_price         DECIMAL NOT NULL,
    is_active           BOOLEAN,
    source_updated_at   TIMESTAMP(3),
    source_updated_by   BIGINT,
    source_created_at   TIMESTAMP(3),
    source_created_by   BIGINT,
    operation_type      VARCHAR(10), -- INSERT, UPDATE, DELETE
    effective_from      TIMESTAMP(3) NOT NULL,
    effective_to        TIMESTAMP(3), -- NULL means current version
    is_current          BOOLEAN NOT NULL DEFAULT TRUE
);

-- Create index for faster lookups
CREATE INDEX idx_products_history_product_id ON analytics.products_history(product_id);
CREATE INDEX idx_products_history_is_current ON analytics.products_history(is_current);

-- Historical orders table with versioning
CREATE TABLE IF NOT EXISTS analytics.orders_history(
    id                  BIGSERIAL NOT NULL PRIMARY KEY,
    order_id            BIGINT NOT NULL,
    order_date          DATE,
    delivery_date       DATE,
    customer_id         BIGINT,
    status              VARCHAR,
    source_updated_at   TIMESTAMP(3),
    source_updated_by   BIGINT,
    source_created_at   TIMESTAMP(3),
    source_created_by   BIGINT,
    operation_type      VARCHAR(10), -- INSERT, UPDATE, DELETE
    effective_from      TIMESTAMP(3) NOT NULL,
    effective_to        TIMESTAMP(3), -- NULL means current version
    is_current          BOOLEAN NOT NULL DEFAULT TRUE
);

-- Create index for faster lookups
CREATE INDEX idx_orders_history_order_id ON analytics.orders_history(order_id);
CREATE INDEX idx_orders_history_is_current ON analytics.orders_history(is_current);

-- Historical order items table with versioning
CREATE TABLE IF NOT EXISTS analytics.order_items_history(
    id                  BIGSERIAL NOT NULL PRIMARY KEY,
    order_item_id       BIGINT NOT NULL,
    order_id            BIGINT,
    product_id          BIGINT,
    quantity            INTEGER,
    source_updated_at   TIMESTAMP(3),
    source_updated_by   BIGINT,
    source_created_at   TIMESTAMP(3),
    source_created_by   BIGINT,
    operation_type      VARCHAR(10), -- INSERT, UPDATE, DELETE
    effective_from      TIMESTAMP(3) NOT NULL,
    effective_to        TIMESTAMP(3), -- NULL means current version
    is_current          BOOLEAN NOT NULL DEFAULT TRUE
);

-- Create index for faster lookups
CREATE INDEX idx_order_items_history_order_item_id ON analytics.order_items_history(order_item_id);
CREATE INDEX idx_order_items_history_is_current ON analytics.order_items_history(is_current);

-- Create analytics views for common queries
-- 1. View for open orders by delivery date and status
CREATE OR REPLACE VIEW analytics.open_orders_by_delivery_date_status AS
SELECT
    delivery_date,
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
    delivery_date, status;

-- 2. View for top delivery dates with most open orders
CREATE OR REPLACE VIEW analytics.top_delivery_dates_open_orders AS
SELECT
    delivery_date,
    COUNT(*) as open_order_count
FROM
    analytics.orders_history
WHERE
    is_current = true
    AND status IN ('OPEN', 'PENDING')
GROUP BY
    delivery_date
ORDER BY
    open_order_count DESC
LIMIT 3;

-- 3. View for open pending items by product
CREATE OR REPLACE VIEW analytics.open_pending_items_by_product AS
SELECT
    oi.product_id,
    p.product_name,
    SUM(oi.quantity) as pending_item_count
FROM
    analytics.order_items_history oi
JOIN
    analytics.orders_history o ON oi.order_id = o.order_id
JOIN
    analytics.products_history p ON oi.product_id = p.product_id
WHERE
    oi.is_current = true
    AND o.is_current = true
    AND p.is_current = true
    AND o.status IN ('PROCESSING', 'REPROCESSING', 'PENDING')
	AND oi.product_id IS NOT NULL
    AND oi.quantity > 0
GROUP BY
    oi.product_id, p.product_name
ORDER BY
    pending_item_count DESC;

-- 4. View for top customers with most pending orders
CREATE OR REPLACE VIEW analytics.top_customers_with_pending_orders AS
SELECT
    c.customer_id,
    c.customer_name,
    COUNT(DISTINCT o.order_id) as pending_order_count
FROM
    analytics.customers_history c
JOIN
    analytics.orders_history o ON c.customer_id = o.customer_id
WHERE
    c.is_current = true
    AND o.is_current = true
    AND o.status = 'PENDING'
GROUP BY
    c.customer_id, c.customer_name
ORDER BY
    pending_order_count DESC
LIMIT 3;

-- Create directory for target database data
CREATE ROLE spark_user WITH LOGIN PASSWORD 'spark1234';
GRANT ALL PRIVILEGES ON SCHEMA analytics TO spark_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO spark_user;
-- Grant privileges on views specifically
GRANT SELECT ON analytics.open_orders_by_delivery_date_status TO spark_user;
GRANT SELECT ON analytics.top_delivery_dates_open_orders TO spark_user;
GRANT SELECT ON analytics.open_pending_items_by_product TO spark_user;
GRANT SELECT ON analytics.top_customers_with_pending_orders TO spark_user;
