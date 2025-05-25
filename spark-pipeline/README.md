# Spark PostgreSQL CDC Pipeline

This project implements a Change Data Capture (CDC) pipeline using Apache Spark to stream data from a PostgreSQL database to another PostgreSQL database for analytics purposes.

## Architecture

The pipeline uses the following components:

1. **Source PostgreSQL Database**: Contains operational data with transactions
2. **Spark Streaming Application**: Reads data from the source database, transforms it, and writes to the target database
3. **Target PostgreSQL Database**: Stores historical data for analytics with time travel capabilities

## Features

- Real-time Change Data Capture from PostgreSQL
- Historical tracking of all changes (inserts, updates, deletes)
- Time-travel capabilities for analytics
- Fault-tolerant data streaming
- Support for schema evolution

## Prerequisites

- Docker and docker-compose
- PostgreSQL with logical replication enabled
- Apache Spark 3.x

## Configuration

Configuration is managed through environment variables:

- Source database connection details
- Target database connection details
- Tables to stream
- Spark configuration

## How to Run

1. Start the services:

```
docker-compose up -d
```

2. Monitor the logs:

```
docker-compose logs -f spark-app
```

## Tables

The following tables are streamed from the source database:

- `operations.customers`
- `operations.products`
- `operations.orders`
- `operations.order_items`

Target tables include:

- `analytics.customers_history`
- `analytics.products_history`
- `analytics.orders_history`
- `analytics.order_items_history`

Each target table includes additional columns for historical tracking:
- `operation_type`: The type of operation (INSERT, UPDATE, DELETE)
- `effective_from`: When this version became effective
- `effective_to`: When this version was superseded (NULL for current version)
- `is_current`: Whether this is the current version

## Development

To add new tables to the pipeline:

1. Add the table configuration to `config.py`
2. Add the table schema to `get_table_schema` in `main.py`
3. Add table-specific column mappings in `rename_columns_for_target` in `main.py`

## Troubleshooting

Common issues:

- **Connection issues**: Ensure the source and target databases are running and accessible
- **Schema issues**: Verify that the source and target schemas match the expected format
- **CDC issues**: Ensure logical replication is enabled in PostgreSQL
