#!/bin/bash

# Wait for Debezium Connect to be available
echo "Waiting for Debezium Connect to be available..."
until curl -s -f http://localhost:8083/connectors > /dev/null; do
  echo "Waiting for Debezium Connect..."
  sleep 5
done

echo "Debezium Connect is available. Configuring connectors..."

# Create Debezium PostgreSQL connector
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/ -d '{
  "name": "postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "source-db",
    "database.port": "5432",
    "database.user": "cdc_user",
    "database.password": "cdc_1234",
    "database.dbname": "finance_db",
    "database.server.name": "finance-db",
    "topic.prefix": "finance-db",
    "table.include.list": "operations.customers,operations.products,operations.orders,operations.order_items",
    "plugin.name": "pgoutput",
    "publication.name": "cdc_publication",
    "slot.name": "cdc_pgoutput",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,ts_ms"
  }
}'

echo "Connector configuration sent. Check connector status:"
# Check if jq is installed, if not just display the raw JSON
if command -v jq &> /dev/null; then
  curl -s http://localhost:8083/connectors/postgres-connector/status | jq
else
  echo "Note: 'jq' command not found, displaying raw JSON:"
  curl -s http://localhost:8083/connectors/postgres-connector/status
fi

echo "Configuration complete!"
