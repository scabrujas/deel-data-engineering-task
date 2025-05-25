#!/bin/bash
# Script to run analytical queries on the data

echo "Starting analytical processing..."

# Set environment variables for database connections when running standalone
# These variables will override the defaults in config.py
if [ -z "$TARGET_DB_HOST" ]; then
  export TARGET_DB_HOST="target-db"
  echo "Setting TARGET_DB_HOST to target-db"
fi

if [ -z "$SOURCE_DB_HOST" ]; then
  export SOURCE_DB_HOST="source-db"
  echo "Setting SOURCE_DB_HOST to source-db"
fi

# Wait for Spark application to be ready
sleep 5

# Run analytics
cd /app
echo "Running analytics module with TARGET_DB_HOST=$TARGET_DB_HOST"
python -m src.analytics

echo "Analytical processing completed."
