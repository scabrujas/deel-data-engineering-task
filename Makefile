# Data Engineering Pipeline Makefile
# Simple commands to manage the data pipeline

# Colors for better readability
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

.PHONY: help start stop restart clean logs logs-spark logs-debezium logs-api configure status analytics query-views clean-target api-docs

# Show help information
help:
	@echo "${YELLOW}Available commands:${NC}"
	@echo "  ${GREEN}make start${NC}         - Start all services"
	@echo "  ${GREEN}make stop${NC}          - Stop all services"
	@echo "  ${GREEN}make restart${NC}       - Restart all services with fresh target database"
	@echo "  ${GREEN}make clean${NC}         - Remove all containers, networks, and volumes"
	@echo "  ${GREEN}make configure${NC}     - Configure Debezium connector"
	@echo "  ${GREEN}make status${NC}        - Check status of all services"
	@echo "  ${GREEN}make logs${NC}          - Show logs of all services"
	@echo "  ${GREEN}make logs-spark${NC}    - Show logs of Spark application"
	@echo "  ${GREEN}make logs-debezium${NC} - Show logs of Debezium connector"
	@echo "  ${GREEN}make logs-api${NC}      - Show logs of the Analytics API"
	@echo "  ${GREEN}make analytics${NC}     - Run analytics queries"
	@echo "  ${GREEN}make query-views${NC}   - Query the analytical views"
	@echo "  ${GREEN}make api-docs${NC}      - Open the API documentation"
	@echo "  ${GREEN}make clean-target${NC}  - Clean target database data (ensures fresh start)"

# Start all services
start:
	@echo "${GREEN}Starting all services...${NC}"
	@docker-compose up -d
	@echo "${GREEN}Services started. Run 'make configure' to set up Debezium connector.${NC}"

# Stop all services
stop:
	@echo "${YELLOW}Stopping all services...${NC}"
	@docker-compose stop
	@echo "${GREEN}Services stopped.${NC}"

# Restart all services
restart: stop start
	@echo "${GREEN}All services restarted.${NC}"

# Remove all containers, networks, and volumes
clean:
	@echo "${RED}Removing all containers, networks, and volumes...${NC}"
	@docker-compose down
	@echo "${GREEN}Clean completed.${NC}"

# Configure Debezium connector
configure:
	@echo "${GREEN}Configuring Debezium connector...${NC}"
	@chmod +x ./configure-debezium.sh
	@./configure-debezium.sh
	@echo "${GREEN}Debezium connector configured.${NC}"

# Check status of all services
status:
	@echo "${YELLOW}Checking status of all services...${NC}"
	@docker-compose ps
	@echo ""
	@echo "${YELLOW}Checking Debezium connector status...${NC}"
	@curl -s http://localhost:8083/connectors/postgres-connector/status | jq || echo "Connector not available or 'jq' command not installed."

# Show logs of all services
logs:
	@docker-compose logs --tail=100

# Show logs of Spark application
logs-spark:
	@docker-compose logs --tail=100 spark-app

# Show logs of Debezium connector
logs-debezium:
	@docker-compose logs --tail=100 debezium-connect

# Show logs of Analytics API
logs-api:
	@docker-compose logs --tail=100 analytics-api

# Run analytics queries
analytics:
	@echo "${GREEN}Running analytics queries...${NC}"
	@docker run -it --rm --network=deel-data-engineering-task_default \
	  -e TARGET_DB_HOST=target-db \
	  -e SOURCE_DB_HOST=source-db \
	  deel-data-engineering-task-spark-app bash -c "/app/run_analytics.sh"

# Query the analytical views
query-views:
	@echo "${GREEN}Querying analytical views...${NC}"
	@docker run -it --rm --network=deel-data-engineering-task_default \
	  -e TARGET_DB_HOST=target-db \
	  -e SOURCE_DB_HOST=source-db \
	  deel-data-engineering-task-spark-app bash -c "/app/query_views.sh"

# Clean target database data
clean-target:
	@echo "${YELLOW}Cleaning target database data...${NC}"
	@rm -rf ${PWD}/target-db-data/* 2>/dev/null || true
	@echo "${GREEN}Target database data cleaned.${NC}"

# Open API documentation
api-docs:
	@echo "${GREEN}Opening API documentation...${NC}"
	@open http://localhost:8000/docs
