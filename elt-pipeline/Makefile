
.PHONY: help setup build up down restart logs clean test

# Default target
.DEFAULT_GOAL := help

# Colors
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[1;33m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)ELT Pipeline - India Finance & UPI Transaction Analysis$(NC)"
	@echo ""
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-15s$(NC) %s\n", $$1, $$2}'

setup: ## Run initial setup script
	@echo "$(YELLOW)Running setup script...$(NC)"
	@chmod +x setup.sh
	@./setup.sh

build: ## Build all Docker images
	@echo "$(YELLOW)Building Docker images...$(NC)"
	docker-compose build

up: ## Start all services
	@echo "$(YELLOW)Starting all services...$(NC)"
	docker-compose up -d
	@echo "$(GREEN)Services started successfully!$(NC)"
	@echo "Airflow UI: http://localhost:8081"
	@echo "Spark UI: http://localhost:8080"

down: ## Stop all services
	@echo "$(YELLOW)Stopping all services...$(NC)"
	docker-compose down
	@echo "$(GREEN)Services stopped!$(NC)"

restart: down up ## Restart all services

logs: ## Show logs from all services
	docker-compose logs -f

logs-airflow: ## Show Airflow logs
	docker-compose logs -f airflow-webserver airflow-scheduler

logs-spark: ## Show Spark logs
	docker-compose logs -f spark-master spark-worker

logs-kafka: ## Show Kafka logs
	docker-compose logs -f kafka

logs-postgres: ## Show PostgreSQL logs
	docker-compose logs -f postgres

status: ## Show status of all services
	@echo "$(YELLOW)Service Status:$(NC)"
	@docker-compose ps

exec-postgres: ## Connect to PostgreSQL
	docker-compose exec postgres psql -U elt_user -d elt_database

exec-spark: ## Enter Spark container
	docker-compose exec spark-master bash

exec-airflow: ## Enter Airflow container
	docker-compose exec airflow-webserver bash

kafka-topics: ## List Kafka topics
	docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

kafka-consumer-finance: ## Start Kafka consumer for finance topic
	docker-compose exec kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic india_finance_topic \
		--from-beginning

kafka-consumer-upi: ## Start Kafka consumer for UPI topic
	docker-compose exec kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic upi_transaction_topic \
		--from-beginning

run-postgres-producer: ## Run PostgreSQL to Kafka producer
	docker-compose exec spark-master python /opt/kafka/producers/postgres_producer.py

run-upi-producer: ## Run UPI CSV to Kafka producer
	docker-compose exec spark-master python /opt/kafka/producers/upi_csv_producer.py

trigger-dag: ## Trigger Airflow DAG manually
	docker-compose exec airflow-webserver airflow dags trigger elt_pipeline_india_finance_upi

list-dags: ## List all Airflow DAGs
	docker-compose exec airflow-webserver airflow dags list

check-data: ## Check data in HDFS directories
	@echo "$(YELLOW)Checking HDFS data...$(NC)"
	@docker-compose exec spark-master ls -lR /opt/data/

check-hive-tables: ## List Hive tables
	@echo "$(YELLOW)Hive Tables:$(NC)"
	@docker-compose exec hive-metastore hive -e "SHOW DATABASES; USE elt_staging; SHOW TABLES; USE elt_curated; SHOW TABLES;"

clean-data: ## Clean all data directories (WARNING: This will delete all data!)
	@echo "$(YELLOW)WARNING: This will delete all data!$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		rm -rf data/raw/hdfs/* data/staging/* data/curated/* logs/*; \
		echo "$(GREEN)Data cleaned!$(NC)"; \
	fi

clean: down ## Stop services and clean up containers
	@echo "$(YELLOW)Cleaning up...$(NC)"
	docker-compose down -v
	@echo "$(GREEN)Cleanup complete!$(NC)"

reset: clean ## Full reset - clean everything and rebuild
	@echo "$(YELLOW)Performing full reset...$(NC)"
	docker-compose down -v --remove-orphans
	docker system prune -f
	@echo "$(GREEN)Reset complete! Run 'make setup' to start fresh.$(NC)"

test: ## Run tests (if available)
	@echo "$(YELLOW)Running tests...$(NC)"
	pytest tests/ -v

backup-data: ## Backup data directories
	@echo "$(YELLOW)Creating backup...$(NC)"
	@mkdir -p backups
	@tar -czf backups/data-backup-$$(date +%Y%m%d-%H%M%S).tar.gz data/
	@echo "$(GREEN)Backup created in backups/$(NC)"

install-deps: ## Install Python dependencies locally
	pip install -r requirements.txt

validate: ## Validate configuration files
	@echo "$(YELLOW)Validating configurations...$(NC)"
	@docker-compose config > /dev/null && echo "$(GREEN)docker-compose.yml is valid$(NC)"
	@test -f .env && echo "$(GREEN).env file exists$(NC)" || echo "$(YELLOW)Warning: .env file missing$(NC)"

health-check: ## Check health of all services
	@echo "$(YELLOW)Checking service health...$(NC)"
	@docker-compose ps
	@echo ""
	@echo "$(YELLOW)Checking PostgreSQL...$(NC)"
	@docker-compose exec -T postgres pg_isready -U elt_user && echo "$(GREEN)PostgreSQL: OK$(NC)" || echo "$(YELLOW)PostgreSQL: Not Ready$(NC)"
	@echo ""
	@echo "$(YELLOW)Checking Kafka...$(NC)"
	@docker-compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1 && echo "$(GREEN)Kafka: OK$(NC)" || echo "$(YELLOW)Kafka: Not Ready$(NC)"

monitor: ## Monitor resource usage
	@docker stats --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}"

docs: ## Generate documentation
	@echo "$(YELLOW)Documentation available in README.md$(NC)"
	@cat README.md