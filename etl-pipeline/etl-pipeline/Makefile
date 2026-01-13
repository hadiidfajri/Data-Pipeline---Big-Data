.PHONY: help setup build up down restart logs clean init-kafka init-airflow

help:
	@echo "ETL Pipeline Management Commands:"
	@echo "  make setup        - Initial setup (create directories, install dependencies)"
	@echo "  make build        - Build all Docker containers"
	@echo "  make up           - Start all services"
	@echo "  make down         - Stop all services"
	@echo "  make restart      - Restart all services"
	@echo "  make logs         - Show logs from all services"
	@echo "  make clean        - Clean up volumes and generated files"
	@echo "  make init-kafka   - Initialize Kafka topics"
	@echo "  make init-airflow - Initialize Airflow database"

setup:
	@echo "Running initial setup..."
	bash setup.sh

build:
	@echo "Building Docker containers..."
	docker-compose build

up:
	@echo "Starting all services..."
	docker-compose up -d
	@echo "Waiting for services to be healthy..."
	sleep 30
	@echo "Services started successfully!"

down:
	@echo "Stopping all services..."
	docker-compose down

restart: down up

logs:
	docker-compose logs -f

clean:
	@echo "Cleaning up..."
	docker-compose down -v
	rm -rf postgres/data/*
	rm -rf logs/*.log
	@echo "Cleanup completed!"

init-kafka:
	@echo "Initializing Kafka topics..."
	docker exec etl-kafka bash /kafka-scripts/topics.sh

init-airflow:
	@echo "Initializing Airflow..."
	docker exec etl-airflow-webserver airflow db init
	docker exec etl-airflow-webserver airflow users create \
		--username admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@example.com \
		--password admin

run-etl:
	@echo "Running ETL pipeline..."
	docker exec etl-spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.postgresql:postgresql:42.6.0 \
		--conf spark.sql.warehouse.dir=/opt/hive-data/warehouse \
		--conf spark.sql.hive.metastore.uris=thrift://hive-metastore:9083 \
		/opt/spark-apps/etl_main.py
