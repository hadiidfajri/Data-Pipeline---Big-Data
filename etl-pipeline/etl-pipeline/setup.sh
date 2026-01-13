#!/bin/bash

echo "===== ETL Pipeline Setup Script ====="

# Create necessary directories
echo "Creating directory structure..."
mkdir -p data/raw/postgres_seed/india_personal_finance
mkdir -p data/raw/csv/upi_transaction
mkdir -p data/processed/spark/clean
mkdir -p data/processed/spark/rejected
mkdir -p data/metadata
mkdir -p postgres/data
mkdir -p postgres/init
mkdir -p logs

# Set permissions
echo "Setting permissions..."
chmod -R 755 data
chmod -R 755 postgres
chmod -R 755 logs

# Generate Airflow Fernet Key
echo "Generating Airflow Fernet Key..."
FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
echo "AIRFLOW_FERNET_KEY=$FERNET_KEY" >> .env

# Generate Airflow Secret Key
SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_hex(32))")
echo "AIRFLOW_SECRET_KEY=$SECRET_KEY" >> .env

echo "Setup completed successfully!"
echo "Please ensure your dataset files are placed in:"
echo "  - data/raw/postgres_seed/india_personal_finance/India_Personal_Finance_CTGAN.csv"
echo "  - data/raw/csv/upi_transaction/UPI_Transaction.csv"
echo ""
echo "Next steps:"
echo "  1. Run 'make build' to build containers"
echo "  2. Run 'make up' to start services"
echo "  3. Run 'make init-kafka' to initialize Kafka topics"
echo "  4. Run 'make init-airflow' to initialize Airflow"