#!/bin/bash
# ELT Pipeline Setup Script
# Automated setup for India Finance & UPI Transaction Pipeline

set -e

echo "========================================"
echo "ELT Pipeline Setup"
echo "========================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

# Check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker first."
        exit 1
    fi
    print_status "Docker is running"
}

# Check if docker-compose is installed
check_docker_compose() {
    if ! command -v docker-compose &> /dev/null; then
        print_error "docker-compose is not installed. Please install it first."
        exit 1
    fi
    print_status "docker-compose is installed"
}

# Create necessary directories
create_directories() {
    print_status "Creating directory structure..."
    
    mkdir -p data/raw/postgres_seed/india_personal_finance
    mkdir -p data/raw/csv/upi_transaction
    mkdir -p data/staging/hdfs/india_personal_finance
    mkdir -p data/staging/hdfs/upi_transaction
    mkdir -p data/curated/marts
    mkdir -p postgres/init
    mkdir -p postgres/data
    mkdir -p kafka/producers
    mkdir -p spark/jobs/extract
    mkdir -p spark/jobs/load
    mkdir -p spark/streaming
    mkdir -p hive/ddl
    mkdir -p hive/transformations
    mkdir -p airflow/dags
    mkdir -p config
    mkdir -p logs
    mkdir -p docker/spark
    mkdir -p docker/airflow
    mkdir -p docker/postgres
    
    print_status "Directory structure created"
}

# Generate Airflow Fernet key
generate_fernet_key() {
    if command -v python3 &> /dev/null; then
        FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
        print_status "Generated Airflow Fernet key"
    else
        print_warning "Python3 not found. Using default Fernet key."
        FERNET_KEY="default_fernet_key_replace_in_production"
    fi
}

# Check if .env file exists
check_env_file() {
    if [ ! -f .env ]; then
        print_warning ".env file not found. Creating from template..."
        # The .env content is already in the artifacts
        print_status ".env file should be created manually or copied from template"
    else
        print_status ".env file exists"
    fi
}

# Check if dataset files exist
check_datasets() {
    print_status "Checking dataset files..."
    
    if [ ! -f "data/raw/postgres_seed/india_personal_finance/India_Personal_Finance_CTGAN.csv" ]; then
        print_warning "India Personal Finance dataset not found!"
        print_warning "Please place India_Personal_Finance_CTGAN.csv in data/raw/postgres_seed/india_personal_finance/"
    else
        print_status "India Personal Finance dataset found"
    fi
    
    if [ ! -f "data/raw/csv/upi_transaction/UPI_Transaction.csv" ]; then
        print_warning "UPI Transaction dataset not found!"
        print_warning "Please place UPI_Transaction.csv in data/raw/csv/upi_transaction/"
    else
        print_status "UPI Transaction dataset found"
    fi
}

# Build Docker images
build_images() {
    print_status "Building Docker images..."
    docker-compose build
    print_status "Docker images built successfully"
}

# Start services
start_services() {
    print_status "Starting services..."
    docker-compose up -d
    print_status "Services started"
}

# Wait for services to be ready
wait_for_services() {
    print_status "Waiting for services to be ready..."
    sleep 30
    
    # Check PostgreSQL
    if docker-compose exec -T postgres pg_isready -U elt_user > /dev/null 2>&1; then
        print_status "PostgreSQL is ready"
    else
        print_warning "PostgreSQL might not be ready yet"
    fi
    
    # Check Kafka
    if docker-compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
        print_status "Kafka is ready"
    else
        print_warning "Kafka might not be ready yet"
    fi
}

# Create Kafka topics
create_kafka_topics() {
    print_status "Creating Kafka topics..."
    
    docker-compose exec -T kafka kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 3 \
        --topic india_finance_topic \
        --if-not-exists
    
    docker-compose exec -T kafka kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 3 \
        --topic upi_transaction_topic \
        --if-not-exists
    
    print_status "Kafka topics created"
}

# Display access information
display_info() {
    echo ""
    echo "========================================"
    echo "Setup Complete!"
    echo "========================================"
    echo ""
    echo "Access URLs:"
    echo "  - Airflow Web UI: http://localhost:8081"
    echo "    Username: admin"
    echo "    Password: admin"
    echo ""
    echo "  - Spark Master UI: http://localhost:8080"
    echo ""
    echo "  - PostgreSQL: localhost:5432"
    echo "    Database: elt_database"
    echo "    Username: elt_user"
    echo ""
    echo "  - Kafka: localhost:9092"
    echo ""
    echo "Next Steps:"
    echo "  1. Ensure datasets are in place"
    echo "  2. Access Airflow UI and enable the DAG"
    echo "  3. Trigger the pipeline manually or wait for scheduled run"
    echo ""
    echo "To view logs:"
    echo "  docker-compose logs -f [service_name]"
    echo ""
    echo "To stop all services:"
    echo "  docker-compose down"
    echo ""
    echo "========================================"
}

# Main execution
main() {
    echo ""
    check_docker
    check_docker_compose
    create_directories
    generate_fernet_key
    check_env_file
    check_datasets
    
    read -p "Do you want to build and start the services? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        build_images
        start_services
        wait_for_services
        create_kafka_topics
        display_info
    else
        print_status "Setup prepared. Run 'docker-compose up -d' when ready."
    fi
}

# Run main function
main