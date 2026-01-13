# ETL Pipeline: India Personal Finance & UPI Transaction Analysis

## 📋 Project Overview

This ETL pipeline analyzes **The Impact of UPI Transaction Patterns on Personal Spending and Savings Behavior Across Age Groups in India** by integrating two key datasets:

1. **India Personal Finance Dataset** (20,000 records) - Demographics, income, expenses, and savings patterns
2. **UPI Transaction Dataset** (2024 data) - Digital payment transactions across merchant categories

### Key Features

- **Real-time data extraction** using Apache Kafka
- **Distributed processing** with Apache Spark
- **Data quality validation** with comprehensive checks
- **Star schema design** for analytical queries
- **Automated orchestration** with Apache Airflow
- **Scalable architecture** using Docker containers

## 🏗️ Architecture

```
PostgreSQL → Kafka → Spark (Extract, Transform, Load) → Hive Data Warehouse
                ↓
            CSV Files
                ↓
           Airflow Orchestration
```

### Technology Stack

- **Data Extraction**: Apache Kafka
- **Data Processing**: Apache Spark 3.4.0
- **Data Storage**: PostgreSQL, Apache Hive
- **Orchestration**: Apache Airflow 2.7.3
- **Containerization**: Docker & Docker Compose
- **Programming**: Python 3.10, PySpark, SQL

## 📊 Data Transformation Details

### India Personal Finance Dataset

**Transformations:**
1. Remove duplicates
2. Handle missing values (median/mode imputation)
3. Standardize column names (lowercase, snake_case)
4. Create `age_category` feature
5. Encode categorical variables (occupation, city_tier)
6. Normalize numerical columns (income, disposable_income)
7. Detect outliers using IQR method

**Age Categories:**
- Teenagers: 18-25
- Young Adult: 26-35
- Early Middle Age: 36-45
- Late Middle Age: 46-55
- Seniors: 56+

### UPI Transaction Dataset

**Transformations:**
1. Remove duplicates
2. Convert timestamp to proper datetime format
3. Standardize categorical data
4. Create `sender_age_category` feature
5. Extract date components (year, month, day)
6. Create `amount_category` bins
7. Add `time_of_day` feature
8. Normalize transaction amounts
9. Filter invalid records

### Joined Dataset Features

**Enriched Features:**
1. `spending_income_ratio` - Transaction amount vs income
2. `financial_health_score` - Composite score (0-100)
3. `savings_opportunity_score` - Potential savings percentage
4. `is_affordable_transaction` - Affordability flag
5. `weekend_spending_impact` - Weekend spending multiplier
6. `age_transaction_pattern` - Combined age and category
7. `digital_adoption_score` - Device usage score

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.10+
- 8GB RAM minimum
- 20GB disk space

### Setup

1. **Clone repository and navigate to project directory**

```bash
cd etl-pipeline
```

2. **Place your datasets**
```bash
# India Personal Finance CSV
cp India_Personal_Finance_CTGAN.csv data/raw/postgres_seed/india_personal_finance/

# UPI Transaction CSV
cp UPI_Transaction.csv data/raw/csv/upi_transaction/
```

3. **Run setup script**
```bash
chmod +x setup.sh
./setup.sh
```

4. **Build and start services**
```bash
make build
make up
```

5. **Initialize Kafka topics**
```bash
make init-kafka
```

6. **Initialize Airflow**
```bash
make init-airflow
```

7. **Run ETL pipeline**
```bash
make run-etl
```

## 📦 Project Structure

```
etl-pipeline/
├── docker-compose.yml          # Docker services configuration
├── .env                        # Environment variables
├── Makefile                    # Automation commands
├── setup.sh                    # Initial setup script
│
├── docker/                     # Docker configurations
│   ├── spark/
│   ├── airflow/
│   └── postgres/
│
├── data/                       # Data directories
│   ├── raw/                    # Source data
│   ├── processed/              # Processed data
│   └── metadata/               # Data documentation
│
├── postgres/                   # PostgreSQL scripts
│   └── init/
│       ├── schema.sql          # Table schemas
│       └── load_india_finance.sql
│
├── kafka/                      # Kafka producers
│   ├── producers/
│   │   ├── postgres_producer.py
│   │   └── upi_csv_producer.py
│   └── topics.sh
│
├── spark/                      # Spark jobs
│   ├── jobs/
│   │   ├── extract/           # Extraction jobs
│   │   ├── transform/         # Transformation jobs
│   │   └── load/              # Load jobs
│   └── etl_main.py            # Main ETL script
│
├── hive/                       # Hive DDL scripts
│   └── ddl/
│       └── warehouse_tables.hql
│
├── airflow/                    # Airflow DAGs
│   └── dags/
│       └── etl_pipeline_dag.py
│
├── config/                     # Configuration files
│   ├── postgres.yaml
│   ├── kafka.yaml
│   └── spark.yaml
│
└── logs/                       # Application logs
```

## 🔍 Data Quality Checks

The pipeline implements comprehensive quality checks:

1. **Uniqueness Check** - Verify primary key uniqueness
2. **Null Check** - Identify missing values
3. **Range Check** - Validate value ranges
4. **Datatype Consistency** - Ensure correct data types
5. **Referential Integrity** - Check foreign key relationships
6. **Distribution Analysis** - Statistical distribution checks

Quality reports are generated in `/logs/quality_report.json`

## 📈 Star Schema Design

### Dimension Tables
- `dim_age_category` - Age group dimensions
- `dim_occupation` - Occupation types
- `dim_merchant_category` - Merchant categories
- `dim_location` - Geographic locations
- `dim_date` - Date dimension

### Fact Table
- `fact_spending_pattern` - Central fact table with:
  - Transaction metrics
  - Financial metrics
  - Spending by category
  - Potential savings
  - Enriched analytical features

**Partitioning:** By `transaction_year` and `transaction_month` for query optimization

## 📊 Analytical Views

Pre-built views for common analyses:
- `v_spending_by_age` - Spending patterns by age group
- `v_monthly_spending_trends` - Time-series spending analysis
- `v_financial_health_analysis` - Financial health by occupation and city
- `v_top_savings_opportunities` - High-impact savings opportunities

## 🔧 Makefile Commands

```bash
make setup          # Initial setup
make build          # Build Docker containers
make up             # Start all services
make down           # Stop all services
make restart        # Restart services
make logs           # View logs
make clean          # Clean up volumes
make init-kafka     # Initialize Kafka topics
make init-airflow   # Initialize Airflow
make run-etl        # Run ETL pipeline
```

## 🌐 Access Points

- **Spark Master UI**: http://localhost:8080
- **Airflow UI**: http://localhost:8081 (admin/admin)
- **Kafka**: localhost:29092
- **PostgreSQL**: localhost:5432
- **Hive Metastore**: localhost:9083

## 📝 Airflow DAG

The pipeline is orchestrated through Airflow with the following tasks:

1. Initialize Kafka topics
2. Produce India Finance data to Kafka
3. Produce UPI data to Kafka
4. Extract data from Kafka
5. Transform Finance data
6. Transform UPI data
7. Join datasets
8. Run quality checks
9. Load to Hive
10. Generate summary report

**Schedule**: Daily (@daily)

## 🐛 Troubleshooting

### Common Issues

**1. Services not starting**
```bash
docker-compose down -v
make clean
make build
make up
```

**2. Kafka connection issues**
```bash
docker exec etl-kafka kafka-topics --list --bootstrap-server localhost:9092
```

**3. Spark job failures**
- Check logs: `docker logs etl-spark-master`
- Verify data paths in config files

**4. Airflow DAG not appearing**
- Check DAG file for syntax errors
- Restart Airflow scheduler: `docker restart etl-airflow-scheduler`

## 📚 Documentation

- [Data Dictionary](data/metadata/data_dictionary.md)
- [API Documentation](docs/api.md)
- [Configuration Guide](docs/configuration.md)

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Open Pull Request

## 📄 License

This project is licensed under the MIT License.

## 👥 Team

Data Engineering Team
- ETL Development
- Data Quality
- Infrastructure

## 📞 Support

For issues and questions:
- Create an issue in the repository
- Contact: data-engineering@example.com

---

**Note**: This is a sample ETL pipeline for educational purposes. Ensure proper security measures for production deployments.