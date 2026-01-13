-- Hive DDL for Curated Tables (Star Schema)
-- Fact and Dimension Tables

-- Create curated database
CREATE DATABASE IF NOT EXISTS elt_curated
COMMENT 'Curated database with star schema for analytics'
LOCATION '/data/curated';

USE elt_curated;

-- =====================================================
-- DIMENSION: Age Group Dimension
-- =====================================================
CREATE TABLE IF NOT EXISTS dim_age_group (
    age_group_sk INT,
    age_range STRING,
    age_category STRING,
    min_age INT,
    max_age INT,
    generation STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
COMMENT 'Age group dimension for demographic analysis'
STORED AS PARQUET
LOCATION '/data/curated/dim_age_group';

-- =====================================================
-- DIMENSION: Person Dimension (SCD Type 1)
-- =====================================================
CREATE TABLE IF NOT EXISTS dim_person (
    person_sk BIGINT,
    person_id INT,
    age INT,
    age_category STRING,
    dependents INT,
    occupation STRING,
    occupation_category STRING,
    city_tier STRING,
    income_bracket STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
COMMENT 'Person dimension from finance dataset'
PARTITIONED BY (occupation_type STRING)
STORED AS PARQUET
LOCATION '/data/curated/dim_person';

-- =====================================================
-- DIMENSION: Transaction Type Dimension
-- =====================================================
CREATE TABLE IF NOT EXISTS dim_transaction_type (
    transaction_type_sk INT,
    transaction_type STRING,
    transaction_category STRING,
    description STRING,
    created_at TIMESTAMP
)
COMMENT 'Transaction type dimension'
STORED AS PARQUET
LOCATION '/data/curated/dim_transaction_type';

-- =====================================================
-- DIMENSION: Merchant Category Dimension
-- =====================================================
CREATE TABLE IF NOT EXISTS dim_merchant_category (
    merchant_category_sk INT,
    merchant_category STRING,
    category_group STRING,
    is_essential BOOLEAN,
    avg_transaction_amount DECIMAL(12,2),
    created_at TIMESTAMP
)
COMMENT 'Merchant category dimension'
STORED AS PARQUET
LOCATION '/data/curated/dim_merchant_category';

-- =====================================================
-- DIMENSION: Date Dimension
-- =====================================================
CREATE TABLE IF NOT EXISTS dim_date (
    date_sk INT,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    month_name STRING,
    week_of_year INT,
    day_of_month INT,
    day_of_week INT,
    day_name STRING,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    fiscal_year INT,
    fiscal_quarter INT
)
COMMENT 'Date dimension for time-based analysis'
STORED AS PARQUET
LOCATION '/data/curated/dim_date';

-- =====================================================
-- DIMENSION: Location Dimension
-- =====================================================
CREATE TABLE IF NOT EXISTS dim_location (
    location_sk INT,
    state_name STRING,
    region STRING,
    tier STRING,
    population_category STRING,
    created_at TIMESTAMP
)
COMMENT 'Location dimension for geographic analysis'
STORED AS PARQUET
LOCATION '/data/curated/dim_location';

-- =====================================================
-- FACT TABLE: Personal Finance Fact
-- =====================================================
CREATE TABLE IF NOT EXISTS fact_personal_finance (
    finance_fact_sk BIGINT,
    person_sk BIGINT,
    age_group_sk INT,
    location_sk INT,
    date_sk INT,
    
    -- Financial metrics
    income DECIMAL(12,2),
    total_expenses DECIMAL(12,2),
    
    -- Expense categories
    rent DECIMAL(12,2),
    loan_repayment DECIMAL(12,2),
    insurance DECIMAL(12,2),
    groceries DECIMAL(12,2),
    transport DECIMAL(12,2),
    eating_out DECIMAL(12,2),
    entertainment DECIMAL(12,2),
    utilities DECIMAL(12,2),
    healthcare DECIMAL(12,2),
    education DECIMAL(12,2),
    miscellaneous DECIMAL(12,2),
    
    -- Savings metrics
    desired_savings_percentage DECIMAL(5,2),
    desired_savings DECIMAL(12,2),
    disposable_income DECIMAL(12,2),
    actual_savings DECIMAL(12,2),
    savings_rate DECIMAL(5,2),
    
    -- Potential savings
    potential_savings_groceries DECIMAL(12,2),
    potential_savings_transport DECIMAL(12,2),
    potential_savings_eating_out DECIMAL(12,2),
    potential_savings_entertainment DECIMAL(12,2),
    potential_savings_utilities DECIMAL(12,2),
    potential_savings_healthcare DECIMAL(12,2),
    potential_savings_education DECIMAL(12,2),
    potential_savings_miscellaneous DECIMAL(12,2),
    total_potential_savings DECIMAL(12,2),
    
    -- Metadata
    record_created_at TIMESTAMP,
    record_updated_at TIMESTAMP
)
COMMENT 'Fact table for personal finance analysis'
PARTITIONED BY (year INT, month INT)
CLUSTERED BY (person_sk) INTO 8 BUCKETS
STORED AS PARQUET
LOCATION '/data/curated/fact_personal_finance';

-- =====================================================
-- FACT TABLE: UPI Transaction Fact
-- =====================================================
CREATE TABLE IF NOT EXISTS fact_upi_transaction (
    transaction_fact_sk BIGINT,
    transaction_id STRING,
    transaction_type_sk INT,
    merchant_category_sk INT,
    sender_age_group_sk INT,
    receiver_age_group_sk INT,
    sender_location_sk INT,
    date_sk INT,
    
    -- Transaction details
    transaction_timestamp TIMESTAMP,
    amount_inr DECIMAL(12,2),
    transaction_status STRING,
    
    -- Banking details
    sender_bank STRING,
    receiver_bank STRING,
    
    -- Technical details
    device_type STRING,
    network_type STRING,
    
    -- Risk indicators
    fraud_flag INT,
    fraud_score DECIMAL(5,4),
    
    -- Time dimensions
    hour_of_day INT,
    day_of_week STRING,
    is_weekend INT,
    time_of_day_category STRING,
    
    -- Metadata
    record_created_at TIMESTAMP,
    record_updated_at TIMESTAMP
)
COMMENT 'Fact table for UPI transaction analysis'
PARTITIONED BY (year INT, month INT, status STRING)
CLUSTERED BY (transaction_id) INTO 16 BUCKETS
STORED AS PARQUET
LOCATION '/data/curated/fact_upi_transaction';

-- =====================================================
-- AGGREGATED FACT: Spending Behavior by Age Group
-- =====================================================
CREATE TABLE IF NOT EXISTS fact_spending_behavior_agg (
    age_category STRING,
    merchant_category STRING,
    year INT,
    month INT,
    
    -- Aggregated metrics
    total_transactions BIGINT,
    total_amount DECIMAL(15,2),
    avg_transaction_amount DECIMAL(12,2),
    min_transaction_amount DECIMAL(12,2),
    max_transaction_amount DECIMAL(12,2),
    
    -- Finance metrics
    avg_income DECIMAL(12,2),
    avg_savings DECIMAL(12,2),
    avg_disposable_income DECIMAL(12,2),
    
    -- Behavioral indicators
    fraud_transaction_count BIGINT,
    fraud_percentage DECIMAL(5,2),
    weekend_transaction_percentage DECIMAL(5,2),
    
    created_at TIMESTAMP
)
COMMENT 'Aggregated fact for spending behavior analysis'
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION '/data/curated/fact_spending_behavior_agg';

-- =====================================================
-- Enable statistics
-- =====================================================
SET hive.stats.autogather=true;
SET hive.compute.query.using.stats=true;