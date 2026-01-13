-- Hive DDL for Staging Tables
-- Star Schema Design for ELT Pipeline

-- Create database
CREATE DATABASE IF NOT EXISTS elt_staging
COMMENT 'Staging database for ELT pipeline'
LOCATION '/data/staging';

USE elt_staging;

-- =====================================================
-- STAGING TABLE: India Personal Finance (Raw)
-- =====================================================
CREATE EXTERNAL TABLE IF NOT EXISTS stg_india_personal_finance (
    id INT,
    income DECIMAL(12,2),
    age INT,
    dependents INT,
    occupation STRING,
    city_tier STRING,
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
    desired_savings_percentage DECIMAL(5,2),
    desired_savings DECIMAL(12,2),
    disposable_income DECIMAL(12,2),
    potential_savings_groceries DECIMAL(12,2),
    potential_savings_transport DECIMAL(12,2),
    potential_savings_eating_out DECIMAL(12,2),
    potential_savings_entertainment DECIMAL(12,2),
    potential_savings_utilities DECIMAL(12,2),
    potential_savings_healthcare DECIMAL(12,2),
    potential_savings_education DECIMAL(12,2),
    potential_savings_miscellaneous DECIMAL(12,2),
    extracted_at STRING,
    source_system STRING,
    processed_at TIMESTAMP
)
COMMENT 'Staging table for India Personal Finance data'
STORED AS PARQUET
LOCATION '/data/raw/india_personal_finance';

-- =====================================================
-- STAGING TABLE: UPI Transaction (Raw)
-- =====================================================
CREATE EXTERNAL TABLE IF NOT EXISTS stg_upi_transaction (
    transaction_id STRING,
    timestamp STRING,
    transaction_type STRING,
    merchant_category STRING,
    amount_inr DECIMAL(12,2),
    transaction_status STRING,
    sender_age_group STRING,
    receiver_age_group STRING,
    sender_state STRING,
    sender_bank STRING,
    receiver_bank STRING,
    device_type STRING,
    network_type STRING,
    fraud_flag INT,
    hour_of_day INT,
    day_of_week STRING,
    is_weekend INT,
    extracted_at STRING,
    source_system STRING,
    processed_at TIMESTAMP
)
COMMENT 'Staging table for UPI Transaction data'
PARTITIONED BY (year STRING, month STRING, status STRING)
STORED AS PARQUET
LOCATION '/data/raw/upi_transaction';

-- =====================================================
-- STAGING VIEW: Age Group Mapping
-- =====================================================
CREATE VIEW IF NOT EXISTS vw_age_group_mapping AS
SELECT
    '18-25' as age_range,
    'Teenagers' as age_category
UNION ALL
SELECT '26-35', 'Young Adult'
UNION ALL
SELECT '36-45', 'Early Middle Age'
UNION ALL
SELECT '46-55', 'Late Middle Age'
UNION ALL
SELECT '56+', 'Seniors';

-- =====================================================
-- Add partitions for UPI transaction table
-- =====================================================
-- This will be done dynamically during data load
-- MSCK REPAIR TABLE stg_upi_transaction;

-- =====================================================
-- Table Statistics
-- =====================================================
ANALYZE TABLE stg_india_personal_finance COMPUTE STATISTICS;
ANALYZE TABLE stg_upi_transaction COMPUTE STATISTICS;