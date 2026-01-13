-- Create Data Warehouse Database
CREATE DATABASE IF NOT EXISTS india_finance_dw
COMMENT 'Data warehouse for India Personal Finance and UPI Transaction analysis'
LOCATION '/opt/hive-data/warehouse/india_finance_dw.db';

USE india_finance_dw;

-- ========================================
-- DIMENSION TABLES
-- ========================================

-- Dimension: Age Category
CREATE TABLE IF NOT EXISTS dim_age_category (
    age_category_id INT COMMENT 'Unique identifier for age category',
    age_category STRING COMMENT 'Age category name',
    age_range STRING COMMENT 'Age range (e.g., 18-25)',
    description STRING COMMENT 'Age category description'
)
COMMENT 'Dimension table for age categories'
STORED AS PARQUET;

-- Dimension: Occupation
CREATE TABLE IF NOT EXISTS dim_occupation (
    occupation_id INT COMMENT 'Unique identifier for occupation',
    occupation_name STRING COMMENT 'Occupation name',
    occupation_type STRING COMMENT 'Type of occupation'
)
COMMENT 'Dimension table for occupations'
STORED AS PARQUET;

-- Dimension: Merchant Category
CREATE TABLE IF NOT EXISTS dim_merchant_category (
    merchant_category_id INT COMMENT 'Unique identifier for merchant category',
    category_name STRING COMMENT 'Merchant category name',
    category_type STRING COMMENT 'Type of category'
)
COMMENT 'Dimension table for merchant categories'
STORED AS PARQUET;

-- Dimension: Location
CREATE TABLE IF NOT EXISTS dim_location (
    location_id INT COMMENT 'Unique identifier for location',
    state STRING COMMENT 'State name',
    city_tier STRING COMMENT 'City tier (Tier 1, 2, 3)',
    region STRING COMMENT 'Region'
)
COMMENT 'Dimension table for locations'
STORED AS PARQUET;

-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT COMMENT 'Date in YYYYMMDD format',
    full_date DATE COMMENT 'Full date',
    year INT COMMENT 'Year',
    month INT COMMENT 'Month',
    day INT COMMENT 'Day of month',
    quarter INT COMMENT 'Quarter',
    day_of_week STRING COMMENT 'Day of week name',
    is_weekend BOOLEAN COMMENT 'Weekend indicator',
    month_name STRING COMMENT 'Month name'
)
COMMENT 'Dimension table for dates'
STORED AS PARQUET;

-- ========================================
-- FACT TABLE (Star Schema)
-- ========================================

CREATE TABLE IF NOT EXISTS fact_spending_pattern (
    -- Surrogate Keys
    transaction_id STRING COMMENT 'Unique transaction identifier',
    date_id INT COMMENT 'Date dimension key',
    age_category_id INT COMMENT 'Age category dimension key',
    occupation_id INT COMMENT 'Occupation dimension key',
    merchant_category_id INT COMMENT 'Merchant category dimension key',
    location_id INT COMMENT 'Location dimension key',
    
    -- Transaction Metrics
    transaction_amount DOUBLE COMMENT 'Transaction amount in INR',
    transaction_status STRING COMMENT 'Transaction status',
    fraud_flag INT COMMENT 'Fraud indicator (0=No, 1=Yes)',
    
    -- Financial Metrics
    income DOUBLE COMMENT 'Monthly income',
    disposable_income DOUBLE COMMENT 'Disposable income',
    desired_savings DOUBLE COMMENT 'Desired savings amount',
    
    -- Spending by Category
    groceries DOUBLE COMMENT 'Groceries spending',
    transport DOUBLE COMMENT 'Transport spending',
    eating_out DOUBLE COMMENT 'Eating out spending',
    entertainment DOUBLE COMMENT 'Entertainment spending',
    utilities DOUBLE COMMENT 'Utilities spending',
    healthcare DOUBLE COMMENT 'Healthcare spending',
    education DOUBLE COMMENT 'Education spending',
    miscellaneous DOUBLE COMMENT 'Miscellaneous spending',
    
    -- Potential Savings
    potential_savings_groceries DOUBLE COMMENT 'Potential savings on groceries',
    potential_savings_transport DOUBLE COMMENT 'Potential savings on transport',
    potential_savings_eating_out DOUBLE COMMENT 'Potential savings on eating out',
    potential_savings_entertainment DOUBLE COMMENT 'Potential savings on entertainment',
    
    -- Enriched Metrics
    spending_income_ratio DOUBLE COMMENT 'Ratio of spending to income',
    financial_health_score DOUBLE COMMENT 'Financial health score',
    savings_opportunity_score DOUBLE COMMENT 'Savings opportunity score',
    is_affordable_transaction BOOLEAN COMMENT 'Transaction affordability flag',
    
    -- Metadata
    load_timestamp TIMESTAMP COMMENT 'Data load timestamp'
)
COMMENT 'Fact table for spending patterns (Star Schema)'
PARTITIONED BY (transaction_year INT, transaction_month INT)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'orc.compress'='SNAPPY'
);

-- ========================================
-- ANALYTICAL VIEWS
-- ========================================

-- View: Spending Analysis by Age Group
CREATE VIEW IF NOT EXISTS v_spending_by_age AS
SELECT 
    a.age_category,
    a.age_range,
    m.category_name as merchant_category,
    COUNT(f.transaction_id) as transaction_count,
    SUM(f.transaction_amount) as total_spending,
    AVG(f.transaction_amount) as avg_transaction_amount,
    AVG(f.financial_health_score) as avg_financial_health,
    AVG(f.savings_opportunity_score) as avg_savings_opportunity,
    SUM(CASE WHEN f.fraud_flag = 1 THEN 1 ELSE 0 END) as fraud_transactions
FROM fact_spending_pattern f
JOIN dim_age_category a ON f.age_category_id = a.age_category_id
JOIN dim_merchant_category m ON f.merchant_category_id = m.merchant_category_id
GROUP BY a.age_category, a.age_range, m.category_name;

-- View: Monthly Spending Trends
CREATE VIEW IF NOT EXISTS v_monthly_spending_trends AS
SELECT 
    d.year,
    d.month,
    d.month_name,
    COUNT(f.transaction_id) as transaction_count,
    SUM(f.transaction_amount) as total_spending,
    AVG(f.transaction_amount) as avg_transaction_amount,
    SUM(f.potential_savings_groceries + f.potential_savings_transport + 
        f.potential_savings_eating_out + f.potential_savings_entertainment) as total_potential_savings
FROM fact_spending_pattern f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- View: Financial Health by Occupation and City Tier
CREATE VIEW IF NOT EXISTS v_financial_health_analysis AS
SELECT 
    o.occupation_name,
    l.city_tier,
    COUNT(DISTINCT f.transaction_id) as transaction_count,
    AVG(f.income) as avg_income,
    AVG(f.disposable_income) as avg_disposable_income,
    AVG(f.financial_health_score) as avg_financial_health_score,
    AVG(f.spending_income_ratio) as avg_spending_income_ratio
FROM fact_spending_pattern f
JOIN dim_occupation o ON f.occupation_id = o.occupation_id
JOIN dim_location l ON f.location_id = l.location_id
GROUP BY o.occupation_name, l.city_tier;

-- View: Top Savings Opportunities
CREATE VIEW IF NOT EXISTS v_top_savings_opportunities AS
SELECT 
    a.age_category,
    m.category_name,
    COUNT(f.transaction_id) as transaction_count,
    AVG(f.savings_opportunity_score) as avg_savings_opportunity,
    SUM(f.potential_savings_groceries + f.potential_savings_transport + 
        f.potential_savings_eating_out + f.potential_savings_entertainment) as total_potential_savings
FROM fact_spending_pattern f
JOIN dim_age_category a ON f.age_category_id = a.age_category_id
JOIN dim_merchant_category m ON f.merchant_category_id = m.merchant_category_id
WHERE f.savings_opportunity_score > 0
GROUP BY a.age_category, m.category_name
HAVING total_potential_savings > 1000
ORDER BY total_potential_savings DESC;