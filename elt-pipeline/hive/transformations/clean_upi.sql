-- Data Cleaning and Transformation: UPI Transaction
-- Standardization, Normalization, and Feature Engineering

USE elt_curated;

-- =====================================================
-- Step 1: Clean and Standardize UPI Transaction Data
-- =====================================================
CREATE TEMPORARY TABLE tmp_clean_upi AS
SELECT
    -- Transaction ID (trim and lowercase)
    LOWER(TRIM(transaction_id)) as transaction_id,
    
    -- Parse and standardize timestamp
    CAST(timestamp AS TIMESTAMP) as transaction_timestamp,
    
    -- Standardize text fields (lowercase, trim)
    LOWER(TRIM(transaction_type)) as transaction_type,
    LOWER(TRIM(merchant_category)) as merchant_category,
    
    -- Clean amount (handle negatives and nulls)
    CASE 
        WHEN CAST(amount_inr AS DECIMAL(12,2)) < 0 THEN 0
        ELSE COALESCE(CAST(amount_inr AS DECIMAL(12,2)), 0)
    END as amount_inr,
    
    -- Standardize status
    UPPER(TRIM(transaction_status)) as transaction_status,
    
    -- Clean age groups (lowercase, trim, standardize format)
    LOWER(TRIM(REGEXP_REPLACE(sender_age_group, ' ', ''))) as sender_age_group,
    LOWER(TRIM(REGEXP_REPLACE(receiver_age_group, ' ', ''))) as receiver_age_group,
    
    -- Standardize location and bank names
    LOWER(TRIM(sender_state)) as sender_state,
    LOWER(TRIM(sender_bank)) as sender_bank,
    LOWER(TRIM(receiver_bank)) as receiver_bank,
    
    -- Device and network info
    LOWER(TRIM(device_type)) as device_type,
    LOWER(TRIM(network_type)) as network_type,
    
    -- Flags and indicators
    COALESCE(fraud_flag, 0) as fraud_flag,
    COALESCE(hour_of_day, 0) as hour_of_day,
    
    -- Day of week (standardize)
    LOWER(TRIM(day_of_week)) as day_of_week,
    COALESCE(is_weekend, 0) as is_weekend,
    
    CURRENT_TIMESTAMP() as processed_at
FROM elt_staging.stg_upi_transaction
WHERE transaction_id IS NOT NULL
    AND timestamp IS NOT NULL;

-- =====================================================
-- Step 2: Add Age Category Mapping
-- =====================================================
CREATE TEMPORARY TABLE tmp_upi_with_age_category AS
SELECT
    t.*,
    -- Map sender age group to category
    CASE
        WHEN t.sender_age_group = '18-25' THEN 'Teenagers'
        WHEN t.sender_age_group = '26-35' THEN 'Young Adult'
        WHEN t.sender_age_group = '36-45' THEN 'Early Middle Age'
        WHEN t.sender_age_group = '46-55' THEN 'Late Middle Age'
        WHEN t.sender_age_group = '56+' OR t.sender_age_group LIKE '%56%' THEN 'Seniors'
        ELSE 'Unknown'
    END as sender_age_category,
    
    -- Map receiver age group to category
    CASE
        WHEN t.receiver_age_group = '18-25' THEN 'Teenagers'
        WHEN t.receiver_age_group = '26-35' THEN 'Young Adult'
        WHEN t.receiver_age_group = '36-45' THEN 'Early Middle Age'
        WHEN t.receiver_age_group = '46-55' THEN 'Late Middle Age'
        WHEN t.receiver_age_group = '56+' OR t.receiver_age_group LIKE '%56%' THEN 'Seniors'
        ELSE 'Unknown'
    END as receiver_age_category
FROM tmp_clean_upi t;

-- =====================================================
-- Step 3: Date and Time Feature Engineering
-- =====================================================
CREATE TEMPORARY TABLE tmp_upi_time_features AS
SELECT
    *,
    -- Extract date components
    YEAR(transaction_timestamp) as transaction_year,
    MONTH(transaction_timestamp) as transaction_month,
    DAY(transaction_timestamp) as transaction_day,
    
    -- Time of day category
    CASE
        WHEN hour_of_day BETWEEN 0 AND 5 THEN 'Late Night'
        WHEN hour_of_day BETWEEN 6 AND 11 THEN 'Morning'
        WHEN hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN hour_of_day BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Night'
    END as time_of_day_category,
    
    -- Weekend indicator (boolean)
    CASE WHEN is_weekend = 1 THEN TRUE ELSE FALSE END as is_weekend_flag,
    
    -- Business hours indicator
    CASE 
        WHEN hour_of_day BETWEEN 9 AND 17 AND is_weekend = 0 THEN TRUE
        ELSE FALSE
    END as is_business_hours
FROM tmp_upi_with_age_category;

-- =====================================================
-- Step 4: Merchant and Transaction Categorization
-- =====================================================
CREATE TEMPORARY TABLE tmp_upi_categorized AS
SELECT
    *,
    -- Merchant category grouping
    CASE
        WHEN merchant_category IN ('food', 'grocery', 'eating_out') THEN 'Food & Groceries'
        WHEN merchant_category IN ('entertainment', 'shopping') THEN 'Leisure'
        WHEN merchant_category IN ('fuel', 'transport') THEN 'Transportation'
        WHEN merchant_category IN ('healthcare', 'insurance') THEN 'Health & Insurance'
        WHEN merchant_category IN ('education') THEN 'Education'
        WHEN merchant_category IN ('utilities', 'bill payment', 'recharge') THEN 'Utilities & Bills'
        ELSE 'Other'
    END as merchant_category_group,
    
    -- Essential vs discretionary
    CASE
        WHEN merchant_category IN ('food', 'grocery', 'healthcare', 'utilities', 'fuel', 'education') THEN 'Essential'
        ELSE 'Discretionary'
    END as spending_type,
    
    -- Transaction type category
    CASE
        WHEN transaction_type = 'p2p' THEN 'Person to Person'
        WHEN transaction_type = 'p2m' THEN 'Person to Merchant'
        WHEN transaction_type = 'bill payment' THEN 'Bill Payment'
        WHEN transaction_type = 'recharge' THEN 'Recharge'
        ELSE 'Other'
    END as transaction_type_category
FROM tmp_upi_time_features;

-- =====================================================
-- Step 5: Normalization and Encoding
-- =====================================================
CREATE TEMPORARY TABLE tmp_upi_normalized AS
SELECT
    *,
    -- Normalize amount (min-max scaling example)
    CASE 
        WHEN amount_inr > 0 THEN
            (amount_inr - 10) / (50000 - 10)
        ELSE 0
    END as amount_normalized,
    
    -- Encode transaction status
    CASE
        WHEN transaction_status = 'SUCCESS' THEN 1
        WHEN transaction_status = 'FAILED' THEN 0
        ELSE -1
    END as transaction_status_code,
    
    -- Encode device type
    CASE
        WHEN device_type = 'android' THEN 1
        WHEN device_type = 'ios' THEN 2
        WHEN device_type = 'web' THEN 3
        ELSE 0
    END as device_type_code,
    
    -- Encode network type
    CASE
        WHEN network_type = '5g' THEN 4
        WHEN network_type = '4g' THEN 3
        WHEN network_type = 'wifi' THEN 2
        WHEN network_type = '3g' THEN 1
        ELSE 0
    END as network_type_code,
    
    -- Transaction amount category
    CASE
        WHEN amount_inr < 100 THEN 'Micro'
        WHEN amount_inr BETWEEN 100 AND 500 THEN 'Small'
        WHEN amount_inr BETWEEN 501 AND 2000 THEN 'Medium'
        WHEN amount_inr BETWEEN 2001 AND 10000 THEN 'Large'
        ELSE 'Very Large'
    END as transaction_amount_category
FROM tmp_upi_categorized;

-- =====================================================
-- Step 6: Risk and Fraud Features
-- =====================================================
CREATE TEMPORARY TABLE tmp_upi_risk_features AS
SELECT
    *,
    -- Calculate fraud score (simple heuristic)
    (
        (fraud_flag * 0.6) +
        (CASE WHEN amount_inr > 10000 THEN 0.15 ELSE 0 END) +
        (CASE WHEN hour_of_day BETWEEN 0 AND 5 THEN 0.1 ELSE 0 END) +
        (CASE WHEN transaction_status = 'FAILED' THEN 0.15 ELSE 0 END)
    ) as fraud_score,
    
    -- High value transaction flag
    CASE WHEN amount_inr > 5000 THEN TRUE ELSE FALSE END as is_high_value,
    
    -- Unusual time flag
    CASE WHEN hour_of_day BETWEEN 0 AND 5 THEN TRUE ELSE FALSE END as is_unusual_time
FROM tmp_upi_normalized;

-- =====================================================
-- Step 7: Remove Duplicates and Outliers
-- =====================================================
CREATE TABLE IF NOT EXISTS clean_upi_transaction
STORED AS PARQUET AS
SELECT DISTINCT
    transaction_id,
    transaction_timestamp,
    transaction_type,
    transaction_type_category,
    merchant_category,
    merchant_category_group,
    spending_type,
    amount_inr,
    amount_normalized,
    transaction_amount_category,
    transaction_status,
    transaction_status_code,
    sender_age_group,
    sender_age_category,
    receiver_age_group,
    receiver_age_category,
    sender_state,
    sender_bank,
    receiver_bank,
    device_type,
    device_type_code,
    network_type,
    network_type_code,
    fraud_flag,
    fraud_score,
    is_high_value,
    hour_of_day,
    time_of_day_category,
    is_unusual_time,
    day_of_week,
    is_weekend,
    is_weekend_flag,
    is_business_hours,
    transaction_year,
    transaction_month,
    transaction_day,
    processed_at
FROM tmp_upi_risk_features
WHERE transaction_id IS NOT NULL
    AND transaction_timestamp IS NOT NULL
    AND amount_inr >= 0
    AND amount_inr <= 100000  -- Remove extreme outliers
    AND transaction_status IN ('SUCCESS', 'FAILED');

-- =====================================================
-- Data Quality Validation
-- =====================================================
-- Uniqueness check
SELECT 
    'Uniqueness Check' as validation_type,
    COUNT(*) as total_records,
    COUNT(DISTINCT transaction_id) as unique_transactions,
    COUNT(*) - COUNT(DISTINCT transaction_id) as duplicate_count
FROM clean_upi_transaction;

-- Null check
SELECT 
    'Null Check' as validation_type,
    SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) as null_transaction_id,
    SUM(CASE WHEN amount_inr IS NULL THEN 1 ELSE 0 END) as null_amount,
    SUM(CASE WHEN transaction_status IS NULL THEN 1 ELSE 0 END) as null_status
FROM clean_upi_transaction;

-- Range check
SELECT 
    'Range Check' as validation_type,
    MIN(amount_inr) as min_amount,
    MAX(amount_inr) as max_amount,
    AVG(amount_inr) as avg_amount,
    MIN(hour_of_day) as min_hour,
    MAX(hour_of_day) as max_hour
FROM clean_upi_transaction;

-- Distribution by age category
SELECT 
    sender_age_category,
    COUNT(*) as transaction_count,
    AVG(amount_inr) as avg_transaction_amount,
    SUM(CASE WHEN fraud_flag = 1 THEN 1 ELSE 0 END) as fraud_count
FROM clean_upi_transaction
GROUP BY sender_age_category
ORDER BY sender_age_category;