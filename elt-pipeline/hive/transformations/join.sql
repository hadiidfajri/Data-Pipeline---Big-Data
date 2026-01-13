-- Join Transformation: Combine Finance and UPI Data
-- Storyline: Impact of UPI Transaction Patterns on Personal Spending and Savings

USE elt_curated;

-- =====================================================
-- Create Integrated Dataset
-- Join UPI transactions with Personal Finance based on Age Category
-- =====================================================

-- This is a map-side join optimization hint for Hive
SET hive.auto.convert.join=true;
SET hive.mapjoin.smalltable.filesize=25000000;
SET hive.optimize.bucketmapjoin=true;

-- =====================================================
-- Step 1: Create Age-based Join View
-- =====================================================
CREATE TABLE IF NOT EXISTS integrated_spending_savings AS
SELECT
    -- UPI Transaction Info
    u.transaction_id,
    u.transaction_timestamp,
    u.transaction_year,
    u.transaction_month,
    u.transaction_type,
    u.merchant_category,
    u.merchant_category_group,
    u.spending_type,
    u.amount_inr as transaction_amount,
    u.transaction_status,
    u.fraud_flag,
    u.fraud_score,
    u.is_high_value,
    u.hour_of_day,
    u.time_of_day_category,
    u.is_weekend,
    
    -- Age Group Info
    u.sender_age_group,
    u.sender_age_category,
    
    -- Personal Finance Info (aggregated by age category)
    f.person_id,
    f.age,
    f.dependents,
    f.occupation,
    f.occupation_category,
    f.city_tier,
    f.income,
    f.income_bracket,
    f.total_expenses,
    f.expense_ratio,
    f.rent,
    f.groceries,
    f.transport,
    f.eating_out,
    f.entertainment,
    f.utilities,
    f.healthcare,
    f.education,
    f.miscellaneous,
    f.desired_savings,
    f.disposable_income,
    f.savings_achievement_rate,
    f.total_potential_savings,
    f.financial_health_score,
    f.discretionary_spending,
    f.essential_spending,
    
    -- Device and Network Info
    u.sender_state,
    u.device_type,
    u.network_type,
    u.sender_bank,
    u.receiver_bank,
    
    CURRENT_TIMESTAMP() as integration_timestamp
FROM 
    clean_upi_transaction u
LEFT JOIN 
    clean_india_personal_finance f
ON 
    u.sender_age_category = f.age_category
WHERE
    u.transaction_status = 'SUCCESS'
    AND f.person_id IS NOT NULL;

-- =====================================================
-- Step 2: Create Summary Statistics by Age Category
-- =====================================================
CREATE TABLE IF NOT EXISTS age_category_spending_summary AS
SELECT
    sender_age_category,
    transaction_year,
    transaction_month,
    
    -- Transaction metrics
    COUNT(DISTINCT transaction_id) as total_transactions,
    SUM(transaction_amount) as total_transaction_value,
    AVG(transaction_amount) as avg_transaction_amount,
    MIN(transaction_amount) as min_transaction_amount,
    MAX(transaction_amount) as max_transaction_amount,
    STDDEV(transaction_amount) as stddev_transaction_amount,
    
    -- By merchant category
    SUM(CASE WHEN merchant_category_group = 'Food & Groceries' THEN transaction_amount ELSE 0 END) as food_groceries_spending,
    SUM(CASE WHEN merchant_category_group = 'Leisure' THEN transaction_amount ELSE 0 END) as leisure_spending,
    SUM(CASE WHEN merchant_category_group = 'Transportation' THEN transaction_amount ELSE 0 END) as transport_spending,
    SUM(CASE WHEN merchant_category_group = 'Health & Insurance' THEN transaction_amount ELSE 0 END) as health_spending,
    SUM(CASE WHEN merchant_category_group = 'Utilities & Bills' THEN transaction_amount ELSE 0 END) as utilities_spending,
    
    -- By spending type
    SUM(CASE WHEN spending_type = 'Essential' THEN transaction_amount ELSE 0 END) as essential_upi_spending,
    SUM(CASE WHEN spending_type = 'Discretionary' THEN transaction_amount ELSE 0 END) as discretionary_upi_spending,
    
    -- Fraud metrics
    SUM(CASE WHEN fraud_flag = 1 THEN 1 ELSE 0 END) as fraud_transaction_count,
    (SUM(CASE WHEN fraud_flag = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as fraud_percentage,
    
    -- Time-based metrics
    SUM(CASE WHEN is_weekend = 1 THEN 1 ELSE 0 END) as weekend_transactions,
    (SUM(CASE WHEN is_weekend = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as weekend_transaction_percentage,
    
    -- Financial metrics (from finance data)
    AVG(income) as avg_income,
    AVG(total_expenses) as avg_total_expenses,
    AVG(disposable_income) as avg_disposable_income,
    AVG(desired_savings) as avg_desired_savings,
    AVG(savings_achievement_rate) as avg_savings_achievement_rate,
    AVG(financial_health_score) as avg_financial_health_score,
    AVG(expense_ratio) as avg_expense_ratio,
    
    CURRENT_TIMESTAMP() as created_at
FROM 
    integrated_spending_savings
GROUP BY
    sender_age_category,
    transaction_year,
    transaction_month;

-- =====================================================
-- Step 3: Create Merchant Category Analysis
-- =====================================================
CREATE TABLE IF NOT EXISTS merchant_category_analysis AS
SELECT
    sender_age_category,
    merchant_category,
    merchant_category_group,
    
    -- Transaction counts
    COUNT(DISTINCT transaction_id) as transaction_count,
    COUNT(DISTINCT person_id) as unique_persons,
    
    -- Amount metrics
    SUM(transaction_amount) as total_amount,
    AVG(transaction_amount) as avg_amount,
    PERCENTILE(transaction_amount, 0.5) as median_amount,
    
    -- Time patterns
    AVG(CASE WHEN is_weekend = 1 THEN transaction_amount ELSE NULL END) as avg_weekend_amount,
    AVG(CASE WHEN is_weekend = 0 THEN transaction_amount ELSE NULL END) as avg_weekday_amount,
    
    -- Financial health correlation
    AVG(financial_health_score) as avg_financial_health,
    AVG(savings_achievement_rate) as avg_savings_rate,
    AVG(disposable_income) as avg_disposable_income,
    
    -- Behavior indicators
    AVG(CASE WHEN time_of_day_category = 'Evening' THEN 1 ELSE 0 END) as evening_transaction_ratio,
    AVG(fraud_flag) as fraud_rate,
    
    CURRENT_TIMESTAMP() as created_at
FROM 
    integrated_spending_savings
GROUP BY
    sender_age_category,
    merchant_category,
    merchant_category_group;

-- =====================================================
-- Step 4: Create Occupation-based Spending Analysis
-- =====================================================
CREATE TABLE IF NOT EXISTS occupation_spending_analysis AS
SELECT
    occupation_category,
    sender_age_category,
    city_tier,
    
    -- Demographics
    COUNT(DISTINCT person_id) as person_count,
    AVG(age) as avg_age,
    AVG(dependents) as avg_dependents,
    
    -- Financial metrics
    AVG(income) as avg_income,
    AVG(total_expenses) as avg_expenses,
    AVG(disposable_income) as avg_disposable_income,
    AVG(savings_achievement_rate) as avg_savings_rate,
    
    -- UPI transaction metrics
    COUNT(transaction_id) as total_upi_transactions,
    SUM(transaction_amount) as total_upi_spending,
    AVG(transaction_amount) as avg_upi_transaction,
    
    -- Category spending from finance data
    AVG(groceries) as avg_groceries_expense,
    AVG(eating_out) as avg_eating_out_expense,
    AVG(entertainment) as avg_entertainment_expense,
    AVG(transport) as avg_transport_expense,
    AVG(utilities) as avg_utilities_expense,
    
    -- Savings potential
    AVG(total_potential_savings) as avg_potential_savings,
    (AVG(total_potential_savings) / AVG(total_expenses) * 100) as potential_savings_percentage,
    
    CURRENT_TIMESTAMP() as created_at
FROM 
    integrated_spending_savings
GROUP BY
    occupation_category,
    sender_age_category,
    city_tier;

-- =====================================================
-- Step 5: Create Time-based Spending Patterns
-- =====================================================
CREATE TABLE IF NOT EXISTS time_based_spending_patterns AS
SELECT
    sender_age_category,
    time_of_day_category,
    is_weekend,
    day_of_week,
    
    -- Transaction metrics
    COUNT(*) as transaction_count,
    SUM(transaction_amount) as total_amount,
    AVG(transaction_amount) as avg_amount,
    
    -- Merchant preferences
    COUNT(DISTINCT merchant_category) as distinct_categories,
    MODE(merchant_category) as most_common_category,
    
    -- Financial context
    AVG(income) as avg_income,
    AVG(disposable_income) as avg_disposable_income,
    AVG(financial_health_score) as avg_financial_health,
    
    -- Risk indicators
    SUM(fraud_flag) as fraud_count,
    AVG(fraud_score) as avg_fraud_score,
    
    CURRENT_TIMESTAMP() as created_at
FROM 
    integrated_spending_savings
GROUP BY
    sender_age_category,
    time_of_day_category,
    is_weekend,
    day_of_week;

-- =====================================================
-- Validate Join Results
-- =====================================================
SELECT 
    'Join Validation' as check_type,
    COUNT(*) as total_integrated_records,
    COUNT(DISTINCT transaction_id) as unique_transactions,
    COUNT(DISTINCT person_id) as unique_persons,
    COUNT(DISTINCT sender_age_category) as age_categories
FROM integrated_spending_savings;

-- Check coverage by age category
SELECT 
    sender_age_category,
    COUNT(*) as record_count,
    COUNT(DISTINCT transaction_id) as transaction_count,
    COUNT(DISTINCT person_id) as person_count
FROM integrated_spending_savings
GROUP BY sender_age_category
ORDER BY sender_age_category;