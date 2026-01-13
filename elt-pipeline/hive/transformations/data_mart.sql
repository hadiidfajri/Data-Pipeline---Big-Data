-- Data Mart Creation: Analytics-Ready Tables
-- Storyline: Impact of UPI Transaction Patterns on Personal Spending and Savings

USE elt_curated;

-- =====================================================
-- DATA MART 1: Age Group Spending Behavior
-- =====================================================
CREATE TABLE IF NOT EXISTS mart_age_spending_behavior
STORED AS PARQUET AS
SELECT
    sender_age_category as age_category,
    transaction_year as year,
    transaction_month as month,
    
    -- Demographics
    COUNT(DISTINCT person_id) as total_persons,
    ROUND(AVG(age), 1) as avg_age,
    ROUND(AVG(dependents), 1) as avg_dependents,
    
    -- Income and Expenses
    ROUND(AVG(income), 2) as avg_monthly_income,
    ROUND(AVG(total_expenses), 2) as avg_monthly_expenses,
    ROUND(AVG(expense_ratio), 2) as avg_expense_ratio_percent,
    
    -- Savings Metrics
    ROUND(AVG(disposable_income), 2) as avg_disposable_income,
    ROUND(AVG(desired_savings), 2) as avg_desired_savings,
    ROUND(AVG(savings_achievement_rate), 2) as avg_savings_achievement_percent,
    ROUND(AVG(total_potential_savings), 2) as avg_potential_savings,
    
    -- UPI Transaction Patterns
    COUNT(DISTINCT transaction_id) as total_upi_transactions,
    ROUND(SUM(transaction_amount), 2) as total_upi_spending,
    ROUND(AVG(transaction_amount), 2) as avg_transaction_amount,
    
    -- Spending by Category
    ROUND(AVG(groceries), 2) as avg_groceries,
    ROUND(AVG(eating_out), 2) as avg_eating_out,
    ROUND(AVG(entertainment), 2) as avg_entertainment,
    ROUND(AVG(transport), 2) as avg_transport,
    ROUND(AVG(utilities), 2) as avg_utilities,
    ROUND(AVG(healthcare), 2) as avg_healthcare,
    
    -- Discretionary vs Essential
    ROUND(AVG(discretionary_spending), 2) as avg_discretionary_spending,
    ROUND(AVG(essential_spending), 2) as avg_essential_spending,
    
    -- UPI Essential vs Discretionary
    ROUND(SUM(CASE WHEN spending_type = 'Essential' THEN transaction_amount ELSE 0 END), 2) as total_essential_upi,
    ROUND(SUM(CASE WHEN spending_type = 'Discretionary' THEN transaction_amount ELSE 0 END), 2) as total_discretionary_upi,
    
    -- Financial Health
    ROUND(AVG(financial_health_score), 3) as avg_financial_health_score,
    
    -- Fraud Indicators
    SUM(fraud_flag) as fraud_transaction_count,
    ROUND((SUM(fraud_flag) * 100.0 / COUNT(*)), 2) as fraud_rate_percent,
    
    -- Behavioral Patterns
    ROUND((SUM(CASE WHEN is_weekend = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as weekend_transaction_percent,
    ROUND((SUM(CASE WHEN is_high_value = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as high_value_transaction_percent,
    
    CURRENT_TIMESTAMP() as mart_created_at
FROM 
    integrated_spending_savings
GROUP BY
    sender_age_category,
    transaction_year,
    transaction_month;

-- =====================================================
-- DATA MART 2: Merchant Category Performance
-- =====================================================
CREATE TABLE IF NOT EXISTS mart_merchant_performance
STORED AS PARQUET AS
SELECT
    merchant_category,
    merchant_category_group,
    sender_age_category as age_category,
    
    -- Transaction Volume
    COUNT(DISTINCT transaction_id) as transaction_count,
    COUNT(DISTINCT person_id) as unique_customers,
    
    -- Revenue Metrics
    ROUND(SUM(transaction_amount), 2) as total_revenue,
    ROUND(AVG(transaction_amount), 2) as avg_transaction_value,
    ROUND(MIN(transaction_amount), 2) as min_transaction,
    ROUND(MAX(transaction_amount), 2) as max_transaction,
    ROUND(STDDEV(transaction_amount), 2) as stddev_transaction,
    
    -- Customer Financial Profile
    ROUND(AVG(income), 2) as avg_customer_income,
    ROUND(AVG(disposable_income), 2) as avg_customer_disposable_income,
    ROUND(AVG(financial_health_score), 3) as avg_customer_financial_health,
    
    -- Time Patterns
    ROUND(AVG(CASE WHEN is_weekend = 1 THEN transaction_amount ELSE 0 END), 2) as avg_weekend_value,
    ROUND(AVG(CASE WHEN is_weekend = 0 THEN transaction_amount ELSE 0 END), 2) as avg_weekday_value,
    ROUND((SUM(CASE WHEN time_of_day_category = 'Evening' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as evening_transaction_percent,
    
    -- Success Rate
    ROUND((SUM(CASE WHEN transaction_status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as success_rate_percent,
    
    -- Fraud Rate
    ROUND(AVG(fraud_score), 4) as avg_fraud_score,
    
    CURRENT_TIMESTAMP() as mart_created_at
FROM 
    integrated_spending_savings
GROUP BY
    merchant_category,
    merchant_category_group,
    sender_age_category;

-- =====================================================
-- DATA MART 3: Savings Opportunity Analysis
-- =====================================================
CREATE TABLE IF NOT EXISTS mart_savings_opportunity
STORED AS PARQUET AS
SELECT
    sender_age_category as age_category,
    occupation_category,
    city_tier,
    income_bracket,
    
    -- Demographics
    COUNT(DISTINCT person_id) as person_count,
    ROUND(AVG(age), 1) as avg_age,
    
    -- Current Financial Status
    ROUND(AVG(income), 2) as avg_income,
    ROUND(AVG(total_expenses), 2) as avg_expenses,
    ROUND(AVG(disposable_income), 2) as avg_disposable_income,
    ROUND(AVG(savings_achievement_rate), 2) as current_savings_achievement_percent,
    
    -- Savings Gap
    ROUND(AVG(desired_savings), 2) as avg_desired_savings,
    ROUND(AVG(disposable_income - desired_savings), 2) as avg_savings_gap,
    
    -- Potential Savings by Category
    ROUND(AVG(total_potential_savings), 2) as total_potential_savings,
    ROUND(AVG(potential_savings_groceries + potential_savings_eating_out), 2) as potential_food_savings,
    ROUND(AVG(potential_savings_transport), 2) as potential_transport_savings,
    ROUND(AVG(potential_savings_entertainment), 2) as potential_entertainment_savings,
    ROUND(AVG(potential_savings_utilities), 2) as potential_utilities_savings,
    
    -- Savings Potential Percentage
    ROUND((AVG(total_potential_savings) / AVG(total_expenses) * 100), 2) as potential_savings_percent,
    
    -- UPI Spending Analysis
    ROUND(AVG(transaction_amount), 2) as avg_upi_transaction,
    COUNT(transaction_id) as total_upi_transactions,
    ROUND(SUM(transaction_amount), 2) as total_upi_spending,
    
    -- Discretionary Spending Insights
    ROUND(AVG(discretionary_spending), 2) as avg_discretionary_spending_monthly,
    ROUND(SUM(CASE WHEN spending_type = 'Discretionary' THEN transaction_amount ELSE 0 END), 2) as total_discretionary_upi,
    
    -- Recommendations Score (higher = more potential)
    ROUND(
        (AVG(total_potential_savings) / AVG(total_expenses) * 0.4) +
        (AVG(discretionary_spending) / AVG(total_expenses) * 0.3) +
        ((100 - AVG(savings_achievement_rate)) / 100 * 0.3),
        3
    ) as savings_opportunity_score,
    
    CURRENT_TIMESTAMP() as mart_created_at
FROM 
    integrated_spending_savings
GROUP BY
    sender_age_category,
    occupation_category,
    city_tier,
    income_bracket;

-- =====================================================
-- DATA MART 4: Digital Payment Adoption & Behavior
-- =====================================================
CREATE TABLE IF NOT EXISTS mart_digital_payment_behavior
STORED AS PARQUET AS
SELECT
    sender_age_category as age_category,
    device_type,
    network_type,
    sender_state as state,
    
    -- Adoption Metrics
    COUNT(DISTINCT person_id) as unique_users,
    COUNT(DISTINCT transaction_id) as total_transactions,
    ROUND(AVG(transaction_amount), 2) as avg_transaction_amount,
    
    -- Transaction Types
    COUNT(CASE WHEN transaction_type = 'p2p' THEN 1 END) as p2p_count,
    COUNT(CASE WHEN transaction_type = 'p2m' THEN 1 END) as p2m_count,
    COUNT(CASE WHEN transaction_type = 'bill payment' THEN 1 END) as bill_payment_count,
    
    -- Time Patterns
    ROUND(AVG(hour_of_day), 1) as avg_transaction_hour,
    ROUND((SUM(CASE WHEN is_weekend = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as weekend_usage_percent,
    
    -- Success & Security
    ROUND((SUM(CASE WHEN transaction_status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as success_rate_percent,
    ROUND(AVG(fraud_score), 4) as avg_fraud_score,
    SUM(fraud_flag) as fraud_count,
    
    -- Financial Context
    ROUND(AVG(income), 2) as avg_user_income,
    ROUND(AVG(financial_health_score), 3) as avg_financial_health,
    
    CURRENT_TIMESTAMP() as mart_created_at
FROM 
    integrated_spending_savings
GROUP BY
    sender_age_category,
    device_type,
    network_type,
    sender_state;

-- =====================================================
-- DATA MART 5: Monthly Trend Analysis
-- =====================================================
CREATE TABLE IF NOT EXISTS mart_monthly_trends
STORED AS PARQUET AS
SELECT
    transaction_year as year,
    transaction_month as month,
    sender_age_category as age_category,
    
    -- Transaction Trends
    COUNT(DISTINCT transaction_id) as transactions,
    ROUND(SUM(transaction_amount), 2) as total_amount,
    ROUND(AVG(transaction_amount), 2) as avg_amount,
    
    -- Financial Trends
    ROUND(AVG(income), 2) as avg_income,
    ROUND(AVG(total_expenses), 2) as avg_expenses,
    ROUND(AVG(disposable_income), 2) as avg_disposable_income,
    ROUND(AVG(savings_achievement_rate), 2) as avg_savings_rate,
    
    -- Category Distribution
    ROUND(SUM(CASE WHEN merchant_category_group = 'Food & Groceries' THEN transaction_amount ELSE 0 END), 2) as food_spending,
    ROUND(SUM(CASE WHEN merchant_category_group = 'Leisure' THEN transaction_amount ELSE 0 END) , 2) as leisure_spending,
    ROUND(SUM(CASE WHEN merchant_category_group = 'Transportation' THEN transaction_amount ELSE 0 END), 2) as transport_spending,
    ROUND(SUM(CASE WHEN merchant_category_group = 'Utilities & Bills' THEN transaction_amount ELSE 0 END), 2) as utilities_spending,
    
    -- Behavioral Indicators
    ROUND(AVG(financial_health_score), 3) as avg_financial_health,
    ROUND((SUM(fraud_flag) * 100.0 / COUNT(*)), 2) as fraud_rate_percent,
    
    CURRENT_TIMESTAMP() as mart_created_at
FROM 
    integrated_spending_savings
GROUP BY
    transaction_year,
    transaction_month,
    sender_age_category;

-- =====================================================
-- Create Summary Statistics
-- =====================================================
SELECT 'Data Mart Statistics' as report_type;

SELECT 'Age Spending Behavior' as mart_name, COUNT(*) as record_count FROM mart_age_spending_behavior
UNION ALL
SELECT 'Merchant Performance', COUNT(*) FROM mart_merchant_performance
UNION ALL
SELECT 'Savings Opportunity', COUNT(*) FROM mart_savings_opportunity
UNION ALL
SELECT 'Digital Payment Behavior', COUNT(*) FROM mart_digital_payment_behavior
UNION ALL
SELECT 'Monthly Trends', COUNT(*) FROM mart_monthly_trends;