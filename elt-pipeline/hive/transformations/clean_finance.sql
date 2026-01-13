-- Data Cleaning and Transformation: India Personal Finance
-- Storyline: Personal Spending and Savings Behavior Analysis

USE elt_curated;

-- =====================================================
-- Step 1: Clean and Standardize Finance Data
-- =====================================================
CREATE TEMPORARY TABLE tmp_clean_finance AS
SELECT
    -- ID and Keys
    id as person_id,
    
    -- Standardize and clean numeric fields (remove nulls, outliers)
    COALESCE(CAST(income AS DECIMAL(12,2)), 0) as income,
    COALESCE(age, 0) as age,
    COALESCE(dependents, 0) as dependents,
    
    -- Standardize text fields (lowercase, trim)
    LOWER(TRIM(occupation)) as occupation,
    LOWER(TRIM(city_tier)) as city_tier,
    
    -- Expense categories (handle nulls and negatives)
    CASE 
        WHEN CAST(rent AS DECIMAL(12,2)) < 0 THEN 0 
        ELSE COALESCE(CAST(rent AS DECIMAL(12,2)), 0) 
    END as rent,
    
    CASE 
        WHEN CAST(loan_repayment AS DECIMAL(12,2)) < 0 THEN 0 
        ELSE COALESCE(CAST(loan_repayment AS DECIMAL(12,2)), 0) 
    END as loan_repayment,
    
    CASE 
        WHEN CAST(insurance AS DECIMAL(12,2)) < 0 THEN 0 
        ELSE COALESCE(CAST(insurance AS DECIMAL(12,2)), 0) 
    END as insurance,
    
    CASE 
        WHEN CAST(groceries AS DECIMAL(12,2)) < 0 THEN 0 
        ELSE COALESCE(CAST(groceries AS DECIMAL(12,2)), 0) 
    END as groceries,
    
    CASE 
        WHEN CAST(transport AS DECIMAL(12,2)) < 0 THEN 0 
        ELSE COALESCE(CAST(transport AS DECIMAL(12,2)), 0) 
    END as transport,
    
    CASE 
        WHEN CAST(eating_out AS DECIMAL(12,2)) < 0 THEN 0 
        ELSE COALESCE(CAST(eating_out AS DECIMAL(12,2)), 0) 
    END as eating_out,
    
    CASE 
        WHEN CAST(entertainment AS DECIMAL(12,2)) < 0 THEN 0 
        ELSE COALESCE(CAST(entertainment AS DECIMAL(12,2)), 0) 
    END as entertainment,
    
    CASE 
        WHEN CAST(utilities AS DECIMAL(12,2)) < 0 THEN 0 
        ELSE COALESCE(CAST(utilities AS DECIMAL(12,2)), 0) 
    END as utilities,
    
    CASE 
        WHEN CAST(healthcare AS DECIMAL(12,2)) < 0 THEN 0 
        ELSE COALESCE(CAST(healthcare AS DECIMAL(12,2)), 0) 
    END as healthcare,
    
    CASE 
        WHEN CAST(education AS DECIMAL(12,2)) < 0 THEN 0 
        ELSE COALESCE(CAST(education AS DECIMAL(12,2)), 0) 
    END as education,
    
    CASE 
        WHEN CAST(miscellaneous AS DECIMAL(12,2)) < 0 THEN 0 
        ELSE COALESCE(CAST(miscellaneous AS DECIMAL(12,2)), 0) 
    END as miscellaneous,
    
    -- Savings metrics
    COALESCE(CAST(desired_savings_percentage AS DECIMAL(5,2)), 0) as desired_savings_percentage,
    COALESCE(CAST(desired_savings AS DECIMAL(12,2)), 0) as desired_savings,
    COALESCE(CAST(disposable_income AS DECIMAL(12,2)), 0) as disposable_income,
    
    -- Potential savings
    COALESCE(CAST(potential_savings_groceries AS DECIMAL(12,2)), 0) as potential_savings_groceries,
    COALESCE(CAST(potential_savings_transport AS DECIMAL(12,2)), 0) as potential_savings_transport,
    COALESCE(CAST(potential_savings_eating_out AS DECIMAL(12,2)), 0) as potential_savings_eating_out,
    COALESCE(CAST(potential_savings_entertainment AS DECIMAL(12,2)), 0) as potential_savings_entertainment,
    COALESCE(CAST(potential_savings_utilities AS DECIMAL(12,2)), 0) as potential_savings_utilities,
    COALESCE(CAST(potential_savings_healthcare AS DECIMAL(12,2)), 0) as potential_savings_healthcare,
    COALESCE(CAST(potential_savings_education AS DECIMAL(12,2)), 0) as potential_savings_education,
    COALESCE(CAST(potential_savings_miscellaneous AS DECIMAL(12,2)), 0) as potential_savings_miscellaneous,
    
    -- Timestamps
    CURRENT_TIMESTAMP() as processed_at
FROM elt_staging.stg_india_personal_finance
WHERE id IS NOT NULL;

-- =====================================================
-- Step 2: Add Age Category (Feature Engineering)
-- =====================================================
CREATE TEMPORARY TABLE tmp_finance_with_age_category AS
SELECT
    *,
    CASE
        WHEN age BETWEEN 18 AND 25 THEN 'Teenagers'
        WHEN age BETWEEN 26 AND 35 THEN 'Young Adult'
        WHEN age BETWEEN 36 AND 45 THEN 'Early Middle Age'
        WHEN age BETWEEN 46 AND 55 THEN 'Late Middle Age'
        WHEN age >= 56 THEN 'Seniors'
        ELSE 'Unknown'
    END as age_category,
    
    CASE
        WHEN age BETWEEN 18 AND 25 THEN '18-25'
        WHEN age BETWEEN 26 AND 35 THEN '26-35'
        WHEN age BETWEEN 36 AND 45 THEN '36-45'
        WHEN age BETWEEN 46 AND 55 THEN '46-55'
        WHEN age >= 56 THEN '56+'
        ELSE 'Unknown'
    END as age_range
FROM tmp_clean_finance;

-- =====================================================
-- Step 3: Normalize and Encode Categorical Variables
-- =====================================================
CREATE TEMPORARY TABLE tmp_finance_normalized AS
SELECT
    *,
    -- Encode occupation
    CASE 
        WHEN occupation = 'self_employed' THEN 1
        WHEN occupation = 'professional' THEN 2
        WHEN occupation = 'retired' THEN 3
        WHEN occupation = 'student' THEN 4
        ELSE 0
    END as occupation_code,
    
    -- Categorize occupation
    CASE 
        WHEN occupation IN ('self_employed', 'professional') THEN 'Working'
        WHEN occupation = 'retired' THEN 'Retired'
        WHEN occupation = 'student' THEN 'Student'
        ELSE 'Other'
    END as occupation_category,
    
    -- Encode city tier
    CASE 
        WHEN city_tier = 'tier_1' THEN 1
        WHEN city_tier = 'tier_2' THEN 2
        WHEN city_tier = 'tier_3' THEN 3
        ELSE 0
    END as city_tier_code,
    
    -- Normalize income (min-max normalization - example range)
    CASE 
        WHEN income > 0 THEN 
            (income - 5000) / (150000 - 5000)
        ELSE 0
    END as income_normalized,
    
    -- Normalize disposable income
    CASE 
        WHEN disposable_income > 0 THEN 
            (disposable_income - (-10000)) / (80000 - (-10000))
        ELSE 0
    END as disposable_income_normalized,
    
    -- Calculate total expenses
    (rent + loan_repayment + insurance + groceries + transport + 
     eating_out + entertainment + utilities + healthcare + 
     education + miscellaneous) as total_expenses,
    
    -- Calculate total potential savings
    (potential_savings_groceries + potential_savings_transport + 
     potential_savings_eating_out + potential_savings_entertainment + 
     potential_savings_utilities + potential_savings_healthcare + 
     potential_savings_education + potential_savings_miscellaneous) as total_potential_savings
FROM tmp_finance_with_age_category;

-- =====================================================
-- Step 4: Feature Engineering - Additional Metrics
-- =====================================================
CREATE TEMPORARY TABLE tmp_finance_enriched AS
SELECT
    *,
    -- Income bracket
    CASE
        WHEN income < 20000 THEN 'Low Income'
        WHEN income BETWEEN 20000 AND 50000 THEN 'Middle Income'
        WHEN income BETWEEN 50001 AND 100000 THEN 'Upper Middle Income'
        ELSE 'High Income'
    END as income_bracket,
    
    -- Expense ratio
    CASE 
        WHEN income > 0 THEN (total_expenses / income) * 100
        ELSE 0
    END as expense_ratio,
    
    -- Savings achievement rate
    CASE 
        WHEN desired_savings > 0 THEN 
            ((disposable_income / desired_savings) * 100)
        ELSE 0
    END as savings_achievement_rate,
    
    -- Discretionary spending (eating out + entertainment)
    (eating_out + entertainment) as discretionary_spending,
    
    -- Essential spending (rent + groceries + utilities + healthcare)
    (rent + groceries + utilities + healthcare) as essential_spending,
    
    -- Financial health score (example calculation)
    CASE
        WHEN income > 0 THEN
            ((disposable_income / income) * 0.4) +
            ((total_potential_savings / total_expenses) * 0.3) +
            (CASE WHEN dependents = 0 THEN 0.15 ELSE 0.05 END) +
            (CASE WHEN loan_repayment = 0 THEN 0.15 ELSE 0.05 END)
        ELSE 0
    END as financial_health_score
FROM tmp_finance_normalized;

-- =====================================================
-- Step 5: Remove Duplicates
-- =====================================================
CREATE TABLE IF NOT EXISTS clean_india_personal_finance
STORED AS PARQUET AS
SELECT DISTINCT
    person_id,
    income, age, dependents, occupation, city_tier,
    rent, loan_repayment, insurance, groceries, transport,
    eating_out, entertainment, utilities, healthcare,
    education, miscellaneous,
    desired_savings_percentage, desired_savings, disposable_income,
    potential_savings_groceries, potential_savings_transport,
    potential_savings_eating_out, potential_savings_entertainment,
    potential_savings_utilities, potential_savings_healthcare,
    potential_savings_education, potential_savings_miscellaneous,
    age_category, age_range,
    occupation_code, occupation_category,
    city_tier_code,
    income_normalized, disposable_income_normalized,
    total_expenses, total_potential_savings,
    income_bracket, expense_ratio, savings_achievement_rate,
    discretionary_spending, essential_spending,
    financial_health_score,
    processed_at
FROM tmp_finance_enriched
WHERE person_id IS NOT NULL
    AND age > 0
    AND income >= 0;

-- =====================================================
-- Data Quality Validation
-- =====================================================
-- Check for nulls in key columns
SELECT 
    'Null Check' as validation_type,
    COUNT(*) as total_records,
    SUM(CASE WHEN person_id IS NULL THEN 1 ELSE 0 END) as null_person_id,
    SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) as null_age,
    SUM(CASE WHEN income IS NULL THEN 1 ELSE 0 END) as null_income
FROM clean_india_personal_finance;

-- Check data ranges
SELECT 
    'Range Check' as validation_type,
    MIN(age) as min_age,
    MAX(age) as max_age,
    MIN(income) as min_income,
    MAX(income) as max_income,
    AVG(expense_ratio) as avg_expense_ratio
FROM clean_india_personal_finance;

-- Check data distribution
SELECT 
    age_category,
    COUNT(*) as record_count,
    AVG(income) as avg_income,
    AVG(total_expenses) as avg_expenses
FROM clean_india_personal_finance
GROUP BY age_category
ORDER BY age_category;