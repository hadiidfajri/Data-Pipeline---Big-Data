-- Load India Personal Finance data from CSV
\COPY source_data.india_personal_finance(income, age, dependents, occupation, city_tier, rent, loan_repayment, insurance, groceries, transport, eating_out, entertainment, utilities, healthcare, education, miscellaneous, desired_savings_percentage, desired_savings, disposable_income, potential_savings_groceries, potential_savings_transport, potential_savings_eating_out, potential_savings_entertainment, potential_savings_utilities, potential_savings_healthcare, potential_savings_education, potential_savings_miscellaneous) FROM '/seed_data/india_personal_finance/India_Personal_Finance_CTGAN.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

-- Log the load operation
INSERT INTO source_data.audit_log (table_name, operation, record_count, status)
SELECT 
    'india_personal_finance',
    'INITIAL_LOAD',
    COUNT(*),
    'SUCCESS'
FROM source_data.india_personal_finance;

-- Verify data load
SELECT 
    'india_personal_finance' as table_name,
    COUNT(*) as total_records,
    MIN(age) as min_age,
    MAX(age) as max_age,
    AVG(income) as avg_income
FROM source_data.india_personal_finance;