-- Load India Personal Finance data from CSV
COPY india_personal_finance(
    income, age, dependents, occupation, city_tier,
    rent, loan_repayment, insurance, groceries, transport,
    eating_out, entertainment, utilities, healthcare, education,
    miscellaneous, desired_savings_percentage, desired_savings,
    disposable_income, potential_savings_groceries,
    potential_savings_transport, potential_savings_eating_out,
    potential_savings_entertainment, potential_savings_utilities,
    potential_savings_healthcare, potential_savings_education,
    potential_savings_miscellaneous
)
FROM '/seed_data/india_personal_finance/India_Personal_Finance_CTGAN.csv'
DELIMITER ','
CSV HEADER;

-- Verify data load
SELECT COUNT(*) as total_records FROM india_personal_finance;

-- Show sample data
SELECT * FROM india_personal_finance LIMIT 5;