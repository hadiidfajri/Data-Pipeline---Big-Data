-- Create Schema for ELT Pipeline
CREATE SCHEMA IF NOT EXISTS source_data;

-- Create table for India Personal Finance data
CREATE TABLE IF NOT EXISTS source_data.india_personal_finance (
    id SERIAL PRIMARY KEY,
    income DECIMAL(12, 2),
    age INTEGER,
    dependents INTEGER,
    occupation VARCHAR(50),
    city_tier VARCHAR(20),
    rent DECIMAL(12, 2),
    loan_repayment DECIMAL(12, 2),
    insurance DECIMAL(12, 2),
    groceries DECIMAL(12, 2),
    transport DECIMAL(12, 2),
    eating_out DECIMAL(12, 2),
    entertainment DECIMAL(12, 2),
    utilities DECIMAL(12, 2),
    healthcare DECIMAL(12, 2),
    education DECIMAL(12, 2),
    miscellaneous DECIMAL(12, 2),
    desired_savings_percentage DECIMAL(5, 2),
    desired_savings DECIMAL(12, 2),
    disposable_income DECIMAL(12, 2),
    potential_savings_groceries DECIMAL(12, 2),
    potential_savings_transport DECIMAL(12, 2),
    potential_savings_eating_out DECIMAL(12, 2),
    potential_savings_entertainment DECIMAL(12, 2),
    potential_savings_utilities DECIMAL(12, 2),
    potential_savings_healthcare DECIMAL(12, 2),
    potential_savings_education DECIMAL(12, 2),
    potential_savings_miscellaneous DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for better query performance
CREATE INDEX idx_finance_age ON source_data.india_personal_finance(age);
CREATE INDEX idx_finance_occupation ON source_data.india_personal_finance(occupation);
CREATE INDEX idx_finance_city_tier ON source_data.india_personal_finance(city_tier);

-- Create audit log table
CREATE TABLE IF NOT EXISTS source_data.audit_log (
    log_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    operation VARCHAR(20),
    record_count INTEGER,
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20),
    error_message TEXT
);

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA source_data TO elt_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA source_data TO elt_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA source_data TO elt_user;