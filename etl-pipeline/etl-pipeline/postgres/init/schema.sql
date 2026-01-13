-- Create schema for India Personal Finance data
CREATE TABLE IF NOT EXISTS india_personal_finance (
    id SERIAL PRIMARY KEY,
    income NUMERIC(12, 2),
    age INTEGER,
    dependents INTEGER,
    occupation VARCHAR(50),
    city_tier VARCHAR(20),
    rent NUMERIC(12, 2),
    loan_repayment NUMERIC(12, 2),
    insurance NUMERIC(12, 2),
    groceries NUMERIC(12, 2),
    transport NUMERIC(12, 2),
    eating_out NUMERIC(12, 2),
    entertainment NUMERIC(12, 2),
    utilities NUMERIC(12, 2),
    healthcare NUMERIC(12, 2),
    education NUMERIC(12, 2),
    miscellaneous NUMERIC(12, 2),
    desired_savings_percentage NUMERIC(5, 2),
    desired_savings NUMERIC(12, 2),
    disposable_income NUMERIC(12, 2),
    potential_savings_groceries NUMERIC(12, 2),
    potential_savings_transport NUMERIC(12, 2),
    potential_savings_eating_out NUMERIC(12, 2),
    potential_savings_entertainment NUMERIC(12, 2),
    potential_savings_utilities NUMERIC(12, 2),
    potential_savings_healthcare NUMERIC(12, 2),
    potential_savings_education NUMERIC(12, 2),
    potential_savings_miscellaneous NUMERIC(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_age ON india_personal_finance(age);
CREATE INDEX idx_occupation ON india_personal_finance(occupation);
CREATE INDEX idx_city_tier ON india_personal_finance(city_tier);
CREATE INDEX idx_income ON india_personal_finance(income);

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_india_finance_updated_at 
    BEFORE UPDATE ON india_personal_finance 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();