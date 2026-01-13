# Data Dictionary

## Dataset 1: India Personal Finance

### Source Information
- **Source**: PostgreSQL Database
- **Format**: CSV → PostgreSQL → Kafka
- **Records**: 20,000
- **Extraction**: Kafka Topic `india_finance_topic`

### Field Descriptions

| Field Name | Data Type | Description | Sample Values | Constraints |
|------------|-----------|-------------|---------------|-------------|
| id | INTEGER | Unique record identifier | 1, 2, 3... | PRIMARY KEY |
| income | DOUBLE | Monthly income in INR | 44637.25, 26858.60 | > 0 |
| age | INTEGER | Age of individual | 18-64 | 18-100 |
| dependents | INTEGER | Number of dependents | 0, 1, 2, 3, 4 | >= 0 |
| occupation | STRING | Employment type | self_employed, retired, student, professional | NOT NULL |
| city_tier | STRING | Living area tier | tier_1, tier_2, tier_3 | NOT NULL |
| rent | DOUBLE | Monthly rent in INR | 13391.17, 5371.72 | >= 0 |
| loan_repayment | DOUBLE | Monthly loan payment | 0, 4612.10 | >= 0 |
| insurance | DOUBLE | Monthly insurance premium | 2206.49, 869.52 | >= 0 |
| groceries | DOUBLE | Monthly grocery expenses | 6658.77, 2818.44 | >= 0 |
| transport | DOUBLE | Monthly transport expenses | 2636.97, 1543.02 | >= 0 |
| eating_out | DOUBLE | Monthly dining out expenses | 1651.80, 649.38 | >= 0 |
| entertainment | DOUBLE | Monthly entertainment expenses | 1536.18, 1050.24 | >= 0 |
| utilities | DOUBLE | Monthly utility bills | 2911.79, 1626.14 | >= 0 |
| healthcare | DOUBLE | Monthly healthcare expenses | 1546.91, 1137.35 | >= 0 |
| education | DOUBLE | Monthly education expenses | 0, 1551.72 | >= 0 |
| miscellaneous | DOUBLE | Other monthly expenses | 831.53, 564.24 | >= 0 |
| desired_savings_percentage | DOUBLE | Target savings percentage | 13.89, 7.16 | 0-100 |
| desired_savings | DOUBLE | Target savings amount | 6200.54, 1923.18 | >= 0 |
| disposable_income | DOUBLE | Income after expenses | 11265.63, 9676.82 | Can be negative |
| potential_savings_groceries | DOUBLE | Potential savings on groceries | 1685.70, 540.31 | >= 0 |
| potential_savings_transport | DOUBLE | Potential savings on transport | 328.90, 119.35 | >= 0 |
| potential_savings_eating_out | DOUBLE | Potential savings on dining | 465.77, 141.87 | >= 0 |
| potential_savings_entertainment | DOUBLE | Potential savings on entertainment | 195.15, 234.13 | >= 0 |
| potential_savings_utilities | DOUBLE | Potential savings on utilities | 678.29, 286.67 | >= 0 |
| potential_savings_healthcare | DOUBLE | Potential savings on healthcare | 67.68, 6.60 | >= 0 |
| potential_savings_education | DOUBLE | Potential savings on education | 0, 56.31 | >= 0 |
| potential_savings_miscellaneous | DOUBLE | Potential savings on miscellaneous | 85.74, 97.39 | >= 0 |

### Engineered Features

| Field Name | Data Type | Description | Transformation Logic |
|------------|-----------|-------------|---------------------|
| age_category | STRING | Age group classification | 18-25: Teenagers, 26-35: Young Adult, 36-45: Early Middle Age, 46-55: Late Middle Age, 56+: Seniors |
| occupation_encoded | INTEGER | Encoded occupation | self_employed=1, retired=2, student=3, professional=4, unknown=0 |
| city_tier_encoded | INTEGER | Encoded city tier | tier_1=1, tier_2=2, tier_3=3, unknown=0 |
| income_normalized | DOUBLE | Min-max normalized income | (income - min) / (max - min) |
| disposable_income_normalized | DOUBLE | Normalized disposable income | (disposable_income - min) / (max - min) |

---

## Dataset 2: UPI Transaction

### Source Information
- **Source**: CSV File
- **Format**: CSV → Kafka
- **Records**: Variable (2024 transactions)
- **Extraction**: Kafka Topic `upi_transaction_topic`

### Field Descriptions

| Field Name | Data Type | Description | Sample Values | Constraints |
|------------|-----------|-------------|---------------|-------------|
| transaction_id | STRING | Unique transaction ID | TXN000000001, TXN000000002 | PRIMARY KEY |
| timestamp | TIMESTAMP | Transaction date & time | 2024-10-08 15:17:28 | NOT NULL |
| transaction_type | STRING | Type of transaction | p2p, p2m, bill payment, recharge | NOT NULL |
| merchant_category | STRING | Merchant category | food, grocery, fuel, entertainment, etc. | NOT NULL |
| amount | DOUBLE | Transaction amount in INR | 868, 1011, 477 | > 0 |
| transaction_status | STRING | Transaction result | success, failed | NOT NULL |
| sender_age_group | STRING | Sender age range | 18-25, 26-35, 36-45, 46-55, 56+ | NOT NULL |
| receiver_age_group | STRING | Receiver age range | 18-25, 26-35, 36-45, 46-55, 56+ | NOT NULL |
| sender_state | STRING | Sender's state | Delhi, Maharashtra, Karnataka, etc. | NOT NULL |
| sender_bank | STRING | Sender's bank | ICICI, SBI, Axis, PNB, Yes Bank | NOT NULL |
| receiver_bank | STRING | Receiver's bank | ICICI, SBI, Axis, PNB, Yes Bank | NOT NULL |
| device_type | STRING | Device used | android, ios, web | NOT NULL |
| network_type | STRING | Network connection | 4g, 5g, wifi, 3g | NOT NULL |
| fraud_flag | INTEGER | Fraud indicator | 0, 1 | 0 or 1 |
| hour_of_day | INTEGER | Transaction hour | 0-23 | 0-23 |
| day_of_week | STRING | Day name | Monday, Tuesday, etc. | NOT NULL |
| is_weekend | INTEGER | Weekend indicator | 0, 1 | 0 or 1 |

### Engineered Features

| Field Name | Data Type | Description | Transformation Logic |
|------------|-----------|-------------|---------------------|
| sender_age_category | STRING | Mapped age category | Maps sender_age_group to standard categories |
| transaction_date | DATE | Transaction date only | Extracted from timestamp |
| transaction_year | INTEGER | Transaction year | Year from timestamp |
| transaction_month | INTEGER | Transaction month | Month from timestamp |
| transaction_day | INTEGER | Transaction day | Day from timestamp |
| amount_category | STRING | Amount classification | <500: Very Low, 500-1000: Low, 1000-2500: Medium, 2500-5000: High, 5000+: Very High |
| is_weekend_bool | BOOLEAN | Weekend flag (boolean) | Converted from is_weekend integer |
| time_of_day | STRING | Time period | 0-6: Night, 6-12: Morning, 12-18: Afternoon, 18-24: Evening |
| amount_normalized | DOUBLE | Normalized amount | Min-max normalization |

---

## Joined Dataset Features

### Join Logic
- **Join Key**: `age_category` (from both datasets)
- **Join Type**: INNER JOIN
- **Purpose**: Relate personal finance profiles with transaction patterns by age group

### Enriched Features

| Field Name | Data Type | Description | Calculation Logic |
|------------|-----------|-------------|-------------------|
| spending_income_ratio | DOUBLE | Transaction vs income | amount / income (when income > 0) |
| financial_health_score | DOUBLE | Composite health score | (disposable_income_normalized × 40) + ((1 - spending_income_ratio) × 30) + (income_normalized × 30) |
| high_savings_category | DOUBLE | Savings in transaction category | Maps merchant_category to corresponding potential_savings field |
| savings_opportunity_score | DOUBLE | Savings potential % | (high_savings_category / amount) × 100 when applicable |
| is_affordable_transaction | BOOLEAN | Affordability flag | TRUE if amount <= (disposable_income × 0.1) |
| weekend_spending_impact | DOUBLE | Weekend spending multiplier | amount × 1.2 for food/entertainment/shopping on weekends |
| age_transaction_pattern | STRING | Combined pattern | Concatenation of age_category and merchant_category |
| digital_adoption_score | DOUBLE | Digital usage score | android/ios: 100, web: 75, other: 50 |

---

## Star Schema: Data Warehouse

### Dimension Tables

#### dim_age_category
| Column | Type | Description |
|--------|------|-------------|
| age_category_id | INT | Unique ID |
| age_category | STRING | Category name |
| age_range | STRING | Age range |
| description | STRING | Category description |

#### dim_occupation
| Column | Type | Description |
|--------|------|-------------|
| occupation_id | INT | Unique ID (from encoding) |
| occupation_name | STRING | Occupation name |
| occupation_type | STRING | Type classification |

#### dim_merchant_category
| Column | Type | Description |
|--------|------|-------------|
| merchant_category_id | INT | Unique ID |
| category_name | STRING | Category name |
| category_type | STRING | Category type |

#### dim_location
| Column | Type | Description |
|--------|------|-------------|
| location_id | INT | Unique ID |
| state | STRING | State name |
| city_tier | STRING | City tier |
| region | STRING | Region |

#### dim_date
| Column | Type | Description |
|--------|------|-------------|
| date_id | INT | Date in YYYYMMDD format |
| full_date | DATE | Complete date |
| year | INT | Year |
| month | INT | Month |
| day | INT | Day |
| quarter | INT | Quarter |
| day_of_week | STRING | Day name |
| is_weekend | BOOLEAN | Weekend flag |
| month_name | STRING | Month name |

### Fact Table

#### fact_spending_pattern
**Partitioned by**: `transaction_year`, `transaction_month`

Contains all metrics from joined dataset plus dimension foreign keys:
- Surrogate keys to all dimensions
- Transaction metrics
- Financial metrics
- Spending by category
- Potential savings
- Enriched analytical features

---

## Data Quality Rules

### Validation Rules

1. **Uniqueness**
   - transaction_id must be unique
   - id (finance) must be unique

2. **Range Checks**
   - age: 18-100
   - amount: > 0
   - income: > 0
   - fraud_flag: 0 or 1

3. **Null Checks**
   - No nulls in primary keys
   - No nulls in required dimensions
   - Nulls allowed in optional metrics

4. **Referential Integrity**
   - All dimension keys in fact table exist in dimension tables

5. **Data Types**
   - Numerical fields: DOUBLE/INTEGER as specified
   - Date fields: TIMESTAMP/DATE format
   - Categorical fields: STRING

---

## Usage Examples

### Query 1: Average Spending by Age Group
```sql
SELECT 
    age_category,
    AVG(transaction_amount) as avg_spending,
    COUNT(*) as transaction_count
FROM fact_spending_pattern f
JOIN dim_age_category a ON f.age_category_id = a.age_category_id
GROUP BY age_category;
```

### Query 2: Top Savings Opportunities
```sql
SELECT * FROM v_top_savings_opportunities
WHERE avg_savings_opportunity > 10
ORDER BY total_potential_savings DESC
LIMIT 10;
```

### Query 3: Monthly Trends
```sql
SELECT * FROM v_monthly_spending_trends
WHERE year = 2024
ORDER BY month;
```