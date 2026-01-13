from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HiveLoader:
    def __init__(self, spark, hive_config):
        self.spark = spark
        self.hive_config = hive_config
        self.warehouse_dir = hive_config.get('warehouse_dir', '/opt/hive-data/warehouse')
        
    def create_star_schema(self):
        """Create star schema tables in Hive"""
        logger.info("Creating star schema in Hive")
        
        # Create database
        self.spark.sql("CREATE DATABASE IF NOT EXISTS india_finance_dw")
        self.spark.sql("USE india_finance_dw")
        
        # 1. Dimension: dim_age_category
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS dim_age_category (
                age_category_id INT,
                age_category STRING,
                age_range STRING,
                description STRING
            )
            STORED AS PARQUET
        """)
        
        # 2. Dimension: dim_occupation
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS dim_occupation (
                occupation_id INT,
                occupation_name STRING,
                occupation_type STRING
            )
            STORED AS PARQUET
        """)
        
        # 3. Dimension: dim_merchant_category
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS dim_merchant_category (
                merchant_category_id INT,
                category_name STRING,
                category_type STRING
            )
            STORED AS PARQUET
        """)
        
        # 4. Dimension: dim_location
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS dim_location (
                location_id INT,
                state STRING,
                city_tier STRING,
                region STRING
            )
            STORED AS PARQUET
        """)
        
        # 5. Dimension: dim_date
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS dim_date (
                date_id INT,
                full_date DATE,
                year INT,
                month INT,
                day INT,
                quarter INT,
                day_of_week STRING,
                is_weekend BOOLEAN,
                month_name STRING
            )
            STORED AS PARQUET
        """)
        
        # 6. Fact Table: fact_spending_pattern
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS fact_spending_pattern (
                transaction_id STRING,
                date_id INT,
                age_category_id INT,
                occupation_id INT,
                merchant_category_id INT,
                location_id INT,
                
                -- Transaction Metrics
                transaction_amount DOUBLE,
                transaction_status STRING,
                fraud_flag INT,
                
                -- Financial Metrics
                income DOUBLE,
                disposable_income DOUBLE,
                desired_savings DOUBLE,
                
                -- Spending Metrics
                groceries DOUBLE,
                transport DOUBLE,
                eating_out DOUBLE,
                entertainment DOUBLE,
                utilities DOUBLE,
                healthcare DOUBLE,
                education DOUBLE,
                miscellaneous DOUBLE,
                
                -- Potential Savings
                potential_savings_groceries DOUBLE,
                potential_savings_transport DOUBLE,
                potential_savings_eating_out DOUBLE,
                potential_savings_entertainment DOUBLE,
                
                -- Enriched Features
                spending_income_ratio DOUBLE,
                financial_health_score DOUBLE,
                savings_opportunity_score DOUBLE,
                is_affordable_transaction BOOLEAN,
                
                -- Metadata
                load_timestamp TIMESTAMP
            )
            PARTITIONED BY (transaction_year INT, transaction_month INT)
            STORED AS PARQUET
        """)
        
        logger.info("Star schema created successfully")
    
    def load_dimensions(self, df_joined):
        """Load dimension tables"""
        logger.info("Loading dimension tables")
        
        # Load dim_age_category
        df_age = df_joined.select('age_category').distinct()
        df_age = df_age.withColumn('age_category_id', monotonically_increasing_id())
        df_age = df_age.withColumn('age_range',
            when(col('age_category') == 'Teenagers', '18-25')
            .when(col('age_category') == 'Young Adult', '26-35')
            .when(col('age_category') == 'Early Middle Age', '36-45')
            .when(col('age_category') == 'Late Middle Age', '46-55')
            .when(col('age_category') == 'Seniors', '56+')
            .otherwise('Unknown')
        )
        df_age = df_age.withColumn('description', concat(lit('Age group: '), col('age_range')))
        
        df_age.write.mode('overwrite').insertInto('dim_age_category')
        logger.info(f"Loaded {df_age.count()} records into dim_age_category")
        
        # Load dim_occupation
        df_occupation = df_joined.select('occupation', 'occupation_encoded').distinct()
        df_occupation = df_occupation.withColumnRenamed('occupation_encoded', 'occupation_id')
        df_occupation = df_occupation.withColumnRenamed('occupation', 'occupation_name')
        df_occupation = df_occupation.withColumn('occupation_type', lit('Primary'))
        
        df_occupation.write.mode('overwrite').insertInto('dim_occupation')
        logger.info(f"Loaded {df_occupation.count()} records into dim_occupation")
        
        # Load dim_merchant_category
        df_merchant = df_joined.select('merchant_category').distinct()
        df_merchant = df_merchant.withColumn('merchant_category_id', monotonically_increasing_id())
        df_merchant = df_merchant.withColumnRenamed('merchant_category', 'category_name')
        df_merchant = df_merchant.withColumn('category_type', lit('UPI'))
        
        df_merchant.write.mode('overwrite').insertInto('dim_merchant_category')
        logger.info(f"Loaded {df_merchant.count()} records into dim_merchant_category")
        
        # Load dim_location
        df_location = df_joined.select('sender_state', 'city_tier').distinct()
        df_location = df_location.withColumn('location_id', monotonically_increasing_id())
        df_location = df_location.withColumnRenamed('sender_state', 'state')
        df_location = df_location.withColumn('region', lit('India'))
        
        df_location.write.mode('overwrite').insertInto('dim_location')
        logger.info(f"Loaded {df_location.count()} records into dim_location")
        
        # Load dim_date (from transaction timestamps)
        df_date = df_joined.select('transaction_timestamp').distinct()
        df_date = df_date.withColumn('full_date', to_date(col('transaction_timestamp')))
        df_date = df_date.withColumn('date_id', date_format(col('full_date'), 'yyyyMMdd').cast('int'))
        df_date = df_date.withColumn('year', year(col('full_date')))
        df_date = df_date.withColumn('month', month(col('full_date')))
        df_date = df_date.withColumn('day', dayofmonth(col('full_date')))
        df_date = df_date.withColumn('quarter', quarter(col('full_date')))
        df_date = df_date.withColumn('day_of_week', date_format(col('full_date'), 'EEEE'))
        df_date = df_date.withColumn('is_weekend', 
            when(date_format(col('full_date'), 'E').isin(['Sat', 'Sun']), True).otherwise(False))
        df_date = df_date.withColumn('month_name', date_format(col('full_date'), 'MMMM'))
        
        df_date = df_date.select('date_id', 'full_date', 'year', 'month', 'day', 
                                 'quarter', 'day_of_week', 'is_weekend', 'month_name')
        
        df_date.write.mode('overwrite').insertInto('dim_date')
        logger.info(f"Loaded {df_date.count()} records into dim_date")
    
    def load_fact_table(self, df_joined):
        """Load fact table with proper joins to dimensions"""
        logger.info("Loading fact table")
        
        # Prepare fact data with dimension keys
        df_fact = df_joined.select(
            col('transaction_id'),
            date_format(to_date(col('transaction_timestamp')), 'yyyyMMdd').cast('int').alias('date_id'),
            col('age_category'),
            col('occupation_encoded').alias('occupation_id'),
            col('merchant_category'),
            col('sender_state'),
            col('city_tier'),
            col('amount').alias('transaction_amount'),
            col('transaction_status'),
            col('fraud_flag'),
            col('income'),
            col('disposable_income'),
            col('desired_savings'),
            col('groceries'),
            col('transport'),
            col('eating_out'),
            col('entertainment'),
            col('utilities'),
            col('healthcare'),
            col('education'),
            col('miscellaneous'),
            col('potential_savings_groceries'),
            col('potential_savings_transport'),
            col('potential_savings_eating_out'),
            col('potential_savings_entertainment'),
            col('spending_income_ratio'),
            col('financial_health_score'),
            col('savings_opportunity_score'),
            col('is_affordable_transaction'),
            year(col('transaction_timestamp')).alias('transaction_year'),
            month(col('transaction_timestamp')).alias('transaction_month')
        )
        
        # Add load timestamp
        df_fact = df_fact.withColumn('load_timestamp', current_timestamp())
        
        # Join with dimensions to get IDs
        dim_age = self.spark.table('dim_age_category')
        dim_merchant = self.spark.table('dim_merchant_category')
        dim_location = self.spark.table('dim_location')
        
        df_fact = df_fact.join(dim_age, 'age_category', 'left') \
                         .join(dim_merchant, df_fact.merchant_category == dim_merchant.category_name, 'left') \
                         .join(dim_location, (df_fact.sender_state == dim_location.state) & 
                               (df_fact.city_tier == dim_location.city_tier), 'left')
        
        # Select final columns for fact table
        df_fact_final = df_fact.select(
            'transaction_id', 'date_id', 'age_category_id', 'occupation_id',
            'merchant_category_id', 'location_id', 'transaction_amount',
            'transaction_status', 'fraud_flag', 'income', 'disposable_income',
            'desired_savings', 'groceries', 'transport', 'eating_out',
            'entertainment', 'utilities', 'healthcare', 'education',
            'miscellaneous', 'potential_savings_groceries',
            'potential_savings_transport', 'potential_savings_eating_out',
            'potential_savings_entertainment', 'spending_income_ratio',
            'financial_health_score', 'savings_opportunity_score',
            'is_affordable_transaction', 'load_timestamp',
            'transaction_year', 'transaction_month'
        )
        
        # Write to fact table (partitioned by year and month)
        df_fact_final.write \
            .mode('append') \
            .partitionBy('transaction_year', 'transaction_month') \
            .insertInto('fact_spending_pattern')
        
        logger.info(f"Loaded {df_fact_final.count()} records into fact_spending_pattern")

def run_load(spark, df_joined, hive_config):
    """Main load function"""
    loader = HiveLoader(spark, hive_config)
    
    # Create star schema
    loader.create_star_schema()
    
    # Load dimensions
    loader.load_dimensions(df_joined)
    
    # Load fact table
    loader.load_fact_table(df_joined)
    
    logger.info("Data load to Hive completed successfully")