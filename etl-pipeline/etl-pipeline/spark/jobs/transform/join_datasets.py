from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatasetJoiner:
    def __init__(self, spark):
        self.spark = spark
        
    def join_datasets(self, df_finance, df_upi):
        """
        Join India Personal Finance and UPI Transaction datasets
        based on age_category
        """
        logger.info("Starting dataset join operation")
        
        # Prepare finance dataset for join
        df_finance_prep = df_finance.select(
            col('id').alias('finance_id'),
            col('age'),
            col('age_category'),
            col('income'),
            col('occupation'),
            col('city_tier'),
            col('rent'),
            col('loan_repayment'),
            col('groceries'),
            col('transport'),
            col('eating_out'),
            col('entertainment'),
            col('utilities'),
            col('healthcare'),
            col('education'),
            col('miscellaneous'),
            col('desired_savings'),
            col('disposable_income'),
            col('potential_savings_groceries'),
            col('potential_savings_transport'),
            col('potential_savings_eating_out'),
            col('potential_savings_entertainment'),
            col('income_normalized'),
            col('disposable_income_normalized'),
            col('occupation_encoded'),
            col('city_tier_encoded')
        )
        
        # Prepare UPI dataset for join
        df_upi_prep = df_upi.select(
            col('transaction_id'),
            col('timestamp').alias('transaction_timestamp'),
            col('transaction_type'),
            col('merchant_category'),
            col('amount'),
            col('transaction_status'),
            col('sender_age_category').alias('age_category'),  # Join key
            col('sender_state'),
            col('device_type'),
            col('network_type'),
            col('fraud_flag'),
            col('is_weekend'),
            col('amount_category'),
            col('time_of_day'),
            col('transaction_month'),
            col('amount_normalized')
        )
        
        # Perform inner join on age_category
        # This creates a relationship between personal finance and spending patterns
        logger.info("Performing join on age_category")
        
        df_joined = df_upi_prep.join(
            df_finance_prep,
            on='age_category',
            how='inner'
        )
        
        initial_joined_count = df_joined.count()
        logger.info(f"Initial joined records: {initial_joined_count}")
        
        # Feature Engineering on joined dataset
        df_joined = self.create_enriched_features(df_joined)
        
        logger.info(f"Dataset join completed. Total records: {df_joined.count()}")
        
        return df_joined
    
    def create_enriched_features(self, df):
        """Create enriched features from joined data"""
        logger.info("Creating enriched features")
        
        # 1. Spending vs Income Ratio
        df = df.withColumn('spending_income_ratio',
            when(col('income') > 0, col('amount') / col('income'))
            .otherwise(0.0)
        )
        
        # 2. Transaction Alignment with Category
        # Check if merchant category aligns with high potential savings
        df = df.withColumn('high_savings_category',
            when(col('merchant_category') == 'groceries', col('potential_savings_groceries'))
            .when(col('merchant_category') == 'transport', col('potential_savings_transport'))
            .when(col('merchant_category') == 'food', col('potential_savings_eating_out'))
            .when(col('merchant_category') == 'entertainment', col('potential_savings_entertainment'))
            .otherwise(0.0)
        )
        
        # 3. Financial Health Score
        df = df.withColumn('financial_health_score',
            (col('disposable_income_normalized') * 40 +
             (1 - col('spending_income_ratio')) * 30 +
             col('income_normalized') * 30)
        )
        
        # 4. Transaction Affordability Flag
        df = df.withColumn('is_affordable_transaction',
            when(col('amount') <= (col('disposable_income') * 0.1), True)
            .otherwise(False)
        )
        
        # 5. Savings Opportunity Score
        df = df.withColumn('savings_opportunity_score',
            when(col('high_savings_category') > 0,
                 (col('high_savings_category') / col('amount')) * 100)
            .otherwise(0.0)
        )
        
        # 6. Weekend Spending Impact
        df = df.withColumn('weekend_spending_impact',
            when((col('is_weekend') == 1) & (col('merchant_category').isin(['food', 'entertainment', 'shopping'])),
                 col('amount') * 1.2)
            .otherwise(col('amount'))
        )
        
        # 7. Age-Based Transaction Pattern
        df = df.withColumn('age_transaction_pattern',
            concat(col('age_category'), lit('_'), col('merchant_category'))
        )
        
        # 8. Digital Adoption Score
        df = df.withColumn('digital_adoption_score',
            when((col('device_type') == 'android') | (col('device_type') == 'ios'), 100.0)
            .when(col('device_type') == 'web', 75.0)
            .otherwise(50.0)
        )
        
        return df
    
    def create_aggregated_views(self, df):
        """Create aggregated views for analysis"""
        logger.info("Creating aggregated views")
        
        # Aggregate by age category and merchant category
        df_agg = df.groupBy('age_category', 'merchant_category').agg(
            count('transaction_id').alias('transaction_count'),
            sum('amount').alias('total_amount'),
            avg('amount').alias('avg_amount'),
            sum(when(col('fraud_flag') == 1, 1).otherwise(0)).alias('fraud_count'),
            avg('financial_health_score').alias('avg_financial_health'),
            avg('savings_opportunity_score').alias('avg_savings_opportunity'),
            avg('income').alias('avg_income'),
            avg('disposable_income').alias('avg_disposable_income')
        )
        
        return df_agg

def run_join(spark, finance_path, upi_path):
    """Main join function"""
    joiner = DatasetJoiner(spark)
    
    # Read cleaned datasets
    df_finance = spark.read.parquet(finance_path)
    df_upi = spark.read.parquet(upi_path)
    
    # Join datasets
    df_joined = joiner.join_datasets(df_finance, df_upi)
    
    # Create aggregated views
    df_aggregated = joiner.create_aggregated_views(df_joined)
    
    return df_joined, df_aggregated