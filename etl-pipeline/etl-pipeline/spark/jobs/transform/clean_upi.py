from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UPIDataCleaner:
    def __init__(self, spark):
        self.spark = spark
        
    def clean_data(self, df):
        """Clean UPI Transaction data"""
        logger.info("Starting data cleaning for UPI transactions")
        
        # 1. Remove duplicates based on transaction_id
        initial_count = df.count()
        df = df.dropDuplicates(['transaction_id'])
        duplicates_removed = initial_count - df.count()
        logger.info(f"Duplicates removed: {duplicates_removed}")
        
        # 2. Standardize column names
        for col_name in df.columns:
            new_col_name = col_name.strip().lower().replace(' ', '_').replace('(', '').replace(')', '')
            df = df.withColumnRenamed(col_name, new_col_name)
        
        # Rename specific columns for consistency
        if 'amount_inr' in df.columns:
            df = df.withColumnRenamed('amount_inr', 'amount')
        
        # 3. Convert timestamp to proper datetime format
        df = df.withColumn('timestamp', to_timestamp(col('timestamp'), 'yyyy-MM-dd HH:mm:ss'))
        
        # 4. Convert data types
        df = df.withColumn('amount', col('amount').cast(DoubleType()))
        df = df.withColumn('fraud_flag', col('fraud_flag').cast(IntegerType()))
        df = df.withColumn('hour_of_day', col('hour_of_day').cast(IntegerType()))
        df = df.withColumn('is_weekend', col('is_weekend').cast(IntegerType()))
        
        # 5. Standardize categorical data (trim, lowercase)
        categorical_cols = ['transaction_type', 'merchant_category', 'transaction_status',
                           'sender_age_group', 'receiver_age_group', 'sender_state',
                           'sender_bank', 'receiver_bank', 'device_type', 'network_type',
                           'day_of_week']
        
        for col_name in categorical_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, trim(lower(col(col_name))))
        
        # 6. Handle missing values
        # For amount, use median
        median_amount = df.select('amount').na.drop().approxQuantile('amount', [0.5], 0.01)[0]
        df = df.fillna({'amount': median_amount})
        
        # For categorical columns
        df = df.fillna({
            'transaction_type': 'unknown',
            'merchant_category': 'other',
            'transaction_status': 'unknown',
            'sender_age_group': 'unknown',
            'receiver_age_group': 'unknown',
            'sender_state': 'unknown',
            'device_type': 'unknown',
            'network_type': 'unknown'
        })
        
        # 7. Feature Engineering: Create age_category for sender
        df = df.withColumn('sender_age_category',
            when(col('sender_age_group') == '18-25', 'Teenagers')
            .when(col('sender_age_group') == '26-35', 'Young Adult')
            .when(col('sender_age_group') == '36-45', 'Early Middle Age')
            .when(col('sender_age_group') == '46-55', 'Late Middle Age')
            .when(col('sender_age_group') == '56+', 'Seniors')
            .otherwise('Unknown')
        )
        
        # 8. Feature Engineering: Extract date components
        df = df.withColumn('transaction_date', to_date(col('timestamp')))
        df = df.withColumn('transaction_year', year(col('timestamp')))
        df = df.withColumn('transaction_month', month(col('timestamp')))
        df = df.withColumn('transaction_day', dayofmonth(col('timestamp')))
        
        # 9. Feature Engineering: Transaction amount bins
        df = df.withColumn('amount_category',
            when(col('amount') < 500, 'Very Low')
            .when((col('amount') >= 500) & (col('amount') < 1000), 'Low')
            .when((col('amount') >= 1000) & (col('amount') < 2500), 'Medium')
            .when((col('amount') >= 2500) & (col('amount') < 5000), 'High')
            .when(col('amount') >= 5000, 'Very High')
            .otherwise('Unknown')
        )
        
        # 10. Feature Engineering: Weekend indicator (boolean)
        df = df.withColumn('is_weekend_bool', col('is_weekend') == 1)
        
        # 11. Feature Engineering: Time of day category
        df = df.withColumn('time_of_day',
            when((col('hour_of_day') >= 0) & (col('hour_of_day') < 6), 'Night')
            .when((col('hour_of_day') >= 6) & (col('hour_of_day') < 12), 'Morning')
            .when((col('hour_of_day') >= 12) & (col('hour_of_day') < 18), 'Afternoon')
            .when((col('hour_of_day') >= 18) & (col('hour_of_day') < 24), 'Evening')
            .otherwise('Unknown')
        )
        
        # 12. Data validation: Remove invalid records
        df_clean = df.filter(
            (col('amount') > 0) &
            (col('timestamp').isNotNull()) &
            (col('transaction_id').isNotNull())
        )
        
        rejected_count = df.count() - df_clean.count()
        logger.info(f"Rejected records: {rejected_count}")
        
        # 13. Add data quality metadata
        df_clean = df_clean.withColumn('data_quality_score', lit(100.0))
        df_clean = df_clean.withColumn('is_clean', lit(True))
        df_clean = df_clean.withColumn('cleaning_timestamp', current_timestamp())
        
        logger.info(f"Data cleaning completed. Clean records: {df_clean.count()}")
        
        return df_clean, df.subtract(df_clean)  # Return clean and rejected data
    
    def normalize_amount(self, df):
        """Normalize transaction amount"""
        logger.info("Normalizing amount column")
        
        min_val = df.select(min(col('amount'))).collect()[0][0]
        max_val = df.select(max(col('amount'))).collect()[0][0]
        
        if max_val > min_val:
            df = df.withColumn('amount_normalized',
                (col('amount') - lit(min_val)) / (lit(max_val) - lit(min_val))
            )
        
        return df

def run_cleaning(spark, input_path):
    """Main cleaning function"""
    cleaner = UPIDataCleaner(spark)
    
    # Read raw data
    df = spark.read.parquet(input_path)
    
    # Clean data
    df_clean, df_rejected = cleaner.clean_data(df)
    
    # Normalize amount
    df_clean = cleaner.normalize_amount(df_clean)
    
    return df_clean, df_rejected