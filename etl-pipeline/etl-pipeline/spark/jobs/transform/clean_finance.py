from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FinanceDataCleaner:
    def __init__(self, spark):
        self.spark = spark
        
    def clean_data(self, df):
        """Clean India Personal Finance data"""
        logger.info("Starting data cleaning for India Personal Finance")
        
        # 1. Remove duplicates based on id
        initial_count = df.count()
        df = df.dropDuplicates(['id'])
        duplicates_removed = initial_count - df.count()
        logger.info(f"Duplicates removed: {duplicates_removed}")
        
        # 2. Standardize column names (lowercase, snake_case, trim)
        for col_name in df.columns:
            new_col_name = col_name.strip().lower().replace(' ', '_')
            df = df.withColumnRenamed(col_name, new_col_name)
        
        # 3. Convert data types for numerical columns
        numerical_cols = [
            'income', 'rent', 'loan_repayment', 'insurance', 'groceries',
            'transport', 'eating_out', 'entertainment', 'utilities',
            'healthcare', 'education', 'miscellaneous',
            'desired_savings_percentage', 'desired_savings', 'disposable_income',
            'potential_savings_groceries', 'potential_savings_transport',
            'potential_savings_eating_out', 'potential_savings_entertainment',
            'potential_savings_utilities', 'potential_savings_healthcare',
            'potential_savings_education', 'potential_savings_miscellaneous'
        ]
        
        for col_name in numerical_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
        
        # Convert integer columns
        df = df.withColumn('age', col('age').cast(IntegerType()))
        df = df.withColumn('dependents', col('dependents').cast(IntegerType()))
        
        # 4. Handle missing values
        # For numerical columns, replace with median (using mean as approximation)
        for col_name in numerical_cols:
            if col_name in df.columns:
                median_val = df.select(col_name).na.drop().approxQuantile(col_name, [0.5], 0.01)[0]
                df = df.fillna({col_name: median_val})
        
        # For categorical columns, replace with mode or 'Unknown'
        df = df.fillna({'occupation': 'Unknown', 'city_tier': 'Unknown'})
        
        # 5. Standardize categorical data (trim, lowercase)
        df = df.withColumn('occupation', trim(lower(col('occupation'))))
        df = df.withColumn('city_tier', trim(lower(col('city_tier'))))
        
        # 6. Feature Engineering: Create Age_Category
        df = df.withColumn('age_category',
            when((col('age') >= 18) & (col('age') <= 25), 'Teenagers')
            .when((col('age') >= 26) & (col('age') <= 35), 'Young Adult')
            .when((col('age') >= 36) & (col('age') <= 45), 'Early Middle Age')
            .when((col('age') >= 46) & (col('age') <= 55), 'Late Middle Age')
            .when(col('age') >= 56, 'Seniors')
            .otherwise('Unknown')
        )
        
        # 7. Encoding for occupation (Label Encoding)
        occupation_mapping = {
            'self_employed': 1,
            'retired': 2,
            'student': 3,
            'professional': 4,
            'unknown': 0
        }
        
        occupation_expr = create_map([lit(x) for x in sum(occupation_mapping.items(), ())])
        df = df.withColumn('occupation_encoded', occupation_expr[col('occupation')])
        
        # 8. Encoding for city_tier
        city_tier_mapping = {
            'tier_1': 1,
            'tier_2': 2,
            'tier_3': 3,
            'unknown': 0
        }
        
        city_tier_expr = create_map([lit(x) for x in sum(city_tier_mapping.items(), ())])
        df = df.withColumn('city_tier_encoded', city_tier_expr[col('city_tier')])
        
        # 9. Detect and handle outliers using IQR method
        df = self.handle_outliers(df, numerical_cols[:5])  # Apply to first 5 numerical columns
        
        # 10. Add data quality flags
        df = df.withColumn('data_quality_score', lit(100.0))
        df = df.withColumn('is_clean', lit(True))
        df = df.withColumn('cleaning_timestamp', current_timestamp())
        
        logger.info(f"Data cleaning completed. Clean records: {df.count()}")
        
        return df
    
    def handle_outliers(self, df, columns):
        """Detect and handle outliers using IQR method"""
        for col_name in columns:
            if col_name in df.columns:
                # Calculate Q1, Q3, and IQR
                quantiles = df.select(col_name).na.drop().approxQuantile(col_name, [0.25, 0.75], 0.01)
                Q1, Q3 = quantiles[0], quantiles[1]
                IQR = Q3 - Q1
                
                # Define bounds
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                # Flag outliers but don't remove them
                df = df.withColumn(f'{col_name}_outlier_flag',
                    when((col(col_name) < lower_bound) | (col(col_name) > upper_bound), True)
                    .otherwise(False)
                )
        
        return df
    
    def normalize_columns(self, df, columns):
        """Min-Max normalization for specified columns"""
        logger.info(f"Normalizing columns: {columns}")
        
        for col_name in columns:
            if col_name in df.columns:
                min_val = df.select(min(col(col_name))).collect()[0][0]
                max_val = df.select(max(col(col_name))).collect()[0][0]
                
                if max_val > min_val:
                    df = df.withColumn(f'{col_name}_normalized',
                        (col(col_name) - lit(min_val)) / (lit(max_val) - lit(min_val))
                    )
        
        return df

def run_cleaning(spark, input_path):
    """Main cleaning function"""
    cleaner = FinanceDataCleaner(spark)
    
    # Read raw data
    df = spark.read.parquet(input_path)
    
    # Clean data
    df_clean = cleaner.clean_data(df)
    
    # Normalize income and disposable_income
    df_clean = cleaner.normalize_columns(df_clean, ['income', 'disposable_income'])
    
    return df_clean