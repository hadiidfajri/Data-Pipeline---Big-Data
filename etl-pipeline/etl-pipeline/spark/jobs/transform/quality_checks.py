from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataQualityChecker:
    def __init__(self, spark):
        self.spark = spark
        self.quality_results = {}
        
    def check_uniqueness(self, df, columns):
        """Check uniqueness of specified columns"""
        logger.info(f"Checking uniqueness for columns: {columns}")
        
        results = {}
        for col_name in columns:
            if col_name in df.columns:
                total_count = df.count()
                distinct_count = df.select(col_name).distinct().count()
                duplicate_count = total_count - distinct_count
                uniqueness_pct = (distinct_count / total_count * 100) if total_count > 0 else 0
                
                results[col_name] = {
                    'total_count': total_count,
                    'distinct_count': distinct_count,
                    'duplicate_count': duplicate_count,
                    'uniqueness_percentage': round(uniqueness_pct, 2),
                    'passed': duplicate_count == 0
                }
                
                logger.info(f"{col_name}: {uniqueness_pct:.2f}% unique")
        
        self.quality_results['uniqueness'] = results
        return results
    
    def check_null_values(self, df):
        """Check for null values in all columns"""
        logger.info("Checking for null values")
        
        total_count = df.count()
        results = {}
        
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_pct = (null_count / total_count * 100) if total_count > 0 else 0
            
            results[col_name] = {
                'null_count': null_count,
                'null_percentage': round(null_pct, 2),
                'passed': null_count == 0
            }
            
            if null_count > 0:
                logger.warning(f"{col_name}: {null_count} nulls ({null_pct:.2f}%)")
        
        self.quality_results['null_check'] = results
        return results
    
    def check_range(self, df, range_rules):
        """
        Check if values are within expected ranges
        range_rules: dict like {'amount': (0, 100000), 'age': (18, 100)}
        """
        logger.info("Checking value ranges")
        
        results = {}
        
        for col_name, (min_val, max_val) in range_rules.items():
            if col_name in df.columns:
                out_of_range_count = df.filter(
                    (col(col_name) < min_val) | (col(col_name) > max_val)
                ).count()
                
                total_count = df.filter(col(col_name).isNotNull()).count()
                out_of_range_pct = (out_of_range_count / total_count * 100) if total_count > 0 else 0
                
                results[col_name] = {
                    'min_allowed': min_val,
                    'max_allowed': max_val,
                    'out_of_range_count': out_of_range_count,
                    'out_of_range_percentage': round(out_of_range_pct, 2),
                    'passed': out_of_range_count == 0
                }
                
                if out_of_range_count > 0:
                    logger.warning(f"{col_name}: {out_of_range_count} values out of range [{min_val}, {max_val}]")
        
        self.quality_results['range_check'] = results
        return results
    
    def check_datatype_consistency(self, df, expected_types):
        """
        Check if columns have expected data types
        expected_types: dict like {'amount': 'double', 'age': 'int'}
        """
        logger.info("Checking datatype consistency")
        
        results = {}
        type_mapping = {
            'string': StringType(),
            'int': IntegerType(),
            'long': LongType(),
            'double': DoubleType(),
            'float': FloatType(),
            'boolean': BooleanType(),
            'timestamp': TimestampType(),
            'date': DateType()
        }
        
        for col_name, expected_type in expected_types.items():
            if col_name in df.columns:
                actual_type = str(df.schema[col_name].dataType)
                expected_type_obj = str(type_mapping.get(expected_type, expected_type))
                
                passed = expected_type in actual_type.lower()
                
                results[col_name] = {
                    'expected_type': expected_type,
                    'actual_type': actual_type,
                    'passed': passed
                }
                
                if not passed:
                    logger.warning(f"{col_name}: Expected {expected_type}, got {actual_type}")
        
        self.quality_results['datatype_check'] = results
        return results
    
    def check_referential_integrity(self, df, parent_col, child_col):
        """Check if all child values exist in parent values"""
        logger.info(f"Checking referential integrity: {child_col} -> {parent_col}")
        
        parent_values = df.select(parent_col).distinct()
        child_values = df.select(child_col).distinct()
        
        # Find orphan records
        orphans = child_values.join(parent_values, child_values[child_col] == parent_values[parent_col], 'left_anti')
        orphan_count = orphans.count()
        
        result = {
            'parent_column': parent_col,
            'child_column': child_col,
            'orphan_count': orphan_count,
            'passed': orphan_count == 0
        }
        
        if orphan_count > 0:
            logger.warning(f"Found {orphan_count} orphan records in {child_col}")
        
        self.quality_results['referential_integrity'] = result
        return result
    
    def check_distribution(self, df, numerical_columns):
        """Check statistical distribution of numerical columns"""
        logger.info("Checking data distribution")
        
        results = {}
        
        for col_name in numerical_columns:
            if col_name in df.columns:
                stats = df.select(
                    col(col_name)
                ).summary('count', 'mean', 'stddev', 'min', '25%', '50%', '75%', 'max').collect()
                
                stats_dict = {row[0]: float(row[1]) if row[1] else None for row in stats}
                
                # Check for skewness (simple check)
                if stats_dict.get('mean') and stats_dict.get('50%'):
                    skewness = abs(stats_dict['mean'] - stats_dict['50%']) / stats_dict.get('stddev', 1)
                    is_skewed = skewness > 1
                else:
                    is_skewed = False
                
                results[col_name] = {
                    'statistics': stats_dict,
                    'is_skewed': is_skewed,
                    'passed': True  # Distribution check always passes, just informational
                }
                
                logger.info(f"{col_name} statistics: mean={stats_dict.get('mean'):.2f}, median={stats_dict.get('50%'):.2f}")
        
        self.quality_results['distribution'] = results
        return results
    
    def generate_quality_report(self):
        """Generate comprehensive quality report"""
        logger.info("Generating quality report")
        
        total_checks = 0
        passed_checks = 0
        
        for check_type, results in self.quality_results.items():
            if isinstance(results, dict):
                if 'passed' in results:  # Single check result
                    total_checks += 1
                    if results['passed']:
                        passed_checks += 1
                else:  # Multiple check results
                    for item_result in results.values():
                        if isinstance(item_result, dict) and 'passed' in item_result:
                            total_checks += 1
                            if item_result['passed']:
                                passed_checks += 1
        
        quality_score = (passed_checks / total_checks * 100) if total_checks > 0 else 0
        
        report = {
            'timestamp': str(current_timestamp()),
            'total_checks': total_checks,
            'passed_checks': passed_checks,
            'failed_checks': total_checks - passed_checks,
            'quality_score': round(quality_score, 2),
            'details': self.quality_results
        }
        
        logger.info(f"Quality Score: {quality_score:.2f}% ({passed_checks}/{total_checks} checks passed)")
        
        return report

def run_quality_checks(spark, df, check_config):
    """Main quality check function"""
    checker = DataQualityChecker(spark)
    
    # 1. Uniqueness check
    if 'uniqueness_columns' in check_config:
        checker.check_uniqueness(df, check_config['uniqueness_columns'])
    
    # 2. Null check
    checker.check_null_values(df)
    
    # 3. Range check
    if 'range_rules' in check_config:
        checker.check_range(df, check_config['range_rules'])
    
    # 4. Datatype consistency
    if 'expected_types' in check_config:
        checker.check_datatype_consistency(df, check_config['expected_types'])
    
    # 5. Distribution check
    if 'numerical_columns' in check_config:
        checker.check_distribution(df, check_config['numerical_columns'])
    
    # Generate report
    report = checker.generate_quality_report()
    
    return report