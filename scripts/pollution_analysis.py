from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, date_format
import yaml
import os

def load_config():
    """Load configuration from YAML file"""
    config_path = "/root/config/config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)

def create_spark_session(config):
    """Create and configure Spark session"""
    builder = SparkSession.builder \
        .appName(config['spark']['app_name']) \
        .config("spark.sql.adaptive.enabled", "false")
    
    return builder.getOrCreate()

def analyze_pollution_data(spark, config):
    """Main analysis function"""
    try:
        # Read the dataset
        df = spark.read.csv(
            config['hdfs']['input_path'],
            header=True,
            inferSchema=True
        )
        
        # Print dataset info
        print("=== Dataset Information ===")
        row_count = df.count()
        col_count = len(df.columns)
        print(f"Total rows: {row_count}")
        print(f"Total columns: {col_count}")
        
        # Show column names and sample data
        print("\nColumns:", df.columns)
        print("\n=== Sample Data ===")
        df.show(5, truncate=False)
        
        # Calculate daily averages for pollutants
        daily_metrics = df.groupBy(
            date_format('Date', 'yyyy-MM-dd').alias('date')
        ).agg(
            avg('CO(GT)').alias('avg_co'),
            avg('NOx(GT)').alias('avg_nox'),
            avg('NO2(GT)').alias('avg_no2'),
            avg('Temperature').alias('avg_temp'),
            avg('RH').alias('avg_humidity')
        )
        
        # Basic statistics for key metrics
        pollutants = ['CO(GT)', 'NOx(GT)', 'NO2(GT)', 'Temperature', 'RH']
        stats_df = df.select(pollutants).describe()
        
        # Save results
        output_base = config['hdfs']['output_path']
        
        # Save daily metrics
        daily_metrics.write.mode("overwrite").csv(
            f"{output_base}/daily_metrics",
            header=True
        )
        
        # Save statistics
        stats_df.write.mode("overwrite").csv(
            f"{output_base}/statistics",
            header=True
        )
        
        # Save a sample of raw data
        df.limit(100).write.mode("overwrite").csv(
            f"{output_base}/sample_data",
            header=True
        )
        
        print("\n=== Analysis Results ===")
        print("Results have been saved to HDFS:")
        print(f"1. Daily metrics: {output_base}/daily_metrics")
        print(f"2. Statistics: {output_base}/statistics")
        print(f"3. Sample data: {output_base}/sample_data")
        
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def main():
    config = load_config()
    spark = create_spark_session(config)
    
    try:
        analyze_pollution_data(spark, config)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
