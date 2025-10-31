from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import yaml
import os

try:
    import hbase_storage
    HBASE_OK = True
except Exception:
    HBASE_OK = False

def load_config():
    cfg_path = '/root/config/config.yaml'
    with open(cfg_path, 'r') as f:
        return yaml.safe_load(f)

def create_spark_session(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

def process_data(spark, input_path):
    # Define schema for stability
    schema = StructType([
        StructField("Date", StringType(), True),
        StructField("Time", StringType(), True),
        StructField("CO(GT)", FloatType(), True),
        StructField("PT08.S1(CO)", FloatType(), True),
        StructField("NMHC(GT)", FloatType(), True),
        StructField("C6H6(GT)", FloatType(), True),
        StructField("PT08.S2(NMHC)", FloatType(), True),
        StructField("NOx(GT)", FloatType(), True),
        StructField("PT08.S3(NOx)", FloatType(), True),
        StructField("NO2(GT)", FloatType(), True)
    ])

    df = spark.read.csv(input_path, header=True, schema=schema)

    # Calculate metrics
    daily_metrics = df.groupBy("Date").agg(
        F.avg("CO(GT)").alias("avg_co"),
        F.avg("NOx(GT)").alias("avg_nox"),
        F.avg("NO2(GT)").alias("avg_no2")
    )

    return daily_metrics

def main():
    cfg = load_config()
    spark = create_spark_session(cfg['spark']['app_name'])
    input_path = cfg['hdfs']['input_path']
    output_path = cfg['hdfs']['output_path']
    metrics = process_data(spark, input_path)
    
    # Save to HDFS
    metrics.write.mode("overwrite").csv(output_path, header=True)
    
    # Store in HBase
    if HBASE_OK:
        try:
            table = hbase_storage.create_table()
            for row in metrics.collect():
                hbase_storage.store_metrics(table, row.Date, {
                    'co': row.avg_co,
                    'nox': row.avg_nox,
                    'no2': row.avg_no2
                })
        except Exception:
            pass

if __name__ == "__main__":
    main()