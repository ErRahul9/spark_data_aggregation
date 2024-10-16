import argparse
import logging
import os

from pyspark.sql import functions as f, SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, hour, date_format
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from spark_jobs.spark_connector import spark_connection
import time

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)
parser = argparse.ArgumentParser(description="A description of your script.")
parser.add_argument("date", type=str, help="date")
parser.add_argument("hour", type=str, help="hour")
parser.add_argument("output_location", type=str, help="output location for aggregated data")
args = parser.parse_args()


def spark_connection():
    jar_path = "libs"
    jar_files = [
            "hadoop-aws-3.2.2.jar",
            "redshift-jdbc42-2.1.0.30.jar" ,
            "aws-java-sdk-bundle-1.11.874.jar"
        ]
    jars = ",".join([os.path.join(jar_path, jar) for jar in jar_files])
    return SparkSession.builder \
        .appName("MySparkApp") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")\
        .config("spark.jars", jars) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.sql.shuffle.partitions", "40") \
        .getOrCreate()



def spark_runner_hrly(date,part_time,s3_output_path):
    spark = spark_connection()
    S3_path = 's3a://mntn-data-archive-prod/bid_price_log_v2/dt={}/hh={}/cgid_mod=74'.format(date,part_time).replace("'","")
    logger.info("S3 path: {}".format(S3_path))
    df = spark.read.parquet(S3_path)
    start_time = time.time()
    nested_schema = StructType([
        StructField("flight_campaign_spend", StringType(), True),
        StructField("flight_campaign_spend_cap", StringType(), True),
        StructField("flight_campaign_group_spend", StringType(), True),
        StructField("flight_campaign_group_spend_cap", StringType(), True),
        StructField("daily_campaign_group_spend", StringType(), True),
        StructField("daily_campaign_group_spend_cap", StringType(), True),
        StructField("daily_campaign_spend", StringType(), True),
        StructField("daily_campaign_spend_cap", StringType(), True)
    ])
    parsed_df = df.withColumn("pacing_debug_data", from_json(col("pacing_debug_data"), nested_schema))
    result_df = parsed_df.select("epoch",
                    "campaign_group_id",
                     "campaign_id",
                    "flight_id",
                    col("pacing_debug_data.flight_campaign_spend").alias("flight_campaign_spend"),
                    col("pacing_debug_data.flight_campaign_spend_cap").alias("flight_campaign_spend_cap"),
                    col("pacing_debug_data.daily_campaign_group_spend").alias("daily_campaign_group_spend"),
                    col("pacing_debug_data.daily_campaign_group_spend_cap").alias("daily_campaign_group_spend_cap"),
                    col("pacing_debug_data.daily_campaign_spend").alias("daily_campaign_spend"),
                    col("pacing_debug_data.daily_campaign_spend_cap").alias("daily_campaign_spend_cap"),
                    (col("campaign_group_id") % 100).alias("cgid_mod"),
                    (f.date_format(f.from_unixtime(f.col("epoch") / 1000), "yyyy-MM-dd")).alias("date"),
                    (f.hour(f.from_unixtime(f.col("epoch")/1000))).alias("hour"),
                    (f.minute(f.from_unixtime(f.col("epoch") / 1000))).alias("minute")

    )
    aggregated_data = result_df.groupby(col("campaign_group_id"),col("campaign_id"),col("flight_id")\
                        ,col("date"),col("hour"),col("minute"),col("cgid_mod"))\
                        .agg(f.max(col("flight_campaign_spend")).alias('max_flight_campaign_spend'),\
                        f.max(col("flight_campaign_spend_cap")).alias('max_flight_campaign_spend_cap'),\
                        f.max(col("daily_campaign_group_spend")).alias('max_daily_campaign_group_spend'),\
                        f.max(col("daily_campaign_group_spend_cap")).alias('max_daily_campaign_group_spend_cap'),\
                        f.max(col("daily_campaign_spend")).alias('max_daily_campaign_spend'),\
                        f.max(col("daily_campaign_spend_cap")).alias('max_daily_campaign_spend_cap')
                             ).orderBy(col("date"),col("hour"),col("cgid_mod"))

    aggregated_data.show(truncate=False)
    # row_count = aggregated_data.count()
    aggregated_data.write \
        .partitionBy("date", "hour", "cgid_mod") \
        .mode("overwrite")  \
        .parquet(s3_output_path.split("=")[1])
    end_time = time.time()
    execution_time = end_time -start_time
    logging.info(f"Execution time: {execution_time}")
    logging.info(f'job ran for {start_time} to {end_time} for {execution_time}')
    spark.stop()

if __name__ == "__main__":
    date = args.date.split("=")[1]
    part_time = args.hour.split("=")[1]
    s3_output_path = args.output_location
    logger.info(f'running aggregation for {date} for the {time} hour and writing output to {s3_output_path} location')
    spark_runner_hrly(date,part_time,s3_output_path)
