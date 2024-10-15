import configparser
import os
import subprocess
import time

import boto3
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, hour, date_format
from pyspark.sql.types import StringType, StructType, StructField, IntegerType

from spark_jobs.cred_setter import set_creds
from spark_jobs.spark_connector import spark_connection

set_creds()
spark = spark_connection()
date = "2024-10-08"
hour = "04"

df = spark.read.parquet(S3_path)
new_df = df.withColumn("dt",f.lit(date)) \
        .withColumn("hh",f.lit(hour))
src_count = df.count()
start_time = time.time()

def spark_runner(date=date , hour=time):
    S3_path = f's3a://mntn-data-archive-prod/bid_price_log_v2/dt={date}/hh={hour}'
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
    #
    parsed_df = new_df.withColumn("pacing_debug_data", from_json(col("pacing_debug_data"), nested_schema))
    result_df = parsed_df.select(
                    "dt",
                    "hh",
                    "cgid_mod",
                    "epoch",
                    "campaign_group_id",
                    "campaign_id",
                    "flight_id",
                    col("pacing_debug_data.flight_campaign_spend").alias("flight_campaign_spend"),
                    col("pacing_debug_data.flight_campaign_spend_cap").alias("flight_campaign_spend_cap"),
                    col("pacing_debug_data.daily_campaign_group_spend").alias("daily_campaign_group_spend"),
                    col("pacing_debug_data.daily_campaign_group_spend_cap").alias("daily_campaign_group_spend_cap"),
                    col("pacing_debug_data.daily_campaign_spend").alias("daily_campaign_spend"),
                    col("pacing_debug_data.daily_campaign_spend_cap").alias("daily_campaign_spend_cap")
    )
    aggregated_data = result_df.groupby(col("campaign_group_id"),col("campaign_id"),col("flight_id"),
                        col("dt").alias("date"),col("hh").alias("hour"),col("cgid_mod").alias("cgid_mod"))\
                        .agg(f.max(col("flight_campaign_spend")).alias('hrly_flight_campaign_spend'),\
                        f.max(col("flight_campaign_spend_cap")).alias('hrly_flight_campaign_spend_cap'),\
                        f.max(col("daily_campaign_group_spend")).alias('hrly_daily_campaign_group_spend'),\
                        f.max(col("daily_campaign_group_spend_cap")).alias('hrly_daily_campaign_group_spend_cap'),\
                        f.max(col("daily_campaign_spend")).alias('hrly_daily_campaign_spend'),\
                        f.max(col("daily_campaign_spend_cap")).alias('hrly_daily_campaign_spend_cap')
                             ).orderBy(col("date"),col("hour"),col("cgid_mod"))
    aggregated_data.show(truncate=False)
    s3_output_path = "s3a://bidpricelogaggregate/date_time/"
    row_count = aggregated_data.count()
    aggregated_data.write \
        .partitionBy("date", "hour", "cgid_mod") \
        .mode("overwrite") \
        .parquet(s3_output_path)
    end_time = time.time()
    execution_time = end_time -start_time
    print(f'job ran for {start_time} to {end_time} for {execution_time} and aggregated {src_count} to {row_count} records')
    spark.stop()
