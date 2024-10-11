import configparser
import os
import subprocess

import boto3
from pyspark.sql import SparkSession

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


