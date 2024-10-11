from pyspark.sql import SparkSession

from spark_jobs.spark_connector import spark_connection


# cre
speak_conf = spark_connection()
redis_host = "localhost"
redis_port = 6379
redis_password = None
speak_conf.conf.set("redis.host", redis_host)
speak_conf.conf.set("redis.port", redis_port)
if redis_password:
    speak_conf.conf.set("redis.password", redis_password)

df = speak_conf.read \
    .format("org.apache.spark.redis") \
    .option("key.pattern", "user:*") \
    .load()


df.show()

# Stop the Spark session
speak_conf.stop()