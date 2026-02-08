from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaToHDFSStreaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema (Spark will ignore extra fields)
schema = StructType([
    StructField("status", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("length", DoubleType(), True),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("level", StringType(), True),
    StructField("userId", StringType(), True),
    StructField("sessionId", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("page", StringType(), True),
    StructField("song", StringType(), True),
    StructField("artist", StringType(), True),
    StructField("ts", LongType(), True),
])

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "music_events") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Convert value to JSON
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

parsed_df = json_df.select(
    from_json(col("json_str"), schema).alias("data")
).select("data.*")

# HDFS paths
output_path = "hdfs://localhost:9000/projects/churn_stream/raw"
checkpoint_path = "hdfs://localhost:9000/projects/churn_stream/checkpoints"

# Write to HDFS as Parquet
query = parsed_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime="30 seconds") \
    .start()

print("🔥 Kafka → Spark → HDFS streaming started...")

query.awaitTermination()
