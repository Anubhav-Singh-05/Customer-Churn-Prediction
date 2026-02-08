from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

spark = SparkSession.builder \
    .appName("ChurnBatchProcessing") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read raw parquet data
df = spark.read.parquet("hdfs://localhost:9000/projects/churn_stream/raw")

print("Raw count:", df.count())

# Keep only song play events
songs_df = df.filter(col("page") == "NextSong")

# Feature engineering per user
user_features = songs_df.groupBy("userId").agg(
    count("*").alias("total_songs_played"),
    avg("length").alias("avg_song_length")
)

# Output path
output_path = "hdfs://localhost:9000/projects/churn_stream/curated/user_features"

# Write curated data
user_features.write.mode("overwrite").parquet(output_path)

print("Curated data written to HDFS.")

