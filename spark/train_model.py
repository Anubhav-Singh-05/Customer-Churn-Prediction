from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("ChurnModelTraining").getOrCreate()

# =============================
# 1. Load curated features
# =============================
input_path = "hdfs://localhost:9000/projects/churn_stream/curated/user_features"
df = spark.read.parquet(input_path)

print("Total records:", df.count())
df.printSchema()

# =============================
# 2. Create REALISTIC LABEL (with noise)
# =============================
df = df.withColumn(
    "label",
    when((col("total_songs_played") < 30) & (rand() < 0.7), 1)
    .when((col("total_songs_played") >= 30) & (rand() < 0.2), 1)
    .otherwise(0)
)

# =============================
# 3. Feature Vector
# =============================
feature_cols = ["total_songs_played", "avg_song_length"]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

# =============================
# 4. Train/Test Split
# =============================
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# =============================
# 5. Model
# =============================
lr = LogisticRegression(
    featuresCol="features",
    labelCol="label"
)

pipeline = Pipeline(stages=[assembler, lr])

# =============================
# 6. Train
# =============================
model = pipeline.fit(train_df)

# =============================
# 7. Predict
# =============================
predictions = model.transform(test_df)

# =============================
# 8. Evaluate
# =============================
evaluator = BinaryClassificationEvaluator(
    labelCol="label",
    metricName="areaUnderROC"
)

auc = evaluator.evaluate(predictions)
print("AUC =", auc)

# =============================
# 9. Save model
# =============================
model_path = "hdfs://localhost:9000/projects/churn_stream/models/churn_lr_realistic"
model.write().overwrite().save(model_path)

# =============================
# 10. Save predictions
# =============================
output_path = "hdfs://localhost:9000/projects/churn_stream/curated/predictions_realistic"

predictions.select(
    "userId", "label", "prediction", "probability"
).write.mode("overwrite").parquet(output_path)

print("Predictions saved to:", output_path)

spark.stop()
