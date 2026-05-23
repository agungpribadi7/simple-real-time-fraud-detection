from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum, count

spark = SparkSession.builder \
    .appName("Gold_Fraud_Aggregation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Starting Gold processing: Calculating Fraud Metrics...")

# Notice we don't need a schema here! Silver Parquet is strictly typed and Spark can read it perfectly.
silver_stream = spark.readStream \
    .schema(spark.read.parquet("./datalake/silver/transactions").schema) \
    .parquet("./datalake/silver/transactions")

# The Gold Transformation (Aggregating User Behavior)
gold_df = silver_stream \
    .withColumn("event_time", col("timestamp").cast("timestamp")) \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("user_id")
    ) \
    .agg(
        sum("amount").alias("total_spent_5m"),
        count("transaction_id").alias("transaction_count_5m")
    ) \
    .filter((col("transaction_count_5m") >= 3) | (col("total_spent_5m") > 5000))

# 4. Write output to CONSOLE first so we can see the fraud alerts instantly!
print("Monitoring for fraudulent behavior...")
query = gold_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()
print("Gold processing complete.")