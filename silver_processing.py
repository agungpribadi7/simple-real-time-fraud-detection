from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("Silver_Fraud_Processing") \
    .getOrCreate()

print("Starting Silver processing: Enforcing Data Contracts...")

# 2. Define the Strict Silver Payload Schema (The "Bouncer")
# These keys match your producer perfectly
payload_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("ip_address", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# 3. DYNAMICALLY read the exact Bronze schema
# This prevents us from ever guessing the column names or Variant types wrong!
bronze_schema = spark.read.parquet("./datalake/bronze/transactions").schema

# 4. Read the Bronze Data Lake
print("Reading new data from Bronze layer...")
bronze_stream = spark.readStream \
    .schema(bronze_schema) \
    .parquet("./datalake/bronze/transactions")

# 5. The Silver Transformation (Unpack Variant, Flatten, & Filter)
silver_df = bronze_stream \
    .withColumn("raw_string", col("variant_data").cast("string")) \
    .withColumn("structured_data", from_json(col("raw_string"), payload_schema)) \
    .select("structured_data.*", "kafka_timestamp") \
    .filter(col("transaction_id").isNotNull()) \
    .filter(col("amount") > 0) 

# 6. Write to Silver Data Lake
print("Writing clean, flattened data to Silver layer...")
query = silver_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "./datalake/silver/transactions") \
    .option("checkpointLocation", "./datalake/silver/checkpoints/transactions_ckpt") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()
print("Silver processing complete. Clean data securely stored!")