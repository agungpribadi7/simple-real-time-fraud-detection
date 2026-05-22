from pyspark.sql import SparkSession
from pyspark.sql.functions import col, parse_json 

spark = SparkSession.builder \
    .appName("Bronze-Kafka-Ingestion-Variant") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Connecting to Kafka Cluster over SSL...")

kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094") \
    .option("subscribe", "raw_transactions_v2") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 5000) \
    .option("kafka.security.protocol", "SSL") \
    .option("kafka.ssl.truststore.location", "secrets/kafka.truststore.jks") \
    .option("kafka.ssl.truststore.password", "confluent") \
    .option("kafka.ssl.endpoint.identification.algorithm", "") \
    .load()

parsed_stream = kafka_stream \
    .selectExpr("CAST(value AS STRING) as raw_json", "timestamp as kafka_timestamp") \
    .select(parse_json(col("raw_json")).alias("variant_data"), col("kafka_timestamp"))

print("Stream configured with VARIANT! Waiting for data...")

query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()