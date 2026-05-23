from pyspark.sql import SparkSession
from pyspark.sql.functions import col, parse_json, when, raise_error, concat, lit

spark = SparkSession.builder \
    .appName("Bronze-Kafka-Ingestion-Variant") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Connecting to Kafka Cluster over SSL...")
#.option("kafka.ssl.keystore.location", "/opt/airflow/secrets/kafka1.keystore.jks") \

kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host.docker.internal:9092,host.docker.internal:9093,host.docker.internal:9094") \
    .option("subscribe", "raw_transactions_v2") \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SSL") \
    .option("kafka.ssl.truststore.location", "./secrets/kafka.truststore.jks") \
    .option("kafka.ssl.truststore.password", "confluent") \
    .option("kafka.ssl.keystore.location", "./secrets/kafka1.keystore.jks") \
    .option("kafka.ssl.keystore.password", "confluent") \
    .option("kafka.ssl.key.password", "confluent") \
    .option("kafka.ssl.endpoint.identification.algorithm", "") \
    .load()

raw_stream = kafka_stream \
    .selectExpr("CAST(value AS STRING) as raw_json", "timestamp as kafka_timestamp")

# 2. Parse the JSON and force a crash if it fails
parsed_stream = raw_stream \
    .withColumn("variant_data", parse_json(col("raw_json"))) \
    .withColumn(
        "variant_data",
        when(
            col("variant_data").isNull(), 
            # If null, crash the pipeline and print the bad string!
            raise_error(concat(lit("CRITICAL: Invalid JSON detected -> "), col("raw_json")))
        ).otherwise(
            # If valid, keep the variant data
            col("variant_data")
        )
    ) \
    .select("variant_data", "kafka_timestamp")

query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "./datalake/bronze/transactions") \
    .option("checkpointLocation", "./datalake/bronze/checkpoints/transactions_ckpt") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()

print("Batch processing complete. Exiting cleanly.")
