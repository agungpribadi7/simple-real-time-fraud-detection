from pyspark.sql import SparkSession
from delta.tables import DeltaTable

delta_package = "io.delta:delta-spark_2.13:4.1.0"

spark = SparkSession.builder \
    .appName("Delta_Janitor") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", delta_package) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🧹 Running Delta Lake VACUUM on Bronze (1 hour retention)...")
bronze_table = DeltaTable.forPath(spark, "./datalake/bronze/transactions")
bronze_table.optimize().executeCompaction()
bronze_table.vacuum(1)  # 1 hour

print("🧹 Running Delta Lake VACUUM on Silver (2 hours retention)...")
silver_table = DeltaTable.forPath(spark, "./datalake/silver/transactions")
silver_table.optimize().executeCompaction()
silver_table.vacuum(2)

print("✅ Data Lake cleanup complete!")