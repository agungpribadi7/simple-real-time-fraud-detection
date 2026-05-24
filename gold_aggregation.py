import smtplib
from email.mime.text import MIMEText
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum, count

delta_package = "io.delta:delta-spark_2.13:4.1.0"

spark = SparkSession.builder \
    .appName("Gold_Fraud_Aggregation") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", delta_package) \
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

def send_fraud_alert(batch_df, batch_id):
    # Bring the small number of fraud alerts back to the main node
    fraudulent_users = batch_df.collect()
    
    # If the list is empty, no fraud was detected in this batch. Skip the email!
    if not fraudulent_users:
        return 

    # 1. Format the email message body
    alert_text = f"🚨 URGENT: FRAUD DETECTED (Batch {batch_id}) 🚨\n"
    alert_text += "=" * 45 + "\n\n"
    
    for row in fraudulent_users:
        alert_text += f"User ID: {row['user_id']}\n"
        alert_text += f"Total Spent (5m): ${row['total_spent_5m']:.2f}\n"
        alert_text += f"Transaction Count: {row['transaction_count_5m']}\n"
        alert_text += "-" * 45 + "\n"

    sender_email = "p97.agung@gmail.com"
    receiver_email = "bjk18261910@gmail.com"
    password = "mvss ypht pcvl lzzu" 
    
    msg = MIMEText(alert_text)
    msg['Subject'] = f"CRITICAL: {len(fraudulent_users)} Fraudulent Users Detected!"
    msg['From'] = sender_email
    msg['To'] = receiver_email

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, password)
            server.send_message(msg)
        print(f"Successfully sent fraud alert email for batch {batch_id}!")
    except Exception as e:
        print(f"Failed to send email: {e}")

# 4. The Write Stream with foreachBatch
print("Monitoring for fraudulent behavior and routing alerts to email...")
query = gold_df.writeStream \
    .outputMode("update") \
    .foreachBatch(send_fraud_alert) \
    .option("checkpointLocation", "./datalake/gold/checkpoints/fraud_alerts_ckpt") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()
print("Gold processing complete.")
