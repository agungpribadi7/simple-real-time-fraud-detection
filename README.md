# Simple Real-Time Fraud Detection
🛡️ A real-time data engineering pipeline built to ingest, process, and detect fraudulent transactions using a Medallion Architecture.

📖 # Development Log
# May 24, 2026: Production-Ready Architecture
🧹 Automated Data Lake Cleanup: Implemented a system to delete old Parquet files across all Medallion layers at predefined retention hours.
📦 Delta Lake Upgrade: Transitioned the output format of every Medallion layer from standard Spark to Apache Delta Lake. This heavily simplifies bookkeeping, optimizes read/write operations, and efficiently garbage-collects old Parquet files.
📧 Fraud Alerting: Configured an automated email trigger within the Gold Medallion layer to instantly notify stakeholders when fraudulent activity is detected in the rolling time window.

# May 23, 2026: Security, Orchestration & Optimization
🔒 Security: "Witchcrafting" TLS between different containers to fully secure internal communication.
🚨 Alerting: Added email notifications for Airflow task failures.
⚙️ Orchestration: All Medallion layers (Bronze, Silver, Gold) are now running concurrently within a single Airflow DAG.
🚀 Optimization: Upgraded bronze_ingestion to run once every 5 minutes using the Airflow BashOperator and Spark's .trigger(availableNow=True) command. This massive CPU workload optimization prevents the cluster from locking up.
🗑️ Log Management: To prevent Bronze ingestion from stopping after 16 minutes, utilized the Kafka UI (http://localhost:8085) to set a maximum size limit on topics, ensuring Kafka automatically deletes old messages to preserve disk space.

# May 22, 2026: Stress Testing
Generated stream data using a Kafka Producer.
Utilized a heavy cluster configuration: 3 Kafka Nodes and 3 Spark Workers.
Result: After 16 minutes of continuous message production to a Kafka topic, the entire Docker stack hit an Out-of-Memory/CPU lock and stopped responding. (Addressed by the May 23 optimizations).

# Local Environment Setup
1. Configure the Python & Java Environment
Because PySpark relies on a modern Java backend, this project strictly requires OpenJDK 17.
Run the following commands in your terminal:
Bash
# Activate your virtual environment
source venv/bin/activate

# Install PySpark and Java 17
pip install pyspark
brew install openjdk@17 

# Map the Java Home Path for Spark
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
2. Generate TLS Security Files
Secure communication between the Docker containers requires local certificates. From the root directory of the project, run:
Bash
./generate_certs.sh
3. Configure Local DNS for Kafka Routing
To ensure TLS works seamlessly between containers, you must map your local host environment to Docker's internal networking. This allows other containers to resolve the KAFKA_ADVERTISED_LISTENERS domain correctly.
Edit your local hosts file:
Bash
sudo nano /etc/hosts
Add the following line to the file, save, and exit:
Plaintext
127.0.0.1       host.docker.internal
🧠 Architecture & Core Concepts
Infrastructure Decisions
Java 17: Utilizing OpenJDK 17 is critical. Modern Spark leverages the newest Java versions, and modern Kafka using this architecture no longer requires Zookeeper to maintain master-slave nodes (saving up to 2GB of memory). Furthermore, it allows Spark to utilize the Variant object, bypassing the need to hustle object types with StructType and manually manage memory bytes.
Cluster Sizing: This repository utilizes 3 Spark Executors and 3 Kafka Nodes running in parallel inside a TLS-secured connection.
Spark Execution Plan Under the Hood
Understanding how Spark processes data in this pipeline:
The Job: An action from Spark (like readStream or writeStream) creates a Job (e.g., Read JSON → Prettify format → Write to Delta).
The Stage: The Spark engine breaks the Job into Stages. A new stage is created anytime data must be shuffled across the network (e.g., groupBy(), join(), or window()). Because bronze_ingestion features no complex aggregations, the entire process runs as 1 Stage.
The Task & Executors: This Stage is further broken down into 3 Tasks (matching our 3 Kafka partitions). By default, Spark launches exactly 1 Executor inside each Worker container. Each Worker can be configured to allocate multiple CPU cores, allowing each CPU core to handle a specific Task in parallel.
