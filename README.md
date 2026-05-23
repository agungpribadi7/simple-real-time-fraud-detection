# simple-real-time-fraud-detection
- 22 May 2026
Generating data using kafka producer, utilizing 3 kafka node and 3 spark worker, after 16 minutes producing message to a kafka topic, entire docker hit not responding.
- 23 May 2026
Witchcrafting TLS between different containers to secure the communication, add email notification if there is an error in airflow task, make bronze_ingestion run once every 5 minutes to save up CPU workload with trigger(availableNow=True) spark command with airflow BashOperator, all medallion are run concurrently in 1 DAG, openjdk 17 because spark uses newest version
*Note:
-- in venv terminal do:
  pip install pyspark
  brew install openjdk@17 
  export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
  export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
