# Databricks notebook source
# MAGIC %md ##Introduction
# MAGIC
# MAGIC  Fraud detection use case needs number of transactions carried out by a customer in last one hour. If this jumps over the predefined threshold, it is flagged as a possible fraud that might need further investigation.
# MAGIC
# MAGIC  In this notebook, we are reading the cleansed transactions from silver table and we are using for foreachBatch to update the trasaction count for a customer in real time.

# COMMAND ----------

# MAGIC %run "./0-config"

# COMMAND ----------

from pyspark.sql.functions import *

gold_txn_df=spark.readStream \
  .table(primary_config['silver_table']) \
  .withColumn("event_hour", date_format("event_time", "yyyy-MM-dd-HH")) \
  .groupBy("customer_id", "event_hour") \
  .agg(expr("count(customer_id) as no_of_txn"))

# COMMAND ----------

import json

def upsertToDelta(microBatchOutputDF, epochId):
  spark_session = microBatchOutputDF._jdf.sparkSession() 
  appId = primary_config['gold_stream_a']

  spark_session.conf().set("spark.databricks.delta.write.txnAppId", primary_config['gold_stream_a'])
  spark_session.conf().set("spark.databricks.delta.write.txnVersion", epochId)

  metadata = {"stream":primary_config['gold_stream_a'], "batch_id":epochId, "app_id":appId}
  spark_session.conf().set("spark.databricks.delta.commitInfo.userMetadata", json.dumps(metadata))
  # Set the dataframe to view name
  microBatchOutputDF.createOrReplaceTempView("updates")

  # Use the view name to apply MERGE
  # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe
  spark_session.sql("""
    MERGE INTO gold_txn_live t
    USING updates s
    ON s.customer_id = t.customer_id and s.event_hour=t.event_hour 
    WHEN MATCHED THEN UPDATE SET t.no_of_txn=s.no_of_txn+t.no_of_txn
    WHEN NOT MATCHED THEN INSERT *
  """)

# COMMAND ----------

# Start the query to continuously upsert into aggregates tables in update mode
gold_txn_df.writeStream \
  .format("delta") \
  .foreachBatch(upsertToDelta) \
  .queryName(primary_config['gold_stream_a'])\
  .outputMode("update") \
  .option("checkpointLocation",primary_config['checkpoint_path']+"/gold") \
  .start()

# COMMAND ----------


