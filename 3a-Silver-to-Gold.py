# Databricks notebook source
# MAGIC %md ##Introduction
# MAGIC
# MAGIC  Fraud detection use case needs number of transactions carried out by a customer in last one hour. If this jumps over the predefined threshold, it is flagged as a possible fraud that might need further investigation.
# MAGIC
# MAGIC  In this notebook, we are reading the cleansed transactions from silver table and we are using for foreachBatch to update the trasaction count for a customer in real time.

# COMMAND ----------

# MAGIC %run "./0-config"

# COMMAND ----------

# Define widget
dbutils.widgets.dropdown("site", "primary", ["primary","secondary","primary2","secondary2"],"Choose Primary or Secondary site")

# COMMAND ----------

# Get the value of site
site=dbutils.widgets.get("site")

# COMMAND ----------

config = get_configs(site,{})

# COMMAND ----------

# set current datebase context
_ = spark.catalog.setCurrentDatabase(config['db'])

# COMMAND ----------

from pyspark.sql.functions import *

gold_txn_df=spark.readStream \
  .table(config['silver_table']) \
  .withColumn("event_hour", date_format("event_time", "yyyy-MM-dd-HH")) \
  .groupBy("customer_id", "event_hour") \
  .agg(expr("count(customer_id) as no_of_txn"))

# COMMAND ----------

import json

def upsertToDelta(microBatchOutputDF, epochId):
  spark_session = microBatchOutputDF._jdf.sparkSession() 
  appId = config['gold_stream']

  spark_session.conf().set("spark.databricks.delta.write.txnAppId", config['gold_stream'])
  spark_session.conf().set("spark.databricks.delta.write.txnVersion", epochId)

  metadata = {"stream":onfig['gold_stream'], "batch_id":epochId, "app_id":appId}
  spark_session.conf().set("spark.databricks.delta.commitInfo.userMetadata", json.dumps(metadata))
  # Set the dataframe to view name
  microBatchOutputDF.createOrReplaceTempView("updates")

  # Use the view name to apply MERGE
  # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe
  spark_session.sql("""
    MERGE INTO config['gold_table'] t
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
  .queryName(config['gold_stream'])\
  .outputMode("update") \
  .option("checkpointLocation",config['checkpoint_path']+"/gold") \
  .start()
