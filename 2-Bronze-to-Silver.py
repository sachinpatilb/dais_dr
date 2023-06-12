# Databricks notebook source
# MAGIC %md ##Introduction
# MAGIC
# MAGIC In this notebook, we will read data from the bronze table in real time and carry out stateless transactions
# MAGIC - Data qualilty checks
# MAGIC - Data type change for event time
# MAGIC - Adding new features required for fraud detection

# COMMAND ----------

# MAGIC %run "./0-config"

# COMMAND ----------

from pyspark.sql.functions import *

_ = spark \
  .readStream \
  .table(primary_config['bronze_table']) \
  .withColumn("event_time",to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
  .withColumn("diffOrig", col("newBalanceOrig")-col("oldBalanceOrig")) \
  .withColumn("diffDest", col("newBalanceDest")-col("oldBalanceDest")) \
  .filter(col("customer_id").isNotNull()) \
  .filter(col("id").isNotNull()) \
  .writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation",primary_config['checkpoint_path']+"/silver") \
  .queryName(primary_config['silver_stream'])\
  .table(primary_config['silver_table'])
