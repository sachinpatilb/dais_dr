# Databricks notebook source
# MAGIC %md ##Introduction
# MAGIC
# MAGIC Fraud detection use case needs number of transactions carried out by a customer in last one hour. If this jumps over the predefined threshold, it is flagged as a possible fraud that might need further investigation.
# MAGIC
# MAGIC In this notebook, we are reading the cleansed transactions from silver table and we are using stateful transformation with window and watermark to write the transaction in gold table

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

gold_txn_df = spark.readStream \
  .table(config['silver_table']) \
  .withWatermark("event_time", "60 minutes") \
  .groupBy(window("event_time", "60 minutes"), "customer_id") \
  .agg(expr("count(customer_id) as no_of_txn")) \
  .selectExpr("window.start as start_time", "customer_id", "no_of_txn") \
  .writeStream \
  .format("delta") \
  .queryName(config['gold_stream'])\
  .outputMode("append") \
  .option("checkpointLocation",config['checkpoint_path']+"/gold") \
  .table(config['gold_table'])

# COMMAND ----------


