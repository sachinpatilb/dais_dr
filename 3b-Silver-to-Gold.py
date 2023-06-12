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
dbutils.widgets.dropdown("site", "primary", ["primary","secondary"],"Choose Primary or Secondary site")

# COMMAND ----------

# Get the value of site
site=dbutils.widgets.get("site")

# COMMAND ----------

if site =="primary":
  src_path=primary_config['source_path']
  checkpoint_path=primary_config['checkpoint_path']
  schema_path=primary_config['schema_path']
  raw_path=primary_config['raw_path']
  db=primary_config['database']
  bronze_table=primary_config['bronze_table']
  silver_table=primary_config['silver_table']
  gold_table_a=primary_config['gold_table_a']
  gold_table_b=primary_config['gold_table_b']
  bronze_stream=primary_config['bronze_stream']
  silver_stream=primary_config['silver_stream']
  gold_stream_a=primary_config['gold_stream_a']
  gold_stream_b=primary_config['gold_stream_b']
else:
  src_path=secondary_config['source_path']
  checkpoint_path=secondary_config['checkpoint_path']
  schema_path=secondary_config['schema_path']
  raw_path=secondary_config['raw_path']
  db=secondary_config['database']
  bronze_table=secondary_config['bronze_table']
  silver_table=secondary_config['silver_table']
  gold_table_a=secondary_config['gold_table_a']
  gold_table_b=secondary_config['gold_table_b']
  bronze_stream=primary_config['bronze_stream']
  silver_stream=primary_config['silver_stream']
  gold_stream_a=primary_config['gold_stream_a']
  gold_stream_b=primary_config['gold_stream_b']

# COMMAND ----------

# set current datebase context
_ = spark.catalog.setCurrentDatabase(db)

# COMMAND ----------

from pyspark.sql.functions import *

gold_txn_df = spark.readStream \
  .table(silver_table) \
  .withWatermark("event_time", "60 minutes") \
  .groupBy(window("event_time", "60 minutes"), "customer_id") \
  .agg(expr("count(customer_id) as no_of_txn")) \
  .selectExpr("window.start as start_time", "customer_id", "no_of_txn") \
  .writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation",checkpoint_path+"/gold") \
  .table(gold_stream_b)

# COMMAND ----------


