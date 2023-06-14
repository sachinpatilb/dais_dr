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

if(site == 'primary' or site == 'primary2') :
  _ = spark \
    .readStream \
    .table(config['bronze_table']) \
    .withColumn("event_time",to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
    .withColumn("diffOrig", col("newBalanceOrig")-col("oldBalanceOrig")) \
    .withColumn("diffDest", col("newBalanceDest")-col("oldBalanceDest")) \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("id").isNotNull()) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation",config['checkpoint_path']+"/silver") \
    .queryName(config['silver_stream'])\
    .table(config['silver_table'])
else :
  starting_offset = spark.read.table("offset_tracker").agg(max(col("secondary_bronze_version"))).collect()[0][0]
  print(starting_offset)
  _ = spark \
    .readStream \
    .option("startingVersion", starting_offset) \
    .table(config['bronze_table']) \
    .withColumn("event_time",to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
    .withColumn("diffOrig", col("newBalanceOrig")-col("oldBalanceOrig")) \
    .withColumn("diffDest", col("newBalanceDest")-col("oldBalanceDest")) \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("id").isNotNull()) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation",config['checkpoint_path']+"/silver") \
    .queryName(config['silver_stream'])\
    .table(config['silver_table'])
