# Databricks notebook source
# MAGIC %md ##Introduction
# MAGIC
# MAGIC In this notebook, we will ingest the raw data from source files into bronze delta table

# COMMAND ----------

# MAGIC %run "./0-config"

# COMMAND ----------

# Read the input CSV file using AutoLoader
_=(spark.readStream.format("cloudFiles")
            .option("cloudFiles.format","csv")
            .option("cloudFiles.inferSchema", "true")
            .option("cloudFiles.schemaLocation", primary_config['schema_path'])
            .option("cloudFiles.maxFilesPerTrigger", "1")
            .option("header",True)
            .load(primary_config['source_path'])
            .writeStream
            .format("delta")
            .option("checkpointLocation",primary_config['checkpoint_path']+"/bronze")
            .queryName(primary_config['bronze_stream'])
            .table("bronze_txn")
)

# COMMAND ----------


