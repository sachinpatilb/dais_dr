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
            .option("cloudFiles.schemaLocation", config['schema_path'])
            .option("cloudFiles.maxFilesPerTrigger", "1")
            .option("header",True)
            .load(config['source_path'])
            .writeStream
            .format("delta")
            .option("checkpointLocation",config['checkpoint_path']+"/bronze")
            .table("bronze_txn")
)

# COMMAND ----------

