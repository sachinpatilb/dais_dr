# Databricks notebook source
# MAGIC %md ##Introduction
# MAGIC
# MAGIC In this notebook, we will ingest the raw data from source files into bronze delta table

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

# To enable detailed tracking of the files, recordEventChanges is available in DBR12.1.
spark.conf.set("spark.databricks.cloudFiles.recordEventChanges", True)
spark.conf.set("spark.databricks.cloudFiles.optimizedEventSerialization.enabled", True)

# COMMAND ----------

# Read the input CSV file using AutoLoader
_=(spark.readStream.format("cloudFiles")
            .option("cloudFiles.format","csv")
            .option("cloudFiles.inferSchema", "true")
            .option("cloudFiles.schemaLocation", config['schema_path'])
            .option("cloudFiles.maxFilesPerTrigger", "1")
            .option("header",True)
            .load(config['src_path'])
            .writeStream
            .format("delta")
            .option("checkpointLocation",config['checkpoint_path']+"/bronze")
            .queryName(config['bronze_stream'])
            .table(config['bronze_table'])
)
