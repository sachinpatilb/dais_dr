# Databricks notebook source
# MAGIC %md ##Introduction
# MAGIC
# MAGIC In this notebook, we will ingest the raw data from source files into bronze delta table

# COMMAND ----------

# MAGIC %run "./0-config"

# COMMAND ----------

# MAGIC %run "./0-database"

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

# Read the input CSV file using AutoLoader
_=(spark.readStream.format("cloudFiles")
            .option("cloudFiles.format","csv")
            .option("cloudFiles.inferSchema", "true")
            .option("cloudFiles.schemaLocation", schema_path)
            .option("cloudFiles.maxFilesPerTrigger", "1")
            .option("header",True)
            .load(src_path)
            .writeStream
            .format("delta")
            .option("checkpointLocation",checkpoint_path+"/bronze")
            .queryName(bronze_stream)
            .table(bronze_table)
)
