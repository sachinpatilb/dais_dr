# Databricks notebook source
# MAGIC %run "./0-config"

# COMMAND ----------

# Define widget
dbutils.widgets.dropdown("site", "primary", ["primary","secondary","primary2","secondary2"],"Choose Primary or Secondary site")

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
  db_path=primary_config['db_path']
elif site =="primary2":
  src_path=primary2_config['source_path']
  checkpoint_path=primary2_config['checkpoint_path']
  schema_path=primary2_config['schema_path']
  raw_path=primary2_config['raw_path']
  db=primary2_config['database']
  bronze_table=primary2_config['bronze_table']
  silver_table=primary2_config['silver_table']
  gold_table_a=primary2_config['gold_table_a']
  gold_table_b=primary2_config['gold_table_b']
  bronze_stream=primary2_config['bronze_stream']
  silver_stream=primary2_config['silver_stream']
  gold_stream_a=primary2_config['gold_stream_a']
  gold_stream_b=primary2_config['gold_stream_b']
  db_path=primary2_config['db_path']
elif site =="secondary":
  src_path=secondary_config['source_path']
  checkpoint_path=secondary_config['checkpoint_path']
  schema_path=secondary_config['schema_path']
  raw_path=secondary_config['raw_path']
  db=secondary_config['database']
  bronze_table=secondary_config['bronze_table']
  silver_table=secondary_config['silver_table']
  gold_table_a=secondary_config['gold_table_a']
  gold_table_b=secondary_config['gold_table_b']
  bronze_stream=secondary_config['bronze_stream']
  silver_stream=secondary_config['silver_stream']
  gold_stream_a=secondary_config['gold_stream_a']
  gold_stream_b=secondary_config['gold_stream_b']
  db_path=secondary_config['db_path']
else:
  src_path=secondary2_config['source_path']
  checkpoint_path=secondary2_config['checkpoint_path']
  schema_path=secondary2_config['schema_path']
  raw_path=secondary2_config['raw_path']
  db=secondary2_config['database']
  bronze_table=secondary2_config['bronze_table']
  silver_table=secondary2_config['silver_table']
  gold_table_a=secondary2_config['gold_table_a']
  gold_table_b=secondary2_config['gold_table_b']
  bronze_stream=secondary2_config['bronze_stream']
  silver_stream=secondary2_config['silver_stream']
  gold_stream_a=secondary2_config['gold_stream_a']
  gold_stream_b=secondary2_config['gold_stream_b']
  db_path=secondary2_config['db_path']

# COMMAND ----------

import time
import os

# Set up source and destination folder paths
raw_dir = raw_path
src_dir = src_path
# src_dir = config['source_path']

# Get list of files in source folder, ordered by name
files = dbutils.fs.ls(raw_dir)
files = sorted(files, key=lambda x: x.name)

# Copy each file to destination folder, with sleep interval
for file in files:
  raw_path = file.path
  src_path = os.path.join(src_dir, file.name)
  
  dbutils.fs.cp(raw_path, src_path)
  time.sleep(10) # sleep for 1 second between copy operations

# COMMAND ----------


