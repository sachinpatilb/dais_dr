# Databricks notebook source
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

config

# COMMAND ----------

import time
import os

# Set up source and destination folder paths
raw_dir = config['raw_path']
src_dir = config['src_path']
# src_dir = config['source_path']

# Get list of files in source folder, ordered by name
files = dbutils.fs.ls(raw_dir)
files = sorted(files, key=lambda x: x.name)

if(site == 'primary' or site == 'primary2') :
  # Copy each file to destination folder, with sleep interval
  for file in files:
    raw_path = file.path
    src_path = os.path.join(src_dir, file.name)
    dbutils.fs.cp(raw_path, src_path)
    print(f"Copying {raw_path} to {src_path}")
    time.sleep(1) # sleep for 1 second between copy operations
else:
  # set current datebase context
  _ = spark.catalog.setCurrentDatabase(config['db'])
  from pyspark.sql.functions import *
  starting_file = spark.read.table("offset_tracker").agg(max(col("ingestion_file"))).collect()[0][0]
  
  # Copy each file to destination folder, with sleep interval
  for file in files:
    raw_path = file.path
    if file.name > starting_file:
      src_path = os.path.join(src_dir, file.name)
      dbutils.fs.cp(raw_path, src_path)
      print(f"Copying {raw_path} to {src_path}")
      time.sleep(1) # sleep for 1 second between copy operations
