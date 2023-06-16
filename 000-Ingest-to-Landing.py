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

from pyspark.sql.functions import *
starting_file = spark.read.table("offset_tracker").agg(max(col("ingestion_file"))).collect()[0][0]

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
    time.sleep(10) # sleep for 1 second between copy operations
else:
  # Copy each file to destination folder, with sleep interval
  for file in files:
    raw_path = file.path
    if file.name > starting_file:
      src_path = os.path.join(src_dir, file.name)
      dbutils.fs.cp(raw_path, src_path)
      time.sleep(10) # sleep for 1 second between copy operations
