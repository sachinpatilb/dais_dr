# Databricks notebook source
# MAGIC %run "./0-config"

# COMMAND ----------

import time
import os

# Set up source and destination folder paths
raw_dir = config['raw_path']
src_dir = config['source_path']
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

