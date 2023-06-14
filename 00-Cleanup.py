# Databricks notebook source
# MAGIC %run "./0-config"

# COMMAND ----------

# Define widget
dbutils.widgets.dropdown("site", "primary", ["primary","secondary","primary2","secondary2"],"Choose Primary or Secondary site")
dbutils.widgets.dropdown("mode", "all", ["all","db_only","checkpoint_only"],"Choose cleanup mode")

# COMMAND ----------

# Get the value of site
site=dbutils.widgets.get("site")
mode=dbutils.widgets.get("mode")

# COMMAND ----------

config = get_configs(site,{})

# COMMAND ----------

if(mode == "all" or mode == "db_only") :
  # reset the database
  _ = spark.sql(f"DROP DATABASE IF EXISTS {config['db']} CASCADE")
  dbutils.fs.rm(config['db_path'], recurse=True)

# COMMAND ----------

if(mode == "all" or mode == "checkpoint_only") :
  # reset any checkpoint files in existance
  dbutils.fs.rm(config['checkpoint_path'], recurse=True)
  dbutils.fs.rm(config['src_path'], recurse=True)
