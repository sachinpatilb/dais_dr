# Databricks notebook source
# MAGIC %run "./0-config"

# COMMAND ----------

# reset the database
_ = spark.sql(f"DROP DATABASE IF EXISTS {config['database']} CASCADE")

# COMMAND ----------

# reset any checkpoint files in existance
dbutils.fs.rm(config['checkpoint_path'], recurse=True)