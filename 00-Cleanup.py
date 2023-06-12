# Databricks notebook source
# MAGIC %run "./0-config"

# COMMAND ----------

# reset the database
_ = spark.sql(f"DROP DATABASE IF EXISTS {primary_config['database']} CASCADE")
_ = spark.sql(f"DROP DATABASE IF EXISTS {secondary_config['database']} CASCADE")

# COMMAND ----------

# reset any checkpoint files in existance
dbutils.fs.rm(primary_config['checkpoint_path'], recurse=True)
dbutils.fs.rm(primary_config['source_path'], recurse=True)
dbutils.fs.rm(primary_config['db_path'], recurse=True)

# COMMAND ----------

dbutils.fs.rm(secondary_config['checkpoint_path'], recurse=True)
dbutils.fs.rm(secondary_config['source_path'], recurse=True)
dbutils.fs.rm(secondary_config['db_path'], recurse=True)

# COMMAND ----------


