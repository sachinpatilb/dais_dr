# Databricks notebook source
# MAGIC %md
# MAGIC This code is for setting up the configuration required for disaster recovery demo

# COMMAND ----------

# DBTITLE 1,Instantiate Config Variable
if 'config' not in locals().keys():
  config = {}

# COMMAND ----------

# DBTITLE 1,Database
# database name
config['database'] = 'primary_db_dais'

# create database if not exists
_ = spark.sql('create database if not exists {0}'.format(config['database']))

# set current datebase context
_ = spark.catalog.setCurrentDatabase(config['database'])

# COMMAND ----------

# DBTITLE 1,Storage file paths
# mount point name
config['mount_point'] ='/Users/sachin.patil@databricks.com/dais/'

# file paths
config['source_path'] = config['mount_point'] + '/src'
config['checkpoint_path'] = config['mount_point'] + '/checkpoint'
config['schema_path'] = config['mount_point'] + '/schema'
config['raw_path'] = config['mount_point'] + '/raw'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists gold_txn_live (customer_id string,
# MAGIC event_hour string,
# MAGIC no_of_txn long)

# COMMAND ----------

