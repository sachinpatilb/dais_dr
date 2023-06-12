# Databricks notebook source
# MAGIC %run "./0-config"

# COMMAND ----------

# MAGIC %md
# MAGIC This code is for setting up the configuration required for disaster recovery demo

# COMMAND ----------

# DBTITLE 1,Database
# create database if not exists
_ = spark.sql("create database if not exists {0} location '{1}'".format(primary_config['database'],primary_config['db_path']))

# set current datebase context
_ = spark.catalog.setCurrentDatabase(primary_config['database'])

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists gold_txn_live (
# MAGIC   customer_id string,
# MAGIC   event_hour string,
# MAGIC   no_of_txn long
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE if not exists silver_txn (
# MAGIC   amount STRING,
# MAGIC   countryDest STRING,
# MAGIC   countryOrig STRING,
# MAGIC   customer_id STRING,
# MAGIC   id STRING,
# MAGIC   isUnauthorizedOverdraft STRING,
# MAGIC   nameDest STRING,
# MAGIC   nameOrig STRING,
# MAGIC   newBalanceDest STRING,
# MAGIC   newBalanceOrig STRING,
# MAGIC   oldBalanceDest STRING,
# MAGIC   oldBalanceOrig STRING,
# MAGIC   step STRING,
# MAGIC   type STRING,
# MAGIC   event_time TIMESTAMP,
# MAGIC   _rescued_data STRING,
# MAGIC   diffOrig DOUBLE,
# MAGIC   diffDest DOUBLE
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE if not exists bronze_txn (
# MAGIC   amount STRING,
# MAGIC   countryDest STRING,
# MAGIC   countryOrig STRING,
# MAGIC   customer_id STRING,
# MAGIC   id STRING,
# MAGIC   isUnauthorizedOverdraft STRING,
# MAGIC   nameDest STRING,
# MAGIC   nameOrig STRING,
# MAGIC   newBalanceDest STRING,
# MAGIC   newBalanceOrig STRING,
# MAGIC   oldBalanceDest STRING,
# MAGIC   oldBalanceOrig STRING,
# MAGIC   step STRING,
# MAGIC   type STRING,
# MAGIC   event_time STRING,
# MAGIC   _rescued_data STRING
# MAGIC );

# COMMAND ----------

# MAGIC %md Setting up separate configs for secondary environment

# COMMAND ----------

if 'secondary_config' not in locals().keys():
  secondary_config = {}

# mount point name
secondary_config['mount_point'] ='/Users/sachin.patil@databricks.com/dais/secondary'
secondary_config['db_path'] = secondary_config['mount_point']+'/database'

# database name
secondary_config['database'] = 'secondary_db_dais'

# file paths
secondary_config['source_path'] = secondary_config['mount_point'] + '/src'
secondary_config['checkpoint_path'] = secondary_config['mount_point'] + '/checkpoint'
secondary_config['schema_path'] = secondary_config['mount_point'] + '/schema'
secondary_config['raw_path'] = secondary_config['mount_point'] + '/raw'

secondary_config['bronze_table'] = "bronze_txn"
secondary_config['silver_table'] = "silver_txn"
secondary_config['gold_table_a'] = "gold_txn_live"
secondary_config['gold_table_b'] = "gold_txn_window"

secondary_config['bronze_stream'] = "bronze_stream"
secondary_config['silver_stream'] = "silver_stream"
secondary_config['gold_stream_a'] = "gold_stream_a"
secondary_config['gold_stream_b'] = "gold_stream_b"

# COMMAND ----------

# # create database if not exists
# _ = spark.sql("create database if not exists {0} location '{1}'".format(secondary_config['database'],secondary_config['db_path']))

# # set current datebase context
# _ = spark.catalog.setCurrentDatabase(secondary_config['database'])

# COMMAND ----------


