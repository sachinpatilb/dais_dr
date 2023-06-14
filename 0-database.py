# Databricks notebook source
# MAGIC %run "./0-config"

# COMMAND ----------

# MAGIC %md
# MAGIC This code is for setting up the configuration required for disaster recovery demo

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

# DBTITLE 1,Database
# create database if not exists
_ = spark.sql("create database if not exists {0} location '{1}'".format(db,db_path))

# set current datebase context
_ = spark.catalog.setCurrentDatabase(db)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists gold_txn_live (
# MAGIC   customer_id string,
# MAGIC   event_hour string,
# MAGIC   no_of_txn long
# MAGIC );
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
# MAGIC CREATE TABLE if not exists gold_txn_window (
# MAGIC   start_time TIMESTAMP,
# MAGIC   customer_id STRING,
# MAGIC   no_of_txn BIGINT
# MAGIC );
# MAGIC CREATE TABLE spark_catalog.primary_db_dais.offset_tracker (
# MAGIC   ingestion_file STRING,
# MAGIC   primary_bronze_version BIGINT,
# MAGIC   primary_gold_version BIGINT,
# MAGIC   primary_silver_version BIGINT,
# MAGIC   secondary_bronze_version BIGINT,
# MAGIC   secondary_gold_version BIGINT,
# MAGIC   secondary_silver_version BIGINT
# MAGIC );

# COMMAND ----------


