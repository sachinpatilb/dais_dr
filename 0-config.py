# Databricks notebook source
# MAGIC %md
# MAGIC This code is for setting up the configuration required for disaster recovery demo

# COMMAND ----------

# DBTITLE 1,Instantiate Config Variable
if 'primary_config' not in locals().keys():
  primary_config = {}

# COMMAND ----------

# DBTITLE 1,Storage file paths
# mount point name
primary_config['mount_point'] ='/Users/sachin.patil@databricks.com/dais'
primary_config['db_path'] = primary_config['mount_point']+'/database'

# database name
primary_config['database'] = 'primary_db_dais'

# file paths
primary_config['source_path'] = primary_config['mount_point'] + '/src'
primary_config['checkpoint_path'] = primary_config['mount_point'] + '/checkpoint'
primary_config['schema_path'] = primary_config['mount_point'] + '/schema'
primary_config['raw_path'] = primary_config['mount_point'] + '/raw'

primary_config['bronze_table'] = "bronze_txn"
primary_config['silver_table'] = "silver_txn"
primary_config['gold_table_a'] = "gold_txn_live"
primary_config['gold_table_b'] = "gold_txn_window"

primary_config['bronze_stream'] = "bronze_stream"
primary_config['silver_stream'] = "silver_stream"
primary_config['gold_stream_a'] = "gold_stream_a"
primary_config['gold_stream_b'] = "gold_stream_b"

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
