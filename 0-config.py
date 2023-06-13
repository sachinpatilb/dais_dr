# Databricks notebook source
# MAGIC %md
# MAGIC This code is for setting up the configuration required for disaster recovery demo

# COMMAND ----------

# DBTITLE 1,Instantiate Config Variable
if 'primary_config' not in locals().keys():
  primary_config = {}

# COMMAND ----------

# DBTITLE 1,Storage file paths
# Primary Site config with approach 1

# mount point name
primary_config['mount_point'] ='/Users/sachin.patil@databricks.com/dais/primary1'
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

# Primary Site config with approach 2

if 'primary2_config' not in locals().keys():
  primary2_config = {}

# mount point name
primary2_config['mount_point'] ='/Users/sachin.patil@databricks.com/dais/primary2'
primary2_config['db_path'] = primary2_config['mount_point']+'/database'

# database name
primary2_config['database'] = 'primary2_db_dais'

# file paths
primary2_config['source_path'] = primary2_config['mount_point'] + '/src'
primary2_config['checkpoint_path'] = primary2_config['mount_point'] + '/checkpoint'
primary2_config['schema_path'] = primary2_config['mount_point'] + '/schema'
primary2_config['raw_path'] = primary2_config['mount_point'] + '/raw'

primary2_config['bronze_table'] = "bronze_txn"
primary2_config['silver_table'] = "silver_txn"
primary2_config['gold_table_a'] = "gold_txn_live"
primary2_config['gold_table_b'] = "gold_txn_window"

primary2_config['bronze_stream'] = "bronze_stream"
primary2_config['silver_stream'] = "silver_stream"
primary2_config['gold_stream_a'] = "gold_stream_a"
primary2_config['gold_stream_b'] = "gold_stream_b"

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

if 'secondary2_config' not in locals().keys():
  secondary2_config = {}

# mount point name
secondary2_config['mount_point'] ='/Users/sachin.patil@databricks.com/dais/secondary2'
secondary2_config['db_path'] = secondary2_config['mount_point']+'/database'

# database name
secondary2_config['database'] = 'secondary2_db_dais'

# file paths
secondary2_config['source_path'] = secondary2_config['mount_point'] + '/src'
secondary2_config['checkpoint_path'] = secondary2_config['mount_point'] + '/checkpoint'
secondary2_config['schema_path'] = secondary2_config['mount_point'] + '/schema'
secondary2_config['raw_path'] = secondary2_config['mount_point'] + '/raw'

secondary2_config['bronze_table'] = "bronze_txn"
secondary2_config['silver_table'] = "silver_txn"
secondary2_config['gold_table_a'] = "gold_txn_live"
secondary2_config['gold_table_b'] = "gold_txn_window"

secondary2_config['bronze_stream'] = "bronze_stream"
secondary2_config['silver_stream'] = "silver_stream"
secondary2_config['gold_stream_a'] = "gold_stream_a"
secondary2_config['gold_stream_b'] = "gold_stream_b"

# COMMAND ----------

# # create database if not exists
# _ = spark.sql("create database if not exists {0} location '{1}'".format(secondary_config['database'],secondary_config['db_path']))

# # set current datebase context
# _ = spark.catalog.setCurrentDatabase(secondary_config['database'])
