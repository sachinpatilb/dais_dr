# Databricks notebook source
# MAGIC %run "./0-config"

# COMMAND ----------

primary_config

# COMMAND ----------

secondary_config

# COMMAND ----------

from pyspark.sql.functions import col, max

def offset_fetcher(sink_table_path, checkpoint_path, stream_name, source) :
  stream_id = spark.read.json(f"{checkpoint_path}/metadata").collect()[0][0]
  sink_version = (spark.sql(f"""describe history delta.`{sink_table_path}`""")
                .where(f"""operationParameters.queryId == '{stream_id}' """)
                .agg(max(col("version")))).collect()[0][0]
  file_name = f"""{sink_table_path}/_delta_log/{str(sink_version).zfill(20)}.json"""

  latest_batch_id = spark.read.json(file_name).select("txn.*").dropna().select("version").collect()[0][0]
  df = (spark.read.text(f"""{checkpoint_path}/offsets/{latest_batch_id}""")
      .where("value like '%sourceVersion%'")
      .selectExpr("from_json(value,'sourceVersion INT, reservoirId STRING,isStartingVersion BOOLEAN,reservoirVersion INT,index INT') as json"))
  source_version = df.selectExpr("json.reservoirVersion + json.index as source_index").collect()[0][0]
  return {'primary_source_version': source_version, 'primary_sink_version': sink_version, 'stream_name': stream_name}

# COMMAND ----------

# DBTITLE 1,Find mapping offsets for Bronze to Silver stream
sink_table = spark.sql(f"describe detail {primary_config['database']}.{primary_config['silver_table']}").collect()[0]['location']
checkpoint_path = primary_config['checkpoint_path'] + '/silver'
stream_name = primary_config['bronze_stream']
offsets = offset_fetcher(sink_table, checkpoint_path, stream_name, source = "delta")

# COMMAND ----------

# DBTITLE 1,Find mapping offsets for Silver to Gold stream
sink_table = spark.sql(f"describe detail {primary_config['database']}.{primary_config['gold_table_a']}").collect()[0]['location']
checkpoint_path = primary_config['checkpoint_path'] + '/silver'
stream_name = primary_config['bronze_stream']
offsets = offset_fetcher(sink_table, checkpoint_path, stream_name, source = "delta")
