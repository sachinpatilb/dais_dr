# Databricks notebook source
# MAGIC %run "./0-config"

# COMMAND ----------

# Define widget
dbutils.widgets.dropdown("site", "primary", ["primary","secondary","primary2","secondary2"],"Choose Primary or Secondary site")

# COMMAND ----------

# Get the value of site
site=dbutils.widgets.get("site")

# COMMAND ----------

config = get_configs(site,{})

# COMMAND ----------

from pyspark.sql.functions import  col, from_json, max
from pyspark.sql.types import *
import os

def get_offsets(stream_config):

  #find sink offsets
  if(stream_config['sink_type'] == 'delta') :
    stream_id = spark.read.json(f"{stream_config['checkpoint_path']}/metadata").collect()[0][0]
    sink_version = (spark.sql(f"""describe history delta.`{stream_config['sink_path']}`""")
                    .where(f"""operationParameters.queryId == '{stream_id}' """)
                    .agg(max(col("version")))).collect()[0][0]
  elif(stream_config['sink_type'] == 'foreachbatch') :
    userMetadataSchema =  schema = StructType([ \
      StructField("stream",StringType(),True), \
      StructField("batch_id",IntegerType(),True), \
      StructField("app_id",StringType(),True) 
    ])
    sink_version = (spark.sql(f"""describe history delta.`{stream_config['sink_path']}`""")
          .withColumn("userMetadataJson", from_json("userMetadata",userMetadataSchema))
          .orderBy(col("userMetadataJson.batch_id").desc()).select("version").first()[0])
  file_name = f"""{stream_config['sink_path']}/_delta_log/{str(sink_version).zfill(20)}.json"""
  latest_batch_id = spark.read.json(file_name).select("txn.*").dropna().select("version").collect()[0][0]
  
  #find source offsets
  if(stream_config['source_type'] == 'delta'):
    df = (spark.read.text(f"""{stream_config['checkpoint_path']}/offsets/{latest_batch_id}""")
      .where("value like '%sourceVersion%'")
      .selectExpr("from_json(value,'sourceVersion INT, reservoirId STRING,isStartingVersion BOOLEAN,reservoirVersion INT,index INT') as json"))
    source_version = df.selectExpr("json.reservoirVersion + json.index as source_index").collect()[0][0]
  elif(stream_config['source_type'] == 'auto_loader'):
    df = spark.sql(f"""SELECT path FROM cloud_files_state('{stream_config['checkpoint_path']}') order by commit_time desc limit 1""")
    source_version = os.path.basename(df.collect()[0][0])
  return {'source': source_version, 'sink': sink_version, 'stream_name': stream_config['stream_name']}

# COMMAND ----------

# DBTITLE 1,Find mapping offsets for Raw to Bronze stream
bronze_stream_config = {'source_type':'auto_loader',
                        'sink_type':'delta',
                        'source_path':config['src_path'],
                        'checkpoint_path':f"{config['checkpoint_path']}/bronze",
                        'stream_name': config['bronze_stream'],
                        'sink_path':f"{config['db_path']}/{config['bronze_table']}"}              
bronze_offsets = get_offsets(bronze_stream_config)
bronze_offsets

# COMMAND ----------

# DBTITLE 1,Find mapping offsets Backup Bronze to Silver stream
silver_stream_config = {'source_type':'delta',
                        'sink_type':'delta',
                        'source_path':f"{config['db_path']}/{config['bronze_table']}",
                        'checkpoint_path':f"{config['checkpoint_path']}/silver",
                        'stream_name': config['silver_stream'],
                        'sink_path':f"{config['db_path']}/{config['silver_table']}"}
silver_offsets = get_offsets(silver_stream_config)
silver_offsets

# COMMAND ----------

# DBTITLE 1,Find mapping offsets for Silver to Gold stream
gold_stream_config = {'source_type':'delta',
                      'sink_type':'foreachbatch',
                      'source_path':f"{config['db_path']}/{config['silver_table']}",
                      'stream_name': config['gold_stream'],
                      'checkpoint_path':f"{config['checkpoint_path']}/gold",
                      'sink_path':f"{config['db_path']}/{config['gold_table']}"}  
gold_offsets = get_offsets(gold_stream_config)
gold_offsets

# COMMAND ----------

def validate_offsets (bronze_offsets , silver_offsets, gold_offsets):
  return silver_offsets['sink'] == gold_offsets['source'] and bronze_offsets['sink'] == silver_offsets['source']

if (site == "primary"):
  sec_config = get_configs("secondary",{})
  sec_env = "secondary"
elif(site == "primary2"):
  sec_config = get_configs("secondary2",{})
  sec_env = "secondary2"

#instantiate secondary database 
dbutils.notebook.run("./0-database",60,{"site": sec_env})

# COMMAND ----------

from delta.tables import *

def clone_table(src_path, dest_path, version, emptyCommit=True):
  table = DeltaTable.forPath(spark, src_path)
  table.cloneAtVersion(version,dest_path, replace=True)
  if(emptyCommit) :
    table.cloneAtVersion(version,dest_path, replace=True) # Empty commit workaround for now

# COMMAND ----------

# DBTITLE 1,Start cloning
if(validate_offsets(bronze_offsets, silver_offsets, gold_offsets)) :
  #cloning bronze
  clone_table(f"{config['db_path']}/{config['bronze_table']}",f"{sec_config['db_path']}/{sec_config['bronze_table']}",silver_offsets['source'])
  
  #cloning silver
  clone_table(f"{config['db_path']}/{config['silver_table']}", f"{sec_config['db_path']}/{sec_config['silver_table']}", silver_offsets['sink'])
  
  #cloning gold
  clone_table(f"{config['db_path']}/{config['gold_table']}", f"{sec_config['db_path']}/{sec_config['gold_table']}", gold_offsets['sink'])

# COMMAND ----------

offsets = {}

offsets['ingestion_file'] = bronze_offsets['source'] #needs to be replaced with bronze source

offsets['primary_bronze_version'] = silver_offsets['source']
offsets['primary_silver_version'] = silver_offsets['sink']
offsets['primary_gold_version'] = gold_offsets['sink']

offsets['secondary_bronze_version'] = DeltaTable.forPath(spark, f"{sec_config['db_path']}/{sec_config['bronze_table']}").history().agg(max(col("version"))).collect()[0][0]
offsets['secondary_silver_version'] = DeltaTable.forPath(spark, f"{sec_config['db_path']}/{sec_config['silver_table']}").history().agg(max(col("version"))).collect()[0][0]
offsets['secondary_gold_version'] = DeltaTable.forPath(spark, f"{sec_config['db_path']}/{sec_config['gold_table']}").history().agg(max(col("version"))).collect()[0][0]

df = spark.createDataFrame([offsets])
df.display()

# COMMAND ----------

# DBTITLE 1,Write to primary db
df.write.mode("append").saveAsTable(f"{config['db']}.offset_tracker")

# COMMAND ----------

# DBTITLE 1,Write to secondary db
df.write.mode("append").saveAsTable(f"{sec_config['db']}.offset_tracker")
