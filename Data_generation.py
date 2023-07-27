# Databricks notebook source
raw_data_location='/Users/sachin.patil@databricks.com/dais/dataset'
check_point_location='/Users/sachin.patil@databricks.com/dais/checkpoint'
schema_location='/Users/sachin.patil@databricks.com/dais/schema'

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import lit
import pyspark.sql.functions as F

timestamp = datetime(2023, 1, 1, 0, 0, 0)
df=spark.read.json(raw_data_location)
df_with_event_ts=df.withColumn("event_time",(F.unix_timestamp(lit(timestamp).cast("timestamp")) + F.col("step")*60*60-60*60*F.rand()).cast('timestamp'))

# COMMAND ----------

display(df_with_event_ts)

# COMMAND ----------

from pyspark.sql.functions import *
event_time= (
            df_with_event_ts
            .select(date_format(col("event_time"),"yyyy-MM-dd-HH").alias("dt_col"))
            .distinct()
            .orderBy(to_timestamp('event_time'))  # sorting of list is essential for logic below
          ).collect()

# COMMAND ----------

display(event_time)

# COMMAND ----------

for ev_dt in event_time:
  dt=ev_dt['dt_col']
  filtered_df = df_with_event_ts.filter(date_format(col("event_time"),"yyyy-MM-dd-HH").alias("dt_col") == dt)
  filtered_df=filtered_df.coalesce(1)
  output_file_path = f"/Users/sachin.patil@databricks.com/dais/dataset5/{dt}.csv"
  filtered_df.write.format("csv").option("header", "true").option("delimiter", ",").mode("overwrite").save(output_file_path)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Users/sachin.patil@databricks.com/dais/dataset5/2023-01-23-19.csv

# COMMAND ----------

# MAGIC %fs
# MAGIC head /Users/sachin.patil@databricks.com/dais/dataset5/2023-01-23-19.csv/part-00000-tid-1882547258119715324-ed16421a-bb66-4c68-9182-93f7ddc8c4b1-2338-1-c000.csv

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/Users/sachin.patil@databricks.com/dais/dataset2/2022-12-31.csv/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Users/sachin.patil@databricks.com/dais/src

# COMMAND ----------

import fnmatch
import os

dbfs_file_path = "/Users/sachin.patil@databricks.com/dais/dataset5"
local_dir_path = "/Users/sachin.patil@databricks.com/dais/src"

# Define a function to list all files recursively with a given extension
def list_files_recursively(root, extension):
  files = dbutils.fs.ls(root)
  result = []
  for file in files:
    if file.isDir():
      result.extend(list_files_recursively(file.path, extension))
    elif fnmatch.fnmatch(file.path, extension):
      result.append(file.path)
  return result

# Define the directory to search for CSV files recursively
directory = dbfs_file_path

# Call the function to get the list of CSV files
csv_files = list_files_recursively(directory, '*.csv')

# Print the list of CSV files
for csv_file in csv_files:
  file_name = os.path.basename(os.path.dirname(csv_file))
  # Construct the new file path with the local directory name and file name
  new_file_path = local_dir_path + "/" + file_name
  # Copy the file from DBFS to the local directory with the new file name
  dbutils.fs.cp(csv_file, new_file_path)


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Users/sachin.patil@databricks.com/dais/raw

# COMMAND ----------

# Assuming the directory name and file name are already stored in variables:
dbfs_file_path = "/Users/sachin.patil@databricks.com/dais/dataset2"
local_dir_path = "/Users/sachin.patil@databricks.com/dais/dataset3"

# Extract the file name and extension from the DBFS path
import os
file_name = os.path.basename(dbfs_file_path)
file_extension = os.path.splitext(dbfs_file_path)[1]

# Construct the new file path with the local directory name and file name
new_file_path = local_dir_path + "/" + os.path.basename(local_dir_path) + file_extension

# Copy the file from DBFS to the local directory with the new file name
dbutils.fs.cp(dbfs_file_path, new_file_path)

# COMMAND ----------

print(file_name)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Users/sachin.patil@databricks.com/dais/dataset3

# COMMAND ----------

# MAGIC %fs
# MAGIC head dbfs:/Users/sachin.patil@databricks.com/dais/dataset3/2022-12-31.csv

# COMMAND ----------

# MAGIC %fs
# MAGIC cp /Users/sachin.patil@databricks.com/dais/dataset3/2022-12-31.csv /Users/sachin.patil@databricks.com/dais/src/2022-12-31.csv

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Users/sachin.patil@databricks.com/dais/src

# COMMAND ----------

# MAGIC %fs
# MAGIC cp -r /Users/sachin.patil@databricks.com/dais/src /Users/sachin.patil@databricks.com/dais/src_backup

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Users/sachin.patil@databricks.com/dais/src_backup

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dais_dr.gold_txn_live

# COMMAND ----------

# MAGIC %fs
# MAGIC cp -r /Users/sachin.patil@databricks.com/dais/src_backup /Users/sachin.patil@databricks.com/dais/raw

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/Users/sachin.patil@databricks.com/dais/raw'
