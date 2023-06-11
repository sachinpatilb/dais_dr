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
event_dates= (
            df_with_event_ts
            .select(date_format(col("event_time"),"yyyy-MM-dd").alias("dt_col"))
            .distinct()
            .orderBy( to_date(to_timestamp('event_time')))  # sorting of list is essential for logic below
          ).collect()

# COMMAND ----------

for ev_dt in event_dates:
  dt=ev_dt['dt_col']
  filtered_df = df_with_event_ts.filter(date_format(col("event_time"),"yyyy-MM-dd").alias("dt_col") == dt)
  output_file_path = f"/Users/sachin.patil@databricks.com/dais/dataset2/{dt}.csv"
  filtered_df.write.format("csv").option("header", "true").option("delimiter", ",").mode("overwrite").save(output_file_path)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Users/sachin.patil@databricks.com/dais/dataset2/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/Users/sachin.patil@databricks.com/dais/dataset2/2022-12-31.csv/
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC head dbfs:/Users/sachin.patil@databricks.com/dais/dataset2/2022-12-31.csv/part-00000-tid-8014999583909587391-32cd5247-b193-4187-b223-bdca5c04e8e1-397-1-c000.csv
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema dais_dr

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema dais_dr

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %fs
# MAGIC cp -r /dbdemos/fsi/fraud-detection/transactions /Users/sachin.patil@databricks.com/dais/dataset/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Users/sachin.patil@databricks.com/dais/dataset/

# COMMAND ----------

raw_data_location='/Users/sachin.patil@databricks.com/dais/dataset'
check_point_location='/Users/sachin.patil@databricks.com/dais/checkpoint'
schema_location='/Users/sachin.patil@databricks.com/dais/schema'

# COMMAND ----------

from pyspark.sql.functions import date_add, hour
from datetime import datetime
import random

timestamp = datetime(2023, 1, 1, 0, 0, 0)
df=spark.read.json(raw_data_location)


# COMMAND ----------

from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import date_add
from pyspark.sql.functions import lit

df_with_timestamp = df.withColumn("event_time_base", lit(timestamp))

# df_with_timestamp = df.withColumn("constant_timestamp", lit("2023-01-01 00:00:00"))

# Assuming your DataFrame has a "timestamp" column with datetime values
# df2 = df.withColumn("event_time", to_timestamp("timestamp"))

# COMMAND ----------

display(df_with_timestamp)

# COMMAND ----------

import pyspark.sql.functions as F
df2=df_with_timestamp.withColumn("event_time",(F.unix_timestamp(F.col("event_time_base").cast("timestamp")) + F.col("step")*60*60-60*60*F.rand()).cast('timestamp'))
# df2=df_with_timestamp.withColumn("event_time",date_add("event_time_base",2).cast("timestamp"))

# COMMAND ----------

display(df2)

# COMMAND ----------

display(max(df2.step))

# COMMAND ----------

display(spark.read.json(raw_data_location))

# COMMAND ----------

df=spark.read.json(raw_data_location)

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_timestamp()

# COMMAND ----------

from datetime import timedelta
from datetime import datetime
import random

timestamp = datetime(2023, 1, 1, 0, 0, 0)

start_time = datetime.now()
new_timestamp = start_time + timedelta(hours=3)-timedelta(minutes=random.randint(1, 60))
print(new_timestamp)

# random_number = random.randint(1, 60)
# print(random_number)

# COMMAND ----------

from datetime import timedelta
from datetime import datetime
from pyspark.sql.functions import ti

import random

timestamp = datetime(2023, 1, 1, 0, 0, 0)

start_time = datetime.now()
df2 = df.withColumn("event_time",start_time + timedelta(hours=df.step)-timedelta(minutes=random.randint(1, 60)))
show(df2)

# COMMAND ----------

_= (spark.readStream
          .format("cloudFiles")
                  .option("cloudFiles.format", "json")
                  .option("cloudFiles.maxFilesPerTrigger", "1")
                  .option("cloudFiles.inferColumnTypes", "true")
                  .option("cloudFiles.schemaLocation",schema_location)
                  .load(raw_data_location)
          .writeStream
                  .format("delta")
                  .option("checkpointLocation", f"""{check_point_location}/src_to_bronze""")
                  .table("bronze_transactions"))

# COMMAND ----------

