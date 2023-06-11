-- Databricks notebook source
-- MAGIC %sql
-- MAGIC select * from dais_dr.bronze_transactions

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(1) from dais_dr.bronze_transactions

-- COMMAND ----------

select * from dais_dr.silver_txn

-- COMMAND ----------

select * from dais_dr.gold_txn_window

-- COMMAND ----------

select * from dais_dr.gold_txn_live

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC create table dais_dr.gold_txn_live (customer_id string,
-- MAGIC event_hour string,
-- MAGIC no_of_txn long)

-- COMMAND ----------

select * from dais_dr.gold_txn_window

-- COMMAND ----------

select * from dais_dr.gold_txn_live

-- COMMAND ----------

select * from dais_dr.silver_txn

-- COMMAND ----------

select * from dais_dr.bronze_txn

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC df=spark \
-- MAGIC   .read \
-- MAGIC   .table("dais_dr.bronze_txn") \
-- MAGIC   .withColumn("event_time2",to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

describe detail dais_dr.bronze_txn

-- COMMAND ----------

