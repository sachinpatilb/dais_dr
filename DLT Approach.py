# Databricks notebook source
import dlt
from pyspark.sql.functions import col, to_timestamp, window, count

@dlt.table
def bronze_txn():
  return (
    spark.read.format("csv")
      .option("header",True)
      .option("inferSchema",True)
      .load(spark.conf.get("raw_path"))
  )

# COMMAND ----------

@dlt.table
def silver_txn():
  return (
    dlt.read("bronze_txn")
    .withColumn("event_time", to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    .withColumn("diffOrig", col("newBalanceOrig")-col("oldbalanceOrig")) 
    .withColumn("diffDest", col("newBalanceDest")-col("oldBalanceDest")) 
    .filter(col("customer_id").isNotNull()) 
    .filter(col("id").isNotNull()) 
    )

# COMMAND ----------

@dlt.table
def gold_txn():
  return (
    dlt.read("silver_txn")
    .withWatermark("event_time", "60 minutes") 
    .groupBy(window("event_time", "60 minutes"), "customer_id") 
    .agg(count("customer_id").alias("no_of_txn")) 
    .selectExpr("window.start as start_time", "customer_id", "no_of_txn")
  )
