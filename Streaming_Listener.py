# Databricks notebook source
from pyspark.sql.streaming import StreamingQueryListener
import pyspark
from pyspark.sql import SparkSession
import json

# Define my listener.
class MyListener(StreamingQueryListener):  

    logs = "listener_logs"

    def __init__(self, base_dir):
       self.base_dir = base_dir

    def onQueryStarted(self, event):
       print("stream got started!")
       
    def onQueryProgress(self, event):
        progress = event.progress.json
        query_id = str(event.progress.id)
        batch_id = int(event.progress.batchId)
        run_id = int(event.progress.runId)
        file_name = f"{self.base_dir}/{self.logs}/{query_id}_{run_id}_{batch_id}.json"

        f = open(file_name, "w")
        f.write(progress)
        f.close()
        print(f"Write file {file_name} completed.")
        
    def onQueryTerminated(self, event):
        print(f"{event.id} got terminated!")
    
    def onQueryIdle(self, event):
      print("Query is idle")
