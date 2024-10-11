# Databricks notebook source
service_credential = dbutils.secrets.get(scope="cicddatabricksconnect",key="databrickskeysecret")

spark.conf.set("fs.azure.account.auth.type.adfcicddatalakedev.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adfcicddatalakedev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adfcicddatalakedev.dfs.core.windows.net", "f87ce217-ba29-4582-bd8f-e0daa4541381")
spark.conf.set("fs.azure.account.oauth2.client.secret.adfcicddatalakedev.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adfcicddatalakedev.dfs.core.windows.net", "https://login.microsoftonline.com/a3881e6f-f92f-449c-b7e6-e9df56edaf30/oauth2/token")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Example of reading a file from ADLS Gen2
df = spark.read.format("json").load("abfss://bronze@adfcicddatalakedev.dfs.core.windows.net/RealtimeTrainsStatus/RealTimeStatus.json")

# Explode the "TrainPositions" column
df = df.selectExpr("explode(TrainPositions) as TrainPositions")

schema = StructType([
    StructField("CarCount", IntegerType(), True),
    StructField("CircuitId", IntegerType(), True),
    StructField("DestinationStationCode", StringType(), True),
    StructField("DirectionNum", IntegerType(), True),
    StructField("LineCode", StringType(), True),
    StructField("SecondsAtLocation", IntegerType(), True),
    StructField("ServiceType", StringType(), True),
    StructField("TrainId", StringType(), True),
    StructField("TrainNumber", StringType(), True)
])
df_processed = df.select(col("TrainPositions.CarCount"), col("TrainPositions.CircuitId"), col("TrainPositions.DestinationStationCode"), col("TrainPositions.DirectionNum"), col("TrainPositions.LineCode"), col("TrainPositions.SecondsAtLocation"), col("TrainPositions.ServiceType"), col("TrainPositions.TrainId"), col("TrainPositions.TrainNumber"))
display(df_processed)

df_processed.write.mode("append").option("mergeSchema", "true").format("delta").save("abfss://silver@adfcicddatalakedev.dfs.core.windows.net/Processed_Data/realtimetrains")

# COMMAND ----------

display(spark.read.format("delta").load("abfss://silver@adfcicddatalakedev.dfs.core.windows.net/Processed_Data/realtimetrains"))
