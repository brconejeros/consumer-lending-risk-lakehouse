# Databricks notebook source
CATALOG = "consumer_lending_risk_lakehouse"
TABLE = "previous_application"
LANDING_PATH = f"abfss://landing@streditorigination01.dfs.core.windows.net/{TABLE}/"

df = spark.read.parquet(LANDING_PATH)
(df.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(f"{CATALOG}.bronze.{TABLE}"))
