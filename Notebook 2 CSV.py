# Databricks notebook source
df= spark.read.format("csv").option("header", "true").load("/dbfs/FileStore/shopping_trends.csv")

# COMMAND ----------


