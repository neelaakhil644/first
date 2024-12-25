# Databricks notebook source
spark.read.xls("dbfs:/FileStore/nba.xlsx")


# COMMAND ----------

df= spark.read.format('com.crealytics.spark.excel').option('useHeader', 'true').load('dbfs:/FileStore/nba.xlsx')

# COMMAND ----------

%pip install --upgrade spark-excel

df = (spark.read.format('com.crealytics.spark.excel')
      .option('useHeader', 'true')
      .load('/dbfs/FileStore/nba.xlsx'))
display(df)

# COMMAND ----------

%pip install --upgrade com.crealytics:spark-excel_2.12:0.13.5

df = (spark.read.format("com.crealytics.spark.excel")
      .option("useHeader", "true")
      .option("inferSchema", "true")
      .load("dbfs:/FileStore/nba.xlsx"))

display(df)

# COMMAND ----------

# For Python
import pandas as pd

# COMMAND ----------

pip install openpyxl

# COMMAND ----------

file_path = "/dbfs/FileStore/nba.xlsx"
df = pd.read_excel(file_path)

# COMMAND ----------

display(df)

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %ls fs

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# MAGIC %fs help

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

df= dbutils.fs.ls("/databricks-datasets")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text state default "CA"

# COMMAND ----------

# MAGIC %sql
# MAGIC remove widget state 

# COMMAND ----------

# MAGIC %using python creating widgets

# COMMAND ----------

dbutils.widgets.text("name", "brickser", "name")
dbutils.widgets.multiselect("colors", "orange",["red", "orange", "black", "blue"], "traffic sources")

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

df=spark.read.format('/dbfs/FileStore/nba.xlsx')

# COMMAND ----------

display(df)

# COMMAND ----------

df.schema

# COMMAND ----------


