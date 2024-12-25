# Databricks notebook source
df= spark.read.format('csv').option('header','True').load('/FileStore/Baby_Names__Beginning_2007.csv')
display(df)

# COMMAND ----------

df1=df.dropDuplicates(["First Name"])
display(df1)

# COMMAND ----------

df1= spark.read.format('csv').load('dbfs:/FileStore/Baby_Names__Beginning_2007.csv')
df1.show()

# COMMAND ----------

df = spark.read.format('csv').load('/dbfs/FileStore/tables/Baby_Names__Beginning_2007.csv')
display(df)
