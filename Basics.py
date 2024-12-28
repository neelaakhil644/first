# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *

# COMMAND ----------

spark.createDataFrame([('1', 'mahi'), ('2', 'walt')],schema=['id','name'])

# COMMAND ----------

from pyspark.sql.types import *
data=[{'id' : 1, 'name':'mahi'},
      {'id' : 2, 'name':'walt'}]
df=spark.createDataFrame(data)
df.show()
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
data=[(1, 'mahi'), (2, 'wafa')]
schema= StructType([StructField(name='id', dataType=IntegerType()), StructField(name='name', dataType=StringType())])
df=spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

spark.read.csv(path="dbfs:/FileStore/Baby_Names__Beginning_2007.csv", header=True)

# COMMAND ----------

df=spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/Baby_Names__Beginning_2007.csv")
display(df)

# COMMAND ----------

df1=spark.read.csv(path=["dbfs:/FileStore/Baby_Names__Beginning_2007.csv","dbfs:/FileStore/shopping_trends_updated.numbers"], header=True)
display(df1)

# COMMAND ----------

df.withColumn("Total_count", col("Count")*100)
df.show()

# COMMAND ----------


