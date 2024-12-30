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

rename_df=df.withColumnRenamed("Year","Year_Renamed")
display(rename_df)

# COMMAND ----------

df1=spark.read.csv(path=["dbfs:/FileStore/Baby_Names__Beginning_2007.csv","dbfs:/FileStore/shopping_trends_updated.numbers"], header=True)
display(df1)

# COMMAND ----------

df.withColumn("Total_count", col("Count")*100)
df.show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

data=[('abc',[1, 2]), ('def', [3, 4]), ('ghi', [5, 6])]
schema= StructType([StructField('id', Stringtype()),StructField('values', ArrayType(IntegerType()))])

# COMMAND ----------

df3=spark.createDataFrame([Row(a=1,arraylist=[1,2,3], mapfield={'a' : 'b'})])
display(df3)

# COMMAND ----------

display(df3.select(df3.arraylist, explode(df3.arraylist).alias("exploid arraylist")))

# COMMAND ----------

# MAGIC %fs ls
# MAGIC

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/

# COMMAND ----------

ls -l dbfs:/databricks-datasets/COVID/

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets/COVID/')

# COMMAND ----------

df2=spark.read.format('csv').option('header','true').load('ddbfs:dbfs:/databricks-datasets/samples/')
display(df2)

# COMMAND ----------

df5=spark.createDataFrame([('OneATwoBThree', )],['str', ])
df5.select( split(df5.str, '[AB]').alias('str'))
display(df5)

# COMMAND ----------

df6=df1.withColumn("full name", array(col("First Name"), col("County")))
display(df6)

# COMMAND ----------

from pyspark.sql.functions import array_contains, col


# COMMAND ----------

df8=df1.withColumn("albany county", array_contains(array(col("county")), "Albany"))
display(df8)

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType, MapType

# COMMAND ----------

data=[('M', {'Age' : 30,'city' : 'BH'}), ('A', {'Age' : 25,'city' : 'NY'}), ('B', {'Age' : 28,'city' : 'AP'})]
schema= StructType([StructField('name', StringType(), True), StructField('properties', MapType(StringType(), StringType()), True)])
df=spark.createDataFrame(data, schema)

# COMMAND ----------

df10=df.withColumn('city',df.properties["city"])
display(df10)

# COMMAND ----------

df.withColumn('Age', df.properties.getItem('Age')).show()

# COMMAND ----------

from pyspark.sql import Row
a= Row('james',45)
b= Row(name='alice', age=58)
print(a[0]+ " "+ str(a[1]))
print(b.name+ " "+ str(b.age))

# COMMAND ----------

person= Row("name", "age")
p1= person('k', 23)
p2= person('l', 25)
print(p1.name)



# COMMAND ----------

display(df)

# COMMAND ----------

display(df1)

# COMMAND ----------

df12=df1.select(df1['First Name'], when(df1['Sex']=='F', 'Female'),when(df1['Sex']=='M', 'Male').otherwise('Unknown').alias('gender'))
display(df12)

# COMMAND ----------

df1.filter(df1['county'].like('A%')).show()

# COMMAND ----------

df1.filter(col('count') < 30).show()

# COMMAND ----------

df1.where((col('count') < 30) & (col('count') > 20)).show()

# COMMAND ----------

from pyspark.sql.functions import col
df2=df1.select('First Name', 'count')
df2.sort(col('count').asc())
display(df2)

# COMMAND ----------

from pyspark.sql.functions import col
df3=df1.select('First Name', 'count')
df3.sort(col('count').desc())
display(df3)

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df4=df1.select(col('count').cast('int'))
df4.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

df5=df1.filter(df1['First Name'].like('A%') )
display(df5)

# COMMAND ----------

df6=df1.filter(df1['First Name'].like('__A%') )
display(df6)

# COMMAND ----------

df7=df1.filter(~df1['First Name'].like('A%') )
display(df7)

# COMMAND ----------

df1.filter(col('First Name').startswith('B')).show()

# COMMAND ----------

df1.filter(col('First Name').endswith('B')).show()

# COMMAND ----------

df1.filter(col('First Name').contains('B')).show()

# COMMAND ----------

df1.union(df2).show()

# COMMAND ----------

display(df1)

# COMMAND ----------

df9=df1.select('First Name').groupby('County')
display(df9)

# COMMAND ----------

data=df1.collect()
print(data[0][1])

# COMMAND ----------

# MAGIC %scala

# COMMAND ----------

rdd=spark.sparkContext.parallelize(data)
print(rdd.collect())

# COMMAND ----------

rdd.map(lambda x: x*2)

# COMMAND ----------

from pyspark.sql.functions import current_date

df1.withColumn('Today Date', current_date()).show()
