# Databricks notebook source
# MAGIC %fs 
# MAGIC ls

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/

# COMMAND ----------

df=spark.read.format('csv').option('header','True').load('dbfs:/databricks-datasets/airlines/')

# COMMAND ----------

display(df)

# COMMAND ----------

df1=spark.read.format('csv').option('header','True').load('dbfs:/databricks-datasets/amazon/')
display(df1)

# COMMAND ----------

df2=spark.read.format('csv').option('header','True').load('dbfs:/databricks-datasets/credit-card-fraud/')
display(df2)

# COMMAND ----------

df3=spark.read.format('csv').option('header','True').load('dbfs:/databricks-datasets/flights/')
display(df3)


# COMMAND ----------

df3.distinct()
display(df3)

# COMMAND ----------

df3.filter(df3.delay == "0").selectExpr("count(delay) as zero_delay")
display(df3)

# COMMAND ----------


zero_count=df3.select('*',df3.delay.alias('zero_delay')).where(df3.delay == "0").count()


# COMMAND ----------

from pyspark.sql.functions import lit

zero_df= df3.withColumn("zero_delay", lit(zero_count))
display(zero_df)

# COMMAND ----------

from pyspark.sql.functions import lit
filtered_df= df3.filter(df3.delay == "0")
zero_count=filtered_df.count()
result_df=filtered_df.withColumn("zero_delay", lit(zero_count))

# COMMAND ----------

display(filtered_df)

# COMMAND ----------

display(result_df)

# COMMAND ----------

df3.selectExpr('delay as zero_delay').where(df3.delay == "0").count()

# COMMAND ----------

df3.printSchema()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
time_df= df3.withColumn("date_stamp", date_format(col("date"), "dd-MMM-yyyy-EEE")) 
display(time_df)

# COMMAND ----------

df5= df3.withColumn("dt", to_date(col("date"), "d-M-yyyy"))
display(df5)

# COMMAND ----------



# COMMAND ----------

df3.withColumn(
    "data_stamp",
    to_date(col("date").substr(1, 2) + "-" + 
            col("date").substr(3, 2) + "-" + 
            col("date").substr(5, 4), 
            "MM-dd-yyyy"))
display(df3)

# COMMAND ----------

from pyspark.sql.functions import col, to_date, concat_ws

df3 = df3.withColumn(
    "date_stamp",
    to_date(
        concat_ws("-", 
                  col("date").substr(1, 2), 
                  col("date").substr(3, 2), 
                  col("date").substr(5, 4)
                 ),
        "MM-dd-yyyy"
    )
)
display(df3)

# COMMAND ----------

ndf=df3.withColumn("time_stamp",concat_ws("-", 
                  col("date").substr(1, 2), 
                  col("date").substr(3, 2), 
                  col("date").substr(5, 4)
                 ),) \
    .withColumn("time_stamp",to_date("date_stamp", "MM-dd-yyyy"))

display(ndf)


# COMMAND ----------

ndf.printSchema()

# COMMAND ----------

new_df=df3.withColumn(
    "data_stamp",
    to_date(col("date").substr(1, 2) + "-" + 
            col("date").substr(3, 2) + "-" + 
            col("date").substr(5, 4), 
            "MM-dd-yyyy")
)
display(new_df)

# COMMAND ----------

df=spark.sql('select * from adult')
display(df)

# COMMAND ----------

df= spark.read.format('csv').option('header','True').load('/FileStore/Baby_Names__Beginning_2007.csv')
display(df)

# COMMAND ----------

df1=df.dropDuplicates(["First Name"])
display(df1)

# COMMAND ----------

df1= spark.read.format('csv').load('dbfs:/FileStore/Baby_Names__Beginning_2007.csv')
df1.show()

# COMMAND ----------

df = spark.read.format('csv').option('Header','True').load('dbfs:/FileStore/Baby_Names__Beginning_2007.csv')
display(df)

# COMMAND ----------

temp_table_name= "baby_names_2007_view"
df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from baby_names_2007_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as total,sex from baby_names_2007_view group by(sex)

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(count) from baby_names_2007_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(count) from baby_names_2007_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select avg(count) from baby_names_2007_view

# COMMAND ----------

df.groupBy("county").avg("Count")
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

df = df.withColumn("Count", col("Count").cast("int"))
df_grouped = df.groupBy("county").sum("Count")
display(df_grouped)

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_timestamp()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT from_utc_timestamp(current_timestamp(), 'GMT') AS gmt_timestamp

# COMMAND ----------

df= spark.sql("SELECT current_timestamp() as gmt_timestamp")
display(df)

# COMMAND ----------


