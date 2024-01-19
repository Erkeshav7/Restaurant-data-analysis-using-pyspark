# Databricks notebook source
# DBTITLE 1,Starting SparkSession and creating dataframe
from pyspark.sql import *
from pyspark.sql.functions import *
spark=SparkSession.builder.appName('restaurant_revenue_analysis').getOrCreate()

df1= spark.read.format("csv").option("header", "true").option('inferSchema',"true").load("dbfs:/FileStore/FileStore/Restaurant_revenue__1_.csv")
display(df1)

# COMMAND ----------

# DBTITLE 1,Filtering data

df1.filter(col("Number_of_Customers")>50).show()


# COMMAND ----------

# DBTITLE 1,Adding new column
df2=df1.withColumn("double_customers",col("Number_of_Customers")*2)
df2.show()

# COMMAND ----------

# DBTITLE 1,Grouping data
df1.groupBy("cuisine_type").agg(round(avg("number_of_customers"),2).alias('ans')).show()


# COMMAND ----------

# DBTITLE 1,Dropping duplicates
var=df1.count()-df1.dropDuplicates().count()
print(var)



# COMMAND ----------

# DBTITLE 1,Sorting data
df1.orderBy("number_of_customers",ascending=False).show()

# COMMAND ----------

# DBTITLE 1,Changing number of partitions
df1.rdd.getNumPartitions()
df2=df1.repartition(5)
print(df2.rdd.getNumPartitions())


# COMMAND ----------

# DBTITLE 1,Window functions
window_spec=Window.partitionBy("cuisine_type").orderBy(df1["monthly_revenue"].desc())
ranked_df=df1.withColumn("revenue_rank",rank().over(window_spec))
ranked_df.filter(col("revenue_rank")<4).show()

# COMMAND ----------

# DBTITLE 1,Selecting single column with filter
df1.select("menu_price").filter(df1.Menu_Price>30).show()


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


