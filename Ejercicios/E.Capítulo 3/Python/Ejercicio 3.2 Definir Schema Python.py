# Databricks notebook source
# MAGIC %md
# MAGIC *Para definir un esquema programáticamente para un DataFrame con tres columnas con nombre,autor, título y páginas, puede usar la API Spark DataFrame.*

# COMMAND ----------

# In Python
from pyspark.sql.types import *
schema = StructType([StructField("author", StringType(), False),
 StructField("title", StringType(), False),
 StructField("pages", IntegerType(), False)])

# COMMAND ----------

# MAGIC %md
# MAGIC *Definir el mismo esquema usando DDL es mucho más simple:*

# COMMAND ----------

# In Python
schema = "author STRING, title STRING, pages INT"

# COMMAND ----------

# MAGIC %md
# MAGIC *Puede elegir la forma que desee para definir un esquema.*

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# define schema for our data
schema = StructType([
   StructField("Id", IntegerType(), False),
   StructField("First", StringType(), False),
   StructField("Last", StringType(), False),
   StructField("Url", StringType(), False),
   StructField("Published", StringType(), False),
   StructField("Hits", IntegerType(), False),
   StructField("Campaigns", ArrayType(StringType()), False)])

#create our data
data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
       [2, "Brooke","Wenig","https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
       [3, "Denny", "Lee", "https://tinyurl.3","6/7/2019",7659, ["web", "twitter", "FB", "LinkedIn"]],
       [4, "Tathagata", "Das","https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
       [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
       [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]
      ]

# create a DataFrame using the schema defined above
blogs_df = spark.createDataFrame(data, schema)
# show the DataFrame; it should reflect our table above
blogs_df.show()
print()
# print the schema used by Spark to process the DataFrame
print(blogs_df.printSchema())
# Show columns and expressions
blogs_df.select(expr("Hits") * 2).show(2)
blogs_df.select(col("Hits") * 2).show(2)
blogs_df.select(expr("Hits * 2")).show(2)
# show heavy hitters
blogs_df.withColumn("Big Hitters", (expr("Hits > 10000"))).show()
print(blogs_df.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC *Si desea utilizar este esquema en otra parte de su código, simplemente ejecute blogs_df.schema y devolverá la definición del esquema.*