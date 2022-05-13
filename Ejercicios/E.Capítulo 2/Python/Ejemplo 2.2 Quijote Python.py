# Databricks notebook source
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count


    
quijote_df= (spark.read.option("Header","true")
                           .option("inferShema","true")
                           .csv("/FileStore/datos/el_quijote.txt"))
    
quijote_df.show()
print("Total Rows = %d" % (quijote_df.count()))
#ToPandas da como resultado una tabla
quijote_df.toPandas()

quijote_df.show(truncate=3)
quijote_df.show(vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC *DIFERENCIA METODOS*
# MAGIC 
# MAGIC *head*: Returns the first n rows.

# COMMAND ----------

quijote_df.head(2)


# COMMAND ----------

# MAGIC %md
# MAGIC *Take*: Returns the first num rows as a list of Row.

# COMMAND ----------

quijote_df.take(2)

# COMMAND ----------

# MAGIC %md
# MAGIC *First*: Returns the first row as a Row.

# COMMAND ----------

quijote_df.first()

# COMMAND ----------

take_result=quijote_df.head()

head_result=quijote_df.take(2)

first_result=quijote_df.first()

print("El tipo del resultado de ejecutar take es {}\nEl tipo del resultado de ejecutar head es {}\nEl tipo del resultado de ejecutar first es {}".format(type(take_result),type(head_result),type(first_result)))