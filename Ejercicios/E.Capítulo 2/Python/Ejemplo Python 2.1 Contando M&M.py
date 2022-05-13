# Databricks notebook source
# MAGIC %md
# MAGIC **EJEMPLO CONTANDO M&Ms**
# MAGIC 
# MAGIC Escribamos un programa Spark que lea un archivo con más de 100 000 entradas (donde cada fila o línea tiene un <estado, mnm_color, conteo>) y calcule y agregue los conteos para cada color y estado. Estos conteos agregados nos dicen los colores de M&M preferidos por los estudiantes en cada estado.

# COMMAND ----------

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *



mnm_df= spark.read.option("header", "true").option("inferShema", "true").csv("/FileStore/tables/mnm_dataset.csv")


count_mnm_df= mnm_df.select("State", "Color","Count").groupBy("State","Color").agg(count("Count").alias("Total")).orderBy("Total", ascending=False)

count_mnm_df.show(n=60, truncate=False)

print("Total Rows = %d\n" % (count_mnm_df.count()))

count_mnm_df = (mnm_df
             .select("State", "Color", "Count")
             .where(mnm_df.State == "CA" )
             .groupBy("State","Color")
             .agg(count("Count").alias("Total"))
             .orderBy("Total", ascending=False))

count_mnm_df.show(n=10,truncate=False)

count_mnm_df2= (mnm_df.select("State", "Color", "Count")
            .where((mnm_df.State == "CO") | (mnm_df.State == "CA"))
            .groupBy("State", "Color")
            .agg(max("Count")
            .alias("Max_Count"))
            .orderBy("Max_Count", descending=True)
            .show(n=10, truncate=False))


mnm_df.groupBy("State").agg(max("Count").alias("max_count"),
                          min("Count").alias("min_count"),
                          avg("Count").alias("media_count"),
                          count("Count").alias("Count_count")
                         ).show(n=2,truncate=False)

    
    

# COMMAND ----------

mnm_df.createTempView("MnM3")
mnm_df2= spark.sql("select * from MnM3").show()

