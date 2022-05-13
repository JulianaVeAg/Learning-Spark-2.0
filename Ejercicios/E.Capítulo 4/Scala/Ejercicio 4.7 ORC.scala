// Databricks notebook source
// In Scala 
val file = "/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"

val df = spark.read.format("orc").load(file)

df.show(10, false)

// COMMAND ----------

// MAGIC %sql
// MAGIC --*Reading an ORC file into a Spark SQL table*
// MAGIC 
// MAGIC -- In SQL
// MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
// MAGIC  USING orc
// MAGIC  OPTIONS (
// MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"
// MAGIC  )

// COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show()

// COMMAND ----------

//Writing DataFrames to ORC files

// In Scala

df.write.format("orc").mode("overwrite").option("compression", "snappy").save("/tmp/data/orc/df_orc")