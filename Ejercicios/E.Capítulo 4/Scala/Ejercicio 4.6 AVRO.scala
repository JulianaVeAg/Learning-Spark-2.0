// Databricks notebook source
Reading an Avro file into a DataFrame


// COMMAND ----------

// In Scala
val df = spark.read.format("avro").load("/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*")

df.show(false)


// COMMAND ----------

Reading an Avro file into a Spark SQL table

// COMMAND ----------

// MAGIC %sql
// MAGIC -- In SQL 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW episode_tbl
// MAGIC  USING avro
// MAGIC  OPTIONS (
// MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*"
// MAGIC  )

// COMMAND ----------


// In Scala
spark.sql("SELECT * FROM episode_tbl").show(false)


// COMMAND ----------

Writing DataFrames to Avro files

// COMMAND ----------

// In Scala
df.write
 .format("avro")
 .mode("overwrite")
 .save("/tmp/data/avro/df_avro")
