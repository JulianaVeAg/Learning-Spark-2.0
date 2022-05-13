// Databricks notebook source
val file = "/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet"
val df = spark.read.format("parquet").load(file)

val df4 = spark.read.format("json")
 .load("/databricks-datasets/learning-spark-v2/flights/summary-data/json/*")


// COMMAND ----------

// MAGIC %sql
// MAGIC --*Reading a JSON file into a Spark SQL table*
// MAGIC 
// MAGIC -- In SQL
// MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl USING json OPTIONS (path "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*")

// COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show()

// COMMAND ----------

//Writing DataFrames to JSON files

// In Scala
df.write.format("json")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("/tmp/data/json/df_json")
