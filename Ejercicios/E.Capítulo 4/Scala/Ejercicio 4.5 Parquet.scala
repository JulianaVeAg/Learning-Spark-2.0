// Databricks notebook source
Reading Parquet files into a DataFrame

// COMMAND ----------

// In Scala
val file = """/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet/"""

val df = spark.read.format("parquet").load(file)


// COMMAND ----------

Reading Parquet files into a Spark SQL table

// COMMAND ----------

// MAGIC %sql
// MAGIC --Crear una tabla no administrada de Spark SQL o ver directamente usando SQL:
// MAGIC -- In SQL
// MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
// MAGIC  USING parquet
// MAGIC  OPTIONS (path "/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet/" )

// COMMAND ----------

// In Scala
spark.sql("SELECT * FROM us_delay_flights_tbl").show()

// COMMAND ----------

Writing DataFrames to Parquet files

// COMMAND ----------

// In Scala
df.write.format("parquet")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("/tmp/data/parquet/df_parquet")
