// Databricks notebook source
// In Scala
// Use Parquet 
val file = "/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet"
val df = spark.read.format("parquet").load(file)
// Use Parquet; you can omit format("parquet") if you wish as it's the default
val df2 = spark.read.load(file)
// Use CSV
val df3 = spark.read.format("csv")
 .option("inferSchema", "true")
 .option("header", "true")
 .option("mode", "PERMISSIVE")
 .load("/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*")
// Use JSON
val df4 = spark.read.format("json")
 .load("/databricks-datasets/learning-spark-v2/flights/summary-data/json/*")


// COMMAND ----------

Parquet es la fuente de datos predeterminada y preferida para Spark porque es eficiente, usa almacenamiento en columnas y emplea un algoritmo de compresión rápido.

// COMMAND ----------

// In Scala
// Use JSON
val location = ....
df.write.format("json").mode("overwrite").save(location)

// COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show()



// COMMAND ----------

//Writing DataFrames to JSON files

// In Scala
df.write.format("json")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("/tmp/data/json/df_json")

