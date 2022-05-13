// Databricks notebook source
// MAGIC %md
// MAGIC Leer un archivo CSV en un DataFrame:

// COMMAND ----------



// In Scala

val file = "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*"

val schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"

val df = spark.read.format("csv")
 .schema(schema)
 .option("header", "true")
 .option("mode", "FAILFAST") // Exit if any errors
 .option("nullValue", "") // Replace any null data with quotes
 .load(file)

// COMMAND ----------

// MAGIC %sql
// MAGIC --*Leer un archivo CSV en una tabla Spark SQL*
// MAGIC 
// MAGIC --Crear una tabla SQL a partir de una fuente de datos CSV no es diferente de usar Parquet o JSON
// MAGIC 
// MAGIC -- In SQL
// MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
// MAGIC  USING csv
// MAGIC  OPTIONS (
// MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*",
// MAGIC  header "true",
// MAGIC  inferSchema "true",
// MAGIC  mode "FAILFAST"
// MAGIC  )

// COMMAND ----------

// In Scala/Python
spark.sql("SELECT * FROM us_delay_flights_tbl").show(10)


// COMMAND ----------

Guardar un DataFrame como un archivo CSV es simple:

// COMMAND ----------

// In Scala
df.write.format("csv").mode("overwrite").save("/tmp/data/csv/df_csv")

// COMMAND ----------

En pyhton es exactamente igual.