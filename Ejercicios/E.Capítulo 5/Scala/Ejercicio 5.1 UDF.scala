// Databricks notebook source
// In Scala
// Create cubed function
val cubed = (s: Long) => {s * s * s}
// Register UDF
spark.udf.register("cubed", cubed)
// Create temporary view
spark.range(1, 9).createOrReplaceTempView("udf_test")


// COMMAND ----------

Ahora puede usar Spark SQL para ejecutar cualquiera de estas funciones cubed():

// COMMAND ----------

spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

// COMMAND ----------



// COMMAND ----------

