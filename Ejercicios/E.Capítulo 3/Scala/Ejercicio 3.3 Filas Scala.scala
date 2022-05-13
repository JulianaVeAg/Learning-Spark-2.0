// Databricks notebook source
// MAGIC %md
// MAGIC *Acceder a las filas*

// COMMAND ----------



import org.apache.spark.sql.Row
val blogRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",Array("twitter", "LinkedIn"))
blogRow(1)

// COMMAND ----------

// MAGIC %md
// MAGIC *Los objetos de fila se pueden usar para crear marcos de datos si los necesita para una interactividad y exploración rápidas:*

// COMMAND ----------

// In Scala
val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))
val authorsDF = rows.toDF("Author", "State")
authorsDF.show()