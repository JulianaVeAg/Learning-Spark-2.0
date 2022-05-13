// Databricks notebook source

import org.apache.spark.sql.Row
val row = Row(350, true, "Learning Spark 2E", null)



row.getInt(0)


// COMMAND ----------

row.getBoolean(1)


// COMMAND ----------

row.getString(2)