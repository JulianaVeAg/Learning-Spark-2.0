// Databricks notebook source
// MAGIC %md
// MAGIC ####Ejercicio Contando M&M con Scala

// COMMAND ----------



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/**
 * Usage: MnMcount <mnm_file_dataset>
 */   

 // Read the file into a Spark DataFrame
 val mnmDF = spark.read.format("csv")
         .option("header", "true")
         .option("inferSchema", "true")
         .load("/FileStore/tables/mnm_dataset.csv")
     // Aggregate counts of all colors and groupBy() State and Color
 // orderBy() in descending order


val countMnMDF = mnmDF
     .select("State", "Color", "Count")
     .groupBy("State", "Color")
     .agg(count("Count").alias("Total"))
     .orderBy(desc("Total"))
 // Show the resulting aggregations for all the states and colors
 countMnMDF.show(60)
 println(s"Total Rows = ${countMnMDF.count()}")
 println()
 // Find the aggregate counts for California by filtering
 val caCountMnMDF = mnmDF
     .select("State", "Color", "Count")
     .where(col("State") === "CA")
     .groupBy("State", "Color")
     .agg(count("Count").alias("Total"))
     .orderBy(desc("Total"))
 // Show the resulting aggregations for California
 caCountMnMDF.show(10)


mnmDF.groupBy("State").agg(max("Count").as("max_count")).orderBy(asc ("max_count")).show(10)

mnmDF.select ("State", "Color").where((col("State") === "CA" or col("State") === "NV")).show(20)

mnmDF.groupBy("state").agg(
                            max("Count").as("sum_count"),
                            min("Count").as("min_count"),
                            avg("Count").as("media_count"),
                            count("Color").as("cuantos_colores"))
                            .show(10)


mnmDF.createTempView("MNM2")


mnmDF.createTempView("MNM2")