# Databricks notebook source
# MAGIC %scala
# MAGIC // In Scala
# MAGIC import org.apache.spark.sql.functions.avg
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC // Create a DataFrame of names and ages
# MAGIC val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25),
# MAGIC  ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")
# MAGIC // Group the same names together, aggregate their ages, and compute an average
# MAGIC val avgDF = dataDF.groupBy("name").agg(avg("age"))
# MAGIC // Show the results of the final execution
# MAGIC avgDF.show()