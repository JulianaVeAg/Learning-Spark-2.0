// Databricks notebook source
// In Scala
val path = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
val binaryFilesDF = spark.read.format("binaryFile")
 .option("pathGlobFilter", "*.jpg")
 .load(path)
binaryFilesDF.show(5)

// COMMAND ----------

// In Scala
val binaryFilesDF = spark.read.format("binaryFile")
 .option("pathGlobFilter", "*.jpg")
 .option("recursiveFileLookup", "true")
 .load(path)
binaryFilesDF.show(5)
