// Databricks notebook source
Para crear un conjunto de datos [Bloggers] distribuido, primero debemos definir una clase de caso de Scala que defina cada campo individual que comprende un objeto de Scala. Esta clase de caso sirve como modelo o esquema para el objeto escrito Bloggers

// COMMAND ----------

import org.apache.spark.sql.types._
case class Bloggers(Id:BigInt, First:String, Last:String, Url:String, Hits: BigInt, Campaigns:Array[String])

val bloggers = "/FileStore/tables/blogs2_json.json"

//val bloggersDS = spark.read.json(bloggers).as[Bloggers]

val bloggersDS = spark.read.format("json").option("path", bloggers).load().as[Bloggers]
