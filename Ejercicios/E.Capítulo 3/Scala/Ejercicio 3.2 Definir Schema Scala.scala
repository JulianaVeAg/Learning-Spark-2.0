// Databricks notebook source
// MAGIC %md
// MAGIC *Para definir un esquema programáticamente para un DataFrame con tres columnas con nombre,autor, título y páginas, puede usar la API Spark DataFrame.*

// COMMAND ----------


import org.apache.spark.sql.types._
val schema = StructType(Array(StructField("author", StringType, false),
 StructField("title", StringType, false),
 StructField("pages", IntegerType, false)))

// COMMAND ----------

// MAGIC %md
// MAGIC *Definir el mismo esquema usando DDL es mucho más simple:*

// COMMAND ----------


val schema = "author STRING, title STRING, pages INT"


// COMMAND ----------

// MAGIC %md
// MAGIC *Vamos a leer los datos de un archivo JSON en lugar de crear datos estáticos en Scala:*

// COMMAND ----------


import org.apache.spark.sql.types._
// Get the path to the JSON file
val jsonFile = "/FileStore/tables/blogs-1.json"
// Define our schema programmatically
val schema = StructType(Array(StructField("Id", IntegerType, false),
StructField("First", StringType, false),
StructField("Last", StringType, false),
StructField("Url", StringType, false),
StructField("Published", StringType, false),
StructField("Hits", IntegerType, false),
StructField("Campaigns", ArrayType(StringType), false)))
// Create a DataFrame by reading from the JSON file 
// with a predefined schema
val blogsDF = spark.read.schema(schema).json(jsonFile)
// Show the DataFrame schema as output
blogsDF.show()
// Print the schema
println(blogsDF.printSchema())
println(blogsDF.schema)


// COMMAND ----------

// MAGIC %md
// MAGIC *Lo que podemos hacer con columnas:*

// COMMAND ----------


blogsDF.columns

// COMMAND ----------


blogsDF.col("Id")


// COMMAND ----------

import org.apache.spark.sql.functions._

blogsDF.select(expr("Hits * 2")).show(2)


// COMMAND ----------


 blogsDF.select(col("Hits") * 2).show(2)

// COMMAND ----------


blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()


// COMMAND ----------

// MAGIC %md
// MAGIC defwithColumn(colName: Cadena, col: Columna): DataFrame
// MAGIC Devuelve un nuevo conjunto de datos agregando una columna o reemplazando la columna existente que tiene el mismo nombre.
// MAGIC 
// MAGIC La expresión de la columna solo debe hacer referencia a los atributos proporcionados por este conjunto de datos. Es un error agregar una columna que hace referencia a algún otro conjunto de datos.
// MAGIC 
// MAGIC Nota
// MAGIC este método introduce una proyección internamente. Por lo tanto, llamarlo varias veces, por ejemplo, a través de bucles para agregar varias columnas, puede generar grandes planes que pueden causar problemas de rendimiento e incluso StackOverflowException. Para evitar esto, use seleccionar con varias columnas a la vez.

// COMMAND ----------


blogsDF
 .withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id"))))
 .select(col("AuthorsId"))
 .show(4)


// COMMAND ----------


blogsDF.select(expr("Hits")).show(2)
blogsDF.select(col("Hits")).show(2)
blogsDF.select("Hits").show(2)

// COMMAND ----------


blogsDF.sort(col("Id").desc).show()
blogsDF.sort($"Id".desc).show()

// COMMAND ----------

// MAGIC %md
// MAGIC En este último ejemplo, las expresiones blogs_df.sort(col("Id").desc) y blogsDF.sort($"Id".desc) son idénticos. Ambos ordenan la columna de DataFrame denominada Id en orden descendente: uno usa una función explícita, col("Id"), para devolver un objeto de columna, mientras que el otro usa $ antes del nombre de la columna, que es una función en Spark que convierte la columna denominada Id en una columna.