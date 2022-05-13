// Databricks notebook source
Analizaremos algunos ejemplos de consultas en el conjunto de datos Rendimiento de la puntualidad de la aerolínea y Causas de los retrasos en los vuelos, que contiene datos sobre vuelos de EE. UU., incluida la fecha, el retraso, la distancia, el origen y el destino.

// COMMAND ----------

// In Scala
import org.apache.spark.sql.SparkSession 
// Path to data set 
val csvFile="/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
// Read and create a temporary view
// Infer schema (note that for larger files you may want to specify the schema)
val df = spark.read.format("csv")
 .option("inferSchema", "true")
 .option("header", "true")
 .load(csvFile)
// Create a temporary view
df.createOrReplaceTempView("us_delay_flights_tbl")


// COMMAND ----------

Ahora que tenemos una vista temporal, podemos emitir consultas SQL usando Spark SQL.
Estas consultas no son diferentes de las que podría emitir contra una tabla SQL en, por ejemplo, una base de datos MySQL o PostgreSQL. El punto aquí es mostrar que Spark SQL ofrece una interfaz SQL compatible con ANSI: 2003 y demostrar la interoperabilidad entre SQL y DataFrames.

El conjunto de datos de retrasos de vuelos de EE. UU. tiene cinco columnas:

• La columna de fecha contiene una cadena como 02190925. Cuando se convierte, se asigna a 02-19 09:25 am.

• La columna de demora da la demora en minutos entre la hora de salida programada y la real. Las salidas anticipadas muestran números negativos.

• La columna de distancia da la distancia en millas desde el aeropuerto de origen hasta el aeropuerto de destino.

• La columna de origen contiene el código de aeropuerto IATA de origen.

• La columna de destino contiene el código de aeropuerto IATA de destino.

// COMMAND ----------

spark.sql("""SELECT distance, origin, destination 
FROM us_delay_flights_tbl WHERE distance > 1000 
ORDER BY distance DESC""").show(10)

// COMMAND ----------

spark.sql("""SELECT date, delay, origin, destination 
FROM us_delay_flights_tbl 
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' 
ORDER by delay DESC""").show(10)


// COMMAND ----------

spark.sql("""SELECT delay, origin, destination,
 CASE
 WHEN delay > 360 THEN 'Very Long Delays'
 WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
 WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
 WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
 WHEN delay = 0 THEN 'No Delays'
 ELSE 'Early'
 END AS Flight_Delays
 FROM us_delay_flights_tbl
 ORDER BY origin, delay DESC""").show(10)


// COMMAND ----------

spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")

// COMMAND ----------

Cualquier comando que emitamos en nuestra aplicación para crear tablas dará como resultado que las tablas se creen en esta base de datos y residan bajo el nombre de la base de datos learn_spark_db.

Crear una tabla administrada Para crear una tabla administrada dentro de la base de datos learn_spark_db, puede emitir una consulta SQL como la siguiente:

// COMMAND ----------

spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT,distance INT, origin STRING, destination STRING)")


// COMMAND ----------

// In Scala
spark.sql("SELECT * FROM us_delay_flights_tbl").show()
