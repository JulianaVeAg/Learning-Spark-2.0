# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Analizaremos algunos ejemplos de consultas en el conjunto de datos Rendimiento de la puntualidad de la aerolínea y Causas de los retrasos en los vuelos, que contiene datos sobre vuelos de EE. UU., incluida la fecha, el retraso, la distancia, el origen y el destino.

# COMMAND ----------

# In Python
from pyspark.sql import SparkSession 
# Path to data set
csv_file = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
# Read and create a temporary view
# Infer schema (note that for larger files you 
# may want to specify the schema)
df = (spark.read.format("csv")
.option("inferSchema", "true")
.option("header", "true")
.load(csv_file))

df.createOrReplaceTempView("us_delay_flights_tbl")

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora que tenemos una vista temporal, podemos emitir consultas SQL usando Spark SQL.
# MAGIC El conjunto de datos de retrasos de vuelos de EE. UU. tiene cinco columnas:
# MAGIC 
# MAGIC • La columna de fecha contiene una cadena como 02190925. Cuando se convierte, se asigna a 02-19 09:25 am.
# MAGIC 
# MAGIC • La columna de demora da la demora en minutos entre la hora de salida programada y la real. Las salidas anticipadas muestran números negativos.
# MAGIC 
# MAGIC • La columna de distancia da la distancia en millas desde el aeropuerto de origen hasta el aeropuerto de destino.
# MAGIC 
# MAGIC • La columna de origen contiene el código de aeropuerto IATA de origen.
# MAGIC 
# MAGIC • La columna de destino contiene el código de aeropuerto IATA de destino.
# MAGIC 
# MAGIC Con eso en mente, probemos algunas consultas de ejemplo en este conjunto de datos.
# MAGIC 
# MAGIC Primero, buscaremos todos los vuelos cuya distancia sea mayor a 1,000 millas:

# COMMAND ----------

spark.sql("""SELECT distance, origin, destination 
FROM us_delay_flights_tbl WHERE distance > 1000 
ORDER BY distance DESC""").show(10)

# COMMAND ----------

Como muestran los resultados, todos los vuelos más largos fueron entre Honolulu (HNL) y Nueva York (JFK). A continuación, encontraremos todos los vuelos entre San Francisco (SFO) y Chicago (ORD) con al menos dos horas de retraso:

# COMMAND ----------

spark.sql("""SELECT date, delay, origin, destination 
FROM us_delay_flights_tbl 
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' 
ORDER by delay DESC""").show(10)

# COMMAND ----------

En el siguiente ejemplo, queremos etiquetar todos los vuelos de EE. UU., independientemente del origen y el destino, con una indicación de los retrasos que experimentaron: retrasos muy largos (> 6 horas), retrasos largos (2 a 6 horas), etc. Agregaré estas etiquetas legibles por humanos en una nueva columna llamada Flight_Delays:

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC Las tres consultas SQL anteriores se pueden expresar con una consulta API DataFrame equivalente. Por ejemplo, la primera consulta se puede expresar en la API de Python DataFrame como:

# COMMAND ----------

# In Python
from pyspark.sql.functions import col, desc
(df.select("distance", "origin", "destination")
 .where(col("distance") > 1000)
 .orderBy(desc("distance"))).show(10)


# COMMAND ----------

# Or
(df.select("distance", "origin", "destination")
 .where("distance > 1000")
 .orderBy("distance", ascending=False).show(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Las tres consultas SQL anteriores se pueden expresar con una consulta API DataFrame equivalente. Por ejemplo, la primera consulta se puede expresar en la API de Python DataFrame como:

# COMMAND ----------

# In Python
from pyspark.sql.functions import col, desc
(df.select("distance", "origin", "destination")
 .where(col("distance") > 1000)
 .orderBy(desc("distance"))).show(10)
# Or
(df.select("distance", "origin", "destination")
 .where("distance > 1000")
 .orderBy("distance", ascending=False).show(10))


# COMMAND ----------

# MAGIC %md
# MAGIC Ejercicios extra del libro: 
# MAGIC   - Expresar las otras dos consutlas SQL con la API DataFrame equivalente:
# MAGIC     

# COMMAND ----------

#spark.sql("""SELECT date, delay, origin, destination 
#FROM us_delay_flights_tbl 
#WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' 
#ORDER by delay DESC""").show(10)


from pyspark.sql.functions import col, desc

(df.select("date", "delay","origin", "destination")
.where((col("delay") > 120) & (col("origin")== "SFO") & (col("destination") == "ORD"))
.orderBy("delay", ascending=False).show(10))

# COMMAND ----------

from pyspark.sql.functions import expr,desc,col,asc

df1=df.select(col("delay"),col("origin"),col("destination"), expr("CASE WHEN delay > 360 THEN 'Very LOng Delays' " +
           "WHEN delay > 120 AND delay < 360 THEN 'Long Delays' "+
           "WHEN delay > 60 AND delay < 120 THEN 'Short Delays'" +
           "WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'" +
           "WHEN delay = 0 THEN 'No Delays'"
           "ELSE 'Early' END").alias("Flight_Delays"))
df1.orderBy("origin","delay",descending=False).show(10)




# COMMAND ----------

Otras froma de hacerlo:

# COMMAND ----------

from pyspark.sql.functions import desc,col,when, asc

df4=df.select(col("delay"),col("origin"),col("destination"),when(df.delay > 360 , "Very LOng Delays")
                .when((df.delay >120) & (df.delay < 360), "Long Delays")
                .when((df.delay >60) & (df.delay < 120), "Short Delays")
                .when((df.delay >0) & (df.delay < 60), "Tolerable Delays")
                .when(df.delay > 360 , "Very LOng Delays")
                .otherwise("Early").alias("Flight_Delays"))


df4.orderBy("origin","delay",ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Como muestran estos ejemplos, usar la interfaz Spark SQL para consultar datos es similar a escribir una consulta SQL normal en una tabla de base de datos relacional

# COMMAND ----------

# MAGIC %md
# MAGIC Usando el conjunto de datos de retrasos de vuelos de EE. UU., creemos una tabla administrada y no administrada. Para comenzar, crearemos una base de datos llamada
# MAGIC learn_spark_db y dile a Spark que queremos usar esa base de datos:

# COMMAND ----------

spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")

# COMMAND ----------

# MAGIC %md
# MAGIC Cualquier comando que emitamos en nuestra aplicación para crear tablas dará como resultado que las tablas se creen en esta base de datos y residan bajo el nombre de la base de datos learn_spark_db.

# COMMAND ----------

# MAGIC %md
# MAGIC Crear una tabla administrada
# MAGIC Para crear una tabla administrada dentro de la base de datos learn_spark_db, puede emitir una consulta SQL como la siguiente:

# COMMAND ----------


spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")


# COMMAND ----------

Podemos hacer lo mismo usando la API de DataFrame:

# COMMAND ----------

# MAGIC %md
# MAGIC  In Python
# MAGIC * Path to our US flight delays CSV file 
# MAGIC csv_file = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
# MAGIC * Schema as defined in the preceding example
# MAGIC schema="date STRING, delay INT, distance INT, origin STRING, destination STRING"
# MAGIC flights_df = spark.read.csv(csv_file, schema=schema)
# MAGIC flights_df.write.saveAsTable("managed_us_delay_flights_tbl")

# COMMAND ----------

# MAGIC %md
# MAGIC *Creanting views*

# COMMAND ----------

# In Python
df_sfo = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")
df_jfk = spark.sql("SELECT date, delay, origin, destination FROM  us_delay_flights_tbl WHERE origin = 'JFK'")
# Create a temporary and global temporary view
df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

# COMMAND ----------

Con una base de datos existente, learn_spark_db, y una tabla, us_delay_flights_tbl, listas para usar. En lugar de leer desde un archivo JSON externo, simplemente puede usar SQL para consultar la tabla y asignar el resultado devuelto a un DataFrame:

# COMMAND ----------

# In Python
us_flights_df = spark.sql("SELECT * FROM us_delay_flights_tbl")
us_flights_df2 = spark.table("us_delay_flights_tbl")


# COMMAND ----------

# In Python
spark.sql("SELECT * FROM us_delay_flights_tbl").show()