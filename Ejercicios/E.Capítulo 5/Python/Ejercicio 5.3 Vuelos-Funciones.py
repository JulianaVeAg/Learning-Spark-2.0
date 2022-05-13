# Databricks notebook source
# Set file paths
from pyspark.sql.functions import *
tripdelaysFilePath ="/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

airportsnaFilePath ="/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"

# Obtain airports data set
airportsna = (spark.read
.format("csv")
.options(header="true", inferSchema="true", sep="\t")
.load(airportsnaFilePath))
airportsna.createOrReplaceTempView("airports_na")
# Obtain departure delays data set
departureDelays = (spark.read
.format("csv")
.options(header="true")
.load(tripdelaysFilePath))
departureDelays = (departureDelays
.withColumn("delay", expr("CAST(delay as INT) as delay"))
.withColumn("distance", expr("CAST(distance as INT) as distance")))
departureDelays.createOrReplaceTempView("departureDelays")
# Create temporary small table
foo = (departureDelays
.filter(expr("""origin == 'SEA' and destination == 'SFO' and 
date like '01010%' and delay > 0""")))
foo.createOrReplaceTempView("foo")


# COMMAND ----------


spark.sql("SELECT * FROM airports_na LIMIT 10").show()


# COMMAND ----------

spark.sql("SELECT * FROM departureDelays LIMIT 10").show()


# COMMAND ----------

spark.sql("SELECT * FROM foo").show()


# COMMAND ----------

Unions: Un patrón común dentro de Apache Spark es unir dos DataFrames diferentes con el mismo esquema. Esto se puede lograr usando el método union():

# COMMAND ----------

# Union two tables
bar = departureDelays.union(foo)
bar.createOrReplaceTempView("bar")
# Show the union (filtering for SEA and SFO in a specific time range)
bar.filter(expr("""origin == 'SEA' AND destination == 'SFO'
AND date LIKE '01010%' AND delay > 0""")).show()

# COMMAND ----------

Join: es unir dos DataFrames (o tablas).

# COMMAND ----------

# Join departure delays data (foo) with airport info
foo.join(airports,airports.IATA == foo.origin).select("City", "State", "date", "delay", "distance", "destination").show()

# COMMAND ----------

spark.sql("""
SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination 
 FROM foo f
 JOIN airports_na a
 ON a.IATA = f.origin
""").show()

# COMMAND ----------

Windowing: utiliza valores de las filas de una ventana (un rango de filas de entrada) para devolver un conjunto de valores, normalmente en forma de otra fila.Es posible operar en un grupo de filas sin dejar de devolver un solo valor para cada fila de entrada.
Comencemos con una revisión de los TotalDelays (calculados por sum(Delay)) experimentados por vuelos que se originan en Seattle (SEA), San Francisco (SFO) y la ciudad de Nueva York (JFK) y van a un conjunto específico de ubicaciones de destino.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS departureDelaysWindow;
# MAGIC 
# MAGIC CREATE TABLE departureDelaysWindow AS SELECT origin, destination, SUM(delay) AS TotalDelays FROM departureDelays
# MAGIC WHERE origin IN ('SEA', 'SFO', 'JFK')
# MAGIC AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
# MAGIC GROUP BY origin, destination;
# MAGIC SELECT * FROM departureDelaysWindow

# COMMAND ----------

¿Qué pasaría si para cada uno de estos aeropuertos de origen quisiera encontrar los tres destinos que experimentaron la mayor cantidad de demoras? Puede lograr esto ejecutando tres consultas diferentes para cada origen y luego uniendo los resultados, así:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT origin, destination, SUM(TotalDelays) AS TotalDelays
# MAGIC FROM departureDelaysWindow
# MAGIC WHERE origin = 'JFK'
# MAGIC GROUP BY origin, destination
# MAGIC ORDER BY SUM(TotalDelays) DESC
# MAGIC LIMIT 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT origin, destination, SUM(TotalDelays) AS TotalDelays
# MAGIC FROM departureDelaysWindow
# MAGIC WHERE origin = 'SFO'
# MAGIC GROUP BY origin, destination
# MAGIC ORDER BY SUM(TotalDelays) DESC
# MAGIC LIMIT 3

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT origin, destination, SUM(TotalDelays) AS TotalDelays
# MAGIC FROM departureDelaysWindow
# MAGIC WHERE origin = 'SEA'
# MAGIC GROUP BY origin, destination
# MAGIC ORDER BY SUM(TotalDelays) DESC
# MAGIC LIMIT 3

# COMMAND ----------


spark.sql("""
SELECT origin, destination, TotalDelays, rank 
 FROM ( 
 SELECT origin, destination, TotalDelays, dense_rank() 
 OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank 
 FROM departureDelaysWindow
 ) t 
 WHERE rank <= 3
""").show()


# COMMAND ----------

Mediante el uso de la función de ventana dense_rank(), podemos determinar rápidamente que los destinos con los peores retrasos para las tres ciudades de origen fueron:
• Seattle (SEA): San Francisco (SFO), Denver (DEN) y Chicago (ORD)
• San Francisco (SFO): Los Ángeles (LAX), Chicago (ORD) y Nueva York (JFK)
• Nueva York (JFK): Los Ángeles (LAX), San Francisco (SFO) y Atlanta (ATL)
Es importante tener en cuenta que cada grupo de ventanas debe caber en un solo ejecutor y se compondrá en una sola partición durante la ejecución. Por lo tanto, debe asegurarse de que sus consultas no sean ilimitadas (es decir, limiten el tamaño de su ventana).

# COMMAND ----------


foo.show()

# COMMAND ----------

Para agregar una nueva columna al foo DataFrame, use el método withColumn():

# COMMAND ----------

from pyspark.sql.functions import expr
foo2 = (foo.withColumn(
 "status",
 expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
 ))

# COMMAND ----------

foo2.show()


# COMMAND ----------

foo3 = foo2.drop("delay")
foo3.show()

# COMMAND ----------

foo4 = foo3.withColumnRenamed("status", "flight_status")
foo4.show()

# COMMAND ----------

Pivoting: Se usa para cambiar las columnas por las filas- le permite colocar nombres en la columna del mes (en lugar de 1 y 2, puede mostrar enero y febrero, respectivamente), así como realizar cálculos agregados (en este caso, promedio y máximo) sobre las demoras por destino y mes:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
# MAGIC  FROM departureDelays
# MAGIC WHERE origin = 'SEA'