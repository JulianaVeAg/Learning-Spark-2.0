# Databricks notebook source
# MAGIC %md
# MAGIC Para comenzar, leamos un archivo CSV grande que contiene datos sobre las llamadas del Departamento de Bomberos de San Francisco.1 Como se mencionó anteriormente, definiremos un esquema para este archivo y usaremos la clase DataFrameReader y sus métodos para decirle a Spark qué hacer. Debido a que este archivo contiene 28 columnas y más de 4 380 660 registros, es más eficiente definirlo que dejar que Spark lo deduzca.

# COMMAND ----------

# In Python, define a schema 
from pyspark.sql.types import *
# Programmatic way to define a schema 
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
 StructField('UnitID', StringType(), True),
 StructField('IncidentNumber', IntegerType(), True),
 StructField('CallType', StringType(), True), 
 StructField('CallDate', StringType(), True), 
 StructField('WatchDate', StringType(), True),
 StructField('CallFinalDisposition', StringType(), True),
 StructField('AvailableDtTm', StringType(), True),
 StructField('Address', StringType(), True), 
 StructField('City', StringType(), True), 
 StructField('Zipcode', IntegerType(), True), 
 StructField('Battalion', StringType(), True), 
 StructField('StationArea', StringType(), True), 
 StructField('Box', StringType(), True), 
 StructField('OriginalPriority', StringType(), True), 
 StructField('Priority', StringType(), True), 
 StructField('FinalPriority', IntegerType(), True), 
 StructField('ALSUnit', BooleanType(), True), 
 StructField('CallTypeGroup', StringType(), True),
 StructField('NumAlarms', IntegerType(), True),
 StructField('UnitType', StringType(), True),
 StructField('UnitSequenceInCallDispatch', IntegerType(), True),
 StructField('FirePreventionDistrict', StringType(), True),
 StructField('SupervisorDistrict', StringType(), True),
 StructField('Neighborhood', StringType(), True),
 StructField('Location', StringType(), True),
 StructField('RowID', StringType(), True),
 StructField('Delay', FloatType(), True)])
# Use the DataFrameReader interface to read a CSV file
sf_fire_file = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC Proyecciones y filtros. Una proyección en el lenguaje relacional es una forma de devolver solo las filas que coinciden con una determinada condición relacional mediante el uso de filtros. En Spark, las proyecciones se realizan con el método select(), mientras que los filtros se pueden expresar con el método filter() o where().

# COMMAND ----------

# In Python
from pyspark.sql.functions import col

few_fire_df = (fire_df
 .select("IncidentNumber", "AvailableDtTm", "CallType")
 .where(col("CallType") != "Medical Incident"))
few_fire_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ¿Cuántos CallTypes distintos se registraron como las causas de las llamadas de incendio?

# COMMAND ----------

from pyspark.sql.functions import *

# In Python, return number of distinct types of calls using countDistinct()

(fire_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .agg(countDistinct("CallType").alias("DistinctCallTypes"))
 .show())


# COMMAND ----------

# MAGIC %md
# MAGIC Podemos enumerar los distintos tipos de llamadas en el conjunto de datos usando estas consultas:

# COMMAND ----------

# In Python, filter for only distinct non-null CallTypes from all the rows
(fire_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .distinct()
 .show(10, False))


# COMMAND ----------

# MAGIC %md
# MAGIC Cambiar el nombre, agregar y eliminar columnas. A veces desea cambiar el nombre de columnas particulares por razones de estilo o convención, y otras veces por legibilidad o brevedad. Los nombres de las columnas originales en el conjunto de datos del Departamento de Bomberos de SF tenían espacios en ellos. Por ejemplo, el nombre de la columna IncidentNumber era Incident Number. Los espacios en los nombres de las columnas pueden ser problemáticos, especialmente cuando desea escribir o guardar un DataFrame como un archivo Parquet (que lo prohíbe).
# MAGIC Al especificar los nombres de las columnas deseadas en el esquema con StructField, como hicimos nosotros, cambiamos efectivamente todos los nombres en el DataFrame resultante.
# MAGIC Como alternativa, puede cambiar el nombre de las columnas de forma selectiva con el método withColumnRenamed(). Por ejemplo, cambiemos el nombre de nuestra columna Delay a ResponseDelayedinMins y echemos un vistazo a los tiempos de respuesta que duraron más de cinco minutos:

# COMMAND ----------

# In Python
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
(new_fire_df
 .select("ResponseDelayedinMins")
 .where(col("ResponseDelayedinMins") > 5)
 .show(5, False))

# COMMAND ----------

# MAGIC %md
# MAGIC Debido a que las transformaciones de DataFrame son inmutables, cuando cambiamos el nombre de una columna usando withColumnRenamed() obtenemos un nuevo DataFrame mientras conservamos el original con el nombre de columna anterior.
# MAGIC Modificar el contenido de una columna o su tipo son operaciones comunes durante la exploración de datos. En algunos casos, los datos están crudos o sucios, o sus tipos no se pueden proporcionar como argumentos a los operadores relacionales. Por ejemplo, en nuestro conjunto de datos del Departamento de Bomberos de SF, las columnas CallDate, WatchDate y AlarmDtTm son cadenas
# MAGIC en lugar de marcas de tiempo de Unix o fechas de SQL, que Spark admite y puede manipular fácilmente durante transformaciones o acciones (por ejemplo, durante un análisis de datos basado en fecha u hora).
# MAGIC Entonces, ¿cómo los convertimos a un formato más útil? Es bastante simple, gracias a algunos métodos API de alto nivel. spark.sql.functions tiene un conjunto de funciones de fecha/marca de tiempo hasta/desde como to_timestamp() y to_date() que podemos usar solo para esto
# MAGIC objetivo:

# COMMAND ----------

# In Python
fire_ts_df = (new_fire_df
 .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
 .drop("CallDate")
 .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
 .drop("WatchDate")
 .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
 "MM/dd/yyyy hh:mm:ss a"))
 .drop("AvailableDtTm"))
# Select the converted columns
(fire_ts_df
 .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
 .show(5, False))

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Convierte el tipo de datos de la columna existente de cadena a una marca de tiempo compatible con Spark.
# MAGIC 2. Utilice el nuevo formato especificado en la cadena de formato "MM/dd/aaaa" o "MM/dd/aaaa hh:mm:ss a" según corresponda.
# MAGIC 3. Después de convertir al nuevo tipo de datos, suelte() la columna anterior y agregue la nueva especificada en el primer argumento al método withColumn().
# MAGIC 4. Asigne el nuevo DataFrame modificado a fire_ts_df.

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora que hemos modificado las fechas, podemos consultar usando funciones de spark.sql.functions como month(), year() y day() para explorar más nuestros datos.
# MAGIC Podríamos averiguar cuántas llamadas se registraron en los últimos siete días, o podríamos ver cuántos años de llamadas del Departamento de Bomberos se incluyen en el conjunto de datos con esta consulta:

# COMMAND ----------

# In Python
(fire_ts_df
 .select(year('IncidentDate'))
 .distinct()
 .orderBy(year('IncidentDate'))
 .show())

# COMMAND ----------

# MAGIC %md
# MAGIC ¿cuáles fueron los tipos más comunes de llamadas de incendios?

# COMMAND ----------

# In Python
(fire_ts_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .groupBy("CallType")
 .count()
 .orderBy("count", ascending=False)
 .show(n=10, truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC La API de DataFrame también ofrece el método collect(), pero para DataFrames extremadamente grandes, esto consume muchos recursos (es caro) y es peligroso, ya que puede causar excepciones de falta de memoria (OOM).
# MAGIC A diferencia de count(), que devuelve un solo número al controlador, collect() devuelve una colección de todos los objetos Row en todo el DataFrame o Dataset. Si desea echar un vistazo a algunos registros de fila, es mejor que use take(n), que devolverá solo los primeros n objetos de fila del marco de datos.

# COMMAND ----------

# MAGIC %md
# MAGIC Junto con todos los demás que hemos visto, la API de DataFrame proporciona métodos estadísticos descriptivos como min(), max(), sum() y avg(). Aquí calculamos la suma de alarmas, el tiempo de respuesta promedio y los tiempos de respuesta mínimo y máximo para todas las llamadas de incendio en nuestro conjunto de datos, importando las funciones de PySpark de forma Pythonic para no entrar en conflicto con las funciones integradas de Python:

# COMMAND ----------

# In Python
import pyspark.sql.functions as F
(fire_ts_df
.select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
.show())

# COMMAND ----------

# MAGIC %md
# MAGIC Ejercicios extras del libro 

# COMMAND ----------

# MAGIC %md
# MAGIC ¿Cuáles fueron todos los diferentes tipos de llamadas de emergencia en 2018?

# COMMAND ----------

from pyspark.sql.functions import *
(fire_df
 .select("CallType","CallDate")
 .where(col("CallType").isNotNull())
 .distinct()
 .show(10, False))

# COMMAND ----------

from pyspark.sql.functions import *

(fire_df
 .select("CallType","CallDate")
 .where(col("CallType").isNotNull() & (col("CallDate") == "01/11/2002"))
 .distinct()
 .show(10, False))

# COMMAND ----------

display(fire_ts_df
.select("CallType")
.where(year('IncidentDate') == "2018")
.distinct()
.show(10,False))

# COMMAND ----------

¿Qué meses dentro del año 2018 vieron la mayor cantidad de llamadas de incendio?


# COMMAND ----------

from pyspark.sql.functions import *


fire_ts_df.filter(year("IncidentDate")==2018).groupBy(month("IncidentDate")).count().orderBy("count",ascending=False).show()

# COMMAND ----------

fire_ts_df.select("CallType").where(year('IncidentDate') == "2018").count()


# COMMAND ----------

# MAGIC %md
# MAGIC ¿Qué vecindario en San Francisco generó la mayor cantidad de llamadas de emergencia en 2018?

# COMMAND ----------



fire_ts_df.filter(year("IncidentDate")==2018).groupBy("Neighborhood").count().orderBy("count",ascending=False).show()

# COMMAND ----------

# MAGIC   %md
# MAGIC ¿Qué vecindarios tuvieron los peores tiempos de respuesta a las llamadas de incendios en 2018?

# COMMAND ----------

fire_ts_df.filter(year("IncidentDate")==2018).select("Neighborhood","ResponseDelayedinMins").orderBy("ResponseDelayedinMins",ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC • ¿Qué semana del año en 2018 tuvo la mayor cantidad de llamadas de emergencia?

# COMMAND ----------

fire_ts_df.filter(year("IncidentDate")==2018).groupBy(weekofyear("IncidentDate")).count().orderBy("count",ascending=False).show()

# COMMAND ----------

fire_df.select("Neighborhood","Zipcode").show()


# COMMAND ----------

# MAGIC %md
# MAGIC Writing DataFrames to Parquet files

# COMMAND ----------



(fire_df.write.format("csv")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("/tmp/data/csv/fire_df_csv"))