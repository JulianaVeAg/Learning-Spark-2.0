# Databricks notebook source
# MAGIC %md
# MAGIC *Definir un schema*

# COMMAND ----------



schema = StructType([
   StructField("Id", IntegerType(), False),
   StructField("First", StringType(), False),
   StructField("Last", StringType(), False),
   StructField("Url", StringType(), False),
   StructField("Published", StringType(), False),
   StructField("Hits", IntegerType(), False),
   StructField("Campaigns", ArrayType(StringType()), False)])

# COMMAND ----------

# MAGIC %md
# MAGIC *Leer el CSV del ejemplo del cap2 y obtener la estructura del schema dado por  defecto.*

# COMMAND ----------

mnm_df= spark.read.option("header", "true").option("inferShema", "true").csv("/FileStore/tables/mnm_dataset.csv")

# COMMAND ----------

mnm_df.schema

# COMMAND ----------

# MAGIC %md
# MAGIC *Cuando se define un schema al definir un campo por ejemplo StructField('Delay',FloatType(), True) ¿qué significa el último parámetro Boolean?*
# MAGIC 
# MAGIC Indicates if values of this field can be null values.

# COMMAND ----------

# MAGIC %md
# MAGIC *Dataset vs DataFrame (Scala). ¿En qué se diferencian a nivel de código?*

# COMMAND ----------

# MAGIC %md
# MAGIC *Utilizando el mismo ejemplo utilizado en el capítulo para guardar en parquet y guardar los datos en los formatos:*
# MAGIC * JSON
# MAGIC * CSV
# MAGIC * AVRO

# COMMAND ----------


parquet_path = "/FileStore/tables/ejemplo3.parquet"
mnm_df.write.format("parquet").save(parquet_path)

# COMMAND ----------

mnm_df.write.format("avro").save("/tmp/avro/zipcodes.avro")

# COMMAND ----------

mnm_df.write.format("json").save("/tmp/json/zipcodes2.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Revisar al guardar los ficheros (p.e. json, csv, etc) el número de ficheros creados, revisar su contenido para comprender (constatar) como se guardan.
# MAGIC 
# MAGIC * ¿A qué se debe que hayan más de un fichero?
# MAGIC 
# MAGIC Se guardan 4 ficheros por cada archivo: 
# MAGIC   SUCESS
# MAGIC   COMMITTED
# MAGIC   STARTED
# MAGIC   PARTO-000000
# MAGIC   
# MAGIC * ¿Cómo obtener el número de particiones de un DataFrame?
# MAGIC 
# MAGIC defgetNumPartitions: Int
# MAGIC Return the number of partitions in this RDD.
# MAGIC 
# MAGIC * ¿Qué formas existen para modificar el número de particiones de un DataFrame?
# MAGIC mnm_df.repartition(100)