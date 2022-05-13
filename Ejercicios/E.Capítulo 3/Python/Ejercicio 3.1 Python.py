# Databricks notebook source
# MAGIC %md
# MAGIC Demostremos primero la expresividad y la composición, con un simple fragmento de código. En el siguiente ejemplo, queremos agregar todas las edades de cada nombre, agrupar por nombre y luego promediar las edades, un patrón común en el análisis y descubrimiento de datos.
# MAGIC Si tuviéramos que usar la API RDD de bajo nivel para esto, el código se vería de la siguiente manera:

# COMMAND ----------


from pyspark.sql.functions import avg

# Create a DataFrame 
data_df = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30),
 ("TD", 35), ("Brooke", 25)], ["name", "age"])
# Group the same names together, aggregate their ages, and compute an average
avg_df = data_df.groupBy("name").agg(avg("age"))
# Show the results of the final execution
avg_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Spark sabe exactamente lo que deseamos hacer: agrupar a las personas por sus nombres, agregar sus edades y luego calcular la edad promedio de todas las personas con el mismo.