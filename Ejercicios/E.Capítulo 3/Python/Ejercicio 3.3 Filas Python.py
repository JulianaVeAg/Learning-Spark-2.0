# Databricks notebook source
# MAGIC %md
# MAGIC *Acceder a las filas*

# COMMAND ----------

from pyspark.sql import Row
blog_row = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",["twitter", "LinkedIn"])
# access using index for individual items
blog_row[1]


# COMMAND ----------

# MAGIC %md
# MAGIC *Los objetos de fila se pueden usar para crear marcos de datos si los necesita para una interactividad y exploración rápidas:*

# COMMAND ----------

# In Python 
rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
authors_df = spark.createDataFrame(rows, ["Authors", "State"])
authors_df.show()