# Databricks notebook source
# In Python
from pyspark.sql.types import LongType
# Create cubed function
def cubed(s): return s * s * s
# Register UDF
spark.udf.register("cubed", cubed, LongType())
# Generate temporary view
spark.range(1, 9).createOrReplaceTempView("udf_test")


# COMMAND ----------

Ahora puede usar Spark SQL para ejecutar cualquiera de estas funciones cubed():

# COMMAND ----------

spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()