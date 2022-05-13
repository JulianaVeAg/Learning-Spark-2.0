# Databricks notebook source
# In Python
# Import pandas
import pandas as pd
# Import various pyspark SQL functions including pandas_udf
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType
# Declare the cubed function 
def cubed(a: pd.Series) -> pd.Series:
 return a * a * a
# Create the pandas UDF for the cubed function 
cubed_udf = pandas_udf(cubed, returnType=LongType())


# COMMAND ----------

Declara una función llamada cubed() que realiza una operación cubicada. Esta es una función normal de Pandas con la llamada adicional cubed_udf = pandas_udf() para crear nuestra UDF de Pandas.
Comencemos con una Serie Pandas (como se define para x) y luego apliquemos la función local cubed() para el cálculo cubicado

# COMMAND ----------

# Create a Pandas Series
x = pd.Series([1, 2, 3])
# The function for a pandas_udf executed with local Pandas data
print(cubed(x))

# COMMAND ----------

Ahora cambiemos a un Spark DataFrame. Podemos ejecutar esta función como un UDF vectorizado de Spark de la siguiente manera:

# COMMAND ----------

# Create a Spark DataFrame, 'spark' is an existing SparkSession
df = spark.range(1, 4)
# Execute function as a Spark vectorized UDF
df.select("id", cubed_udf(col("id"))).show()

# COMMAND ----------

la función local anterior es una función de Pandas ejecutada solo en el controlador Spark