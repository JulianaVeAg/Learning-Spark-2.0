# Databricks notebook source
Reading Parquet files into a DataFrame

# COMMAND ----------


# In Python
file = """/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet/"""
df = spark.read.format("parquet").load(file)


# COMMAND ----------

Reading Parquet files into a Spark SQL table


# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SQL
# MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
# MAGIC  USING parquet
# MAGIC  OPTIONS (
# MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet/" )

# COMMAND ----------

# In Python
spark.sql("SELECT * FROM us_delay_flights_tbl").show()

# COMMAND ----------

Writing DataFrames to Parquet files

# COMMAND ----------

# In Python
(df.write.format("parquet")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("/tmp/data/parquet/df_parquet"))
