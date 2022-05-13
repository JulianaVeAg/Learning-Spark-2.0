# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC *GlobalTempView vs TempView*
# MAGIC ----------------------------
# MAGIC 
# MAGIC createOrReplaceTempView()  crea/reemplaza una vista temporal local con el marco de datos proporcionado. La vida útil de esta vista depende de  la clase SparkSession.
# MAGIC 
# MAGIC 
# MAGIC createGlobalTempView()  crea una vista temporal global con el marco de datos proporcionado. La vida útil de esta vista depende de la activación de la propia aplicación. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Leer los AVRO, Parquet, JSON y CSV escritos en el cap3

# COMMAND ----------

#Parquet

parquetVuelo= """/tmp/data/parquet/df_parquet"""

parquetVuelo_df= spark.read.format("parquet").load(parquetVuelo)

# COMMAND ----------

# MAGIC %md
# MAGIC Formato csv
# MAGIC 
# MAGIC 
# MAGIC Leer un archivo CSV en un DataFrame:
# MAGIC 
# MAGIC 
# MAGIC df = spark.read.format("csv")
# MAGIC .schema(schema)
# MAGIC 
# MAGIC .option("header", "true")
# MAGIC 
# MAGIC .option("mode", "FAILFAST") // Exit if any errors
# MAGIC 
# MAGIC .option("nullValue", "") // Replace any null data with quotes
# MAGIC 
# MAGIC .load(file)
# MAGIC 
# MAGIC ------------------------------
# MAGIC fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
# MAGIC 
# MAGIC ----------------------------------------------------------
# MAGIC 
# MAGIC Guarda el contenido del DataFrame en formato CSV en la ruta especificada.
# MAGIC 
# MAGIC 
# MAGIC df.write.csv(os.path.join(tempfile.mkdtemp(), 'data'))

# COMMAND ----------

fire_csv= "/tmp/data/csv/fire_df_csv"
fire_csv_df= spark.read.csv(fire_csv, header=True)

# COMMAND ----------

#AVRO
mnm_avro= "/tmp/avro/zipcodes.avro"

mnm_df_avro= spark.read.format("avro").load(mnm_avro)

# COMMAND ----------

#JSON

mnm_json="/tmp/json/zipcodes.json"

mnm_json_df= spark.read.json(mnm_json)

# COMMAND ----------

# MAGIC %md
# MAGIC json(path, schema=None, primitivesAsString=None, prefersDecimal=None, allowComments=None, allowUnquotedFieldNames=None, allowSingleQuotes=None, allowNumericLeadingZero=None, allowBackslashEscapingAnyCharacter=None, mode=None, columnNameOfCorruptRecord=None, dateFormat=None, timestampFormat=None, multiLine=None, allowUnquotedControlChars=None, lineSep=None, samplingRatio=None, dropFieldIfAllNull=None, encoding=None)
# MAGIC 
# MAGIC Loads JSON files and returns the results as a DataFrame.
# MAGIC 
# MAGIC JSON Lines (newline-delimited JSON) is supported by default. For JSON (one record per file), set the multiLine parameter to true.
# MAGIC 
# MAGIC df1 = spark.read.json('python/test_support/sql/people.json')