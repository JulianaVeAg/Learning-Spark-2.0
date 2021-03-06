// Databricks notebook source
// MAGIC %md
// MAGIC Ejemplo Bomberos

// COMMAND ----------

import org.apache.spark.sql.types._
// In Scala 
val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
                                   StructField("UnitID", StringType, true),
                                   StructField("IncidentNumber", IntegerType, true),
                                   StructField("CallType", StringType, true),
                                   StructField("Location", StringType, true),
                                   StructField("CallDate", StringType, true), 
                                   StructField("WatchDate", StringType, true),
                                   StructField("CallFinalDisposition", StringType,true),
                                   StructField("AvailableDtTm", StringType,true),
                                   StructField("Address", StringType,true), 
                                   StructField("City", StringType, true), 
                                   StructField("Zipcode", IntegerType,true), 
                                   StructField("Battalion", StringType, true), 
                                   StructField("StationArea", StringType, true), 
                                   StructField("Box", StringType, true), 
                                   StructField("OriginalPriority", StringType, true), 
                                   StructField("Priority", StringType, true), 
                                   StructField("FinalPriority", IntegerType, true), 
                                   StructField("ALSUnit", BooleanType, true), 
                                   StructField("CallTypeGroup", StringType, true),
                                   StructField("NumAlarms", IntegerType,true),
                                   StructField("UnitType", StringType, true),
                                   StructField("UnitSequenceInCallDispatch", IntegerType,true),
                                   StructField("FirePreventionDistrict", StringType, true),
                                   StructField("SupervisorDistrict", StringType, true),
                                   StructField("Neighborhood", StringType, true),
                                   StructField("RowID", StringType, true),
                                   StructField("Delay", FloatType, true)))
// Read the file using the CSV DataFrameReader
val sfFireFile="/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"
val fireDF = spark.read.schema(fireSchema)
 .option("header", "true")
 .csv(sfFireFile)


// COMMAND ----------

// MAGIC %md
// MAGIC Si no desea especificar el esquema, Spark puede inferir el esquema de una muestra a un costo menor. Por ejemplo, puede utilizar el opci??n de relaci??n de muestreo:

// COMMAND ----------


val sampleDF = spark
 .read
 .option("samplingRatio", 0.001)
 .option("header", true)
 .csv("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC La funci??n spark.read.csv() lee el archivo CSV y devuelve un DataFrame de filas y columnas con nombre con los tipos dictados en el esquema.
// MAGIC Para escribir el DataFrame en una fuente de datos externa en el formato de su elecci??n, puede usar la interfaz DataFrameWriter. Al igual que DataFrameReader, admite m??ltiples fuentes de datos.

// COMMAND ----------

// MAGIC %md
// MAGIC Proyecciones y filtros. Una proyecci??n en el lenguaje relacional es una forma de devolver solo las filas que coinciden con una determinada condici??n relacional mediante el uso de filtros. En Spark, las proyecciones se realizan con el m??todo select(), mientras que los filtros se pueden expresar con el m??todo filter() o where().

// COMMAND ----------


val fewFireDF = fireDF
 .select("IncidentNumber", "AvailableDtTm", "CallType")
 .where($"CallType" =!= "Medical Incident") 
fewFireDF.show(5, false)


// COMMAND ----------

//??Cu??ntos CallTypes distintos se registraron como las causas de las llamadas de incendio?


import org.apache.spark.sql.functions._
fireDF
 .select("CallType")
 .where(col("CallType").isNotNull)
 .agg(countDistinct('CallType) as 'DistinctCallTypes)
 .show()

// COMMAND ----------

// MAGIC %md
// MAGIC Cambiar el nombre, agregar y eliminar columnas. A veces desea cambiar el nombre de columnas particulares por razones de estilo o convenci??n, y otras veces por legibilidad o brevedad. Los nombres de las columnas originales en el conjunto de datos del Departamento de Bomberos de SF ten??an espacios en ellos. Por ejemplo, el nombre de la columna IncidentNumber era Incident Number. Los espacios en los nombres de las columnas pueden ser problem??ticos, especialmente cuando desea escribir o guardar un DataFrame como un archivo Parquet (que lo proh??be).
// MAGIC Al especificar los nombres de las columnas deseadas en el esquema con StructField, como hicimos nosotros, cambiamos efectivamente todos los nombres en el DataFrame resultante.
// MAGIC Como alternativa, puede cambiar el nombre de las columnas de forma selectiva con el m??todo withColumnRenamed(). Por ejemplo, cambiemos el nombre de nuestra columna Delay a ResponseDelayedinMins y echemos un vistazo a los tiempos de respuesta que duraron m??s de cinco minutos:

// COMMAND ----------


val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
newFireDF
 .select("ResponseDelayedinMins")
 .where($"ResponseDelayedinMins" > 5)
 .show(5, false)


// COMMAND ----------

// MAGIC %md
// MAGIC Debido a que las transformaciones de DataFrame son inmutables, cuando cambiamos el nombre de una columna usando withColumnRenamed() obtenemos un nuevo DataFrame mientras conservamos el original con el nombre de columna anterior.

// COMMAND ----------

// In Scala
val fireTsDF = newFireDF
 .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
 .drop("CallDate")
 .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
 .drop("WatchDate")
 .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),"MM/dd/yyyy hh:mm:ss a"))
 .drop("AvailableDtTm")
// Select the converted columns
fireTsDF
 .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
 .show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC 1. Convierte el tipo de datos de la columna existente de cadena a una marca de tiempo compatible con Spark.
// MAGIC 2. Utilice el nuevo formato especificado en la cadena de formato "MM/dd/aaaa" o "MM/dd/aaaa hh:mm:ss a" seg??n corresponda.
// MAGIC 3. Despu??s de convertir al nuevo tipo de datos, suelte() la columna anterior y agregue la nueva especificada en el primer argumento al m??todo withColumn().
// MAGIC 4. Asigne el nuevo DataFrame modificado a fire_ts_df.

// COMMAND ----------

// MAGIC %md
// MAGIC defwithColumn(colName: String, col: Column): DataFrame
// MAGIC Devuelve un nuevo conjunto de datos agregando una columna o reemplazando la columna existente que tiene el mismo nombre.
// MAGIC 
// MAGIC La expresi??n de la columna solo debe hacer referencia a los atributos proporcionados por este conjunto de datos. Es un error agregar una columna que hace referencia a alg??n otro conjunto de datos.
// MAGIC Nota
// MAGIC este m??todo introduce una proyecci??n internamente. Por lo tanto, llamarlo varias veces, por ejemplo, a trav??s de bucles para agregar varias columnas, puede generar grandes planes que pueden causar problemas de rendimiento e incluso StackOverflowException. Para evitar esto, use seleccionar con varias columnas a la vez.

// COMMAND ----------

// MAGIC %md
// MAGIC to_timestamp(s: Columna, fmt: Cadena): Columna
// MAGIC Convierte la cadena de tiempo con el patr??n dado en una marca de tiempo.
// MAGIC 
// MAGIC s
// MAGIC Una fecha, marca de tiempo o cadena. Si es una cadena, los datos deben estar en un formato que se pueda convertir a una marca de tiempo, como aaaa-MM-dd o aaaa-MM-dd HH:mm:ss.SSSS
// MAGIC 
// MAGIC fmt
// MAGIC Un patr??n de fecha y hora que detalla el formato de s cuando s es una cadena
// MAGIC 
// MAGIC devoluciones
// MAGIC Una marca de tiempo, o nulo si s era una cadena que no se pod??a convertir a una marca de tiempo o fmt ten??a un formato no v??lido

// COMMAND ----------

fireDF
 .select($"WatchDate")
 .show()

// COMMAND ----------

// MAGIC %md
// MAGIC Ahora que hemos modificado las fechas, podemos consultar usando funciones de spark.sql.functions como month(), year() y day() para explorar m??s nuestros datos.
// MAGIC Podr??amos averiguar cu??ntas llamadas se registraron en los ??ltimos siete d??as, o podr??amos ver cu??ntos a??os de llamadas del Departamento de Bomberos se incluyen en el conjunto de datos con esta consulta:

// COMMAND ----------

// In Scala
fireTsDF
 .select(year($"IncidentDate"))
 .distinct()
 .orderBy(year($"IncidentDate"))
 .show()


// COMMAND ----------

??cu??les fueron los tipos m??s comunes de llamadas de incendios?

// COMMAND ----------


fireTsDF
 .select("CallType")
 .where(col("CallType").isNotNull)
 .groupBy("CallType")
 .count()
 .orderBy(desc("count"))
 .show(10, false)


// COMMAND ----------

// MAGIC %md
// MAGIC La API de DataFrame tambi??n ofrece el m??todo collect(), pero para DataFrames extremadamente grandes, esto consume muchos recursos (es caro) y es peligroso, ya que puede causar excepciones de falta de memoria (OOM).
// MAGIC A diferencia de count(), que devuelve un solo n??mero al controlador, collect() devuelve una colecci??n de todos los objetos Row en todo el DataFrame o Dataset. Si desea echar un vistazo a algunos registros de fila, es mejor que use take(n), que devolver?? solo los primeros n objetos de fila del marco de datos.

// COMMAND ----------

// MAGIC %md
// MAGIC Junto con todos los dem??s que hemos visto, la API de DataFrame proporciona m??todos estad??sticos descriptivos como min(), max(), sum() y avg(). Aqu?? calculamos la suma de alarmas, el tiempo de respuesta promedio y los tiempos de respuesta m??nimo y m??ximo para todas las llamadas de incendio en nuestro conjunto de datos.

// COMMAND ----------


import org.apache.spark.sql.{functions => F}
fireTsDF
 .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
 F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
 .show()

// COMMAND ----------

// MAGIC %md
// MAGIC Para necesidades estad??sticas m??s avanzadas comunes con cargas de trabajo de ciencia de datos, lea la documentaci??n de la API para m??todos como stat(), describe(), correlaci??n(), covarianza(), sampleBy(), approxQuantile(), commonItems(), etc.

// COMMAND ----------

fireTsDF
 .select($"NumAlarms")
 .show()

// COMMAND ----------

// MAGIC %md
// MAGIC Sale null/outher en algunas columnas, investigo el tipo de dato para ver porque no se ha cargado. O si me he equivocado al hacer el esquema.

// COMMAND ----------

// MAGIC %md
// MAGIC classIntegerType extends IntegralType
// MAGIC The data type representing Int values. Please use the singleton DataTypes.IntegerType.

// COMMAND ----------

// MAGIC %md
// MAGIC StructField(name: String, dataType: DataType, nullable: Boolean = true, metadata: Metadata = Metadata.empty)