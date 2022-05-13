// Databricks notebook source
case class DeviceIoTData (battery_level: Long, c02_level: Long, cca2: String, cca3: String, cn: String, device_id: Long, device_name: String, humidity: Long, ip: String, latitude: Double, lcd: String, longitude: Double, scale:String, temp: Long, timestamp: Long)


// COMMAND ----------


val ds = spark.read
.json("/databricks-datasets/learning-spark-v2/iot-devices/iot_devices.json")
.as[DeviceIoTData]

ds.show(5, false)


// COMMAND ----------


val filterTempDS = ds.filter({d => {d.temp > 30 && d.humidity > 70}})
filterTempDS.show(5, false)


// COMMAND ----------

// MAGIC %md
// MAGIC En esta consulta, usamos una función como argumento para el método filter() del conjunto de datos.
// MAGIC Este es un método sobrecargado con muchas firmas. La versión que usamos, filter(func: (T) > Boolean): Dataset[T], toma una función lambda, func: (T) >Boolean, como argumento.
// MAGIC El argumento de la función lambda es un objeto JVM de tipo DeviceIoTData. Como tal, podemos acceder a sus campos de datos individuales usando la notación de punto (.), como lo haría en una clase Scala o JavaBean.

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Detectar dispositivos defectuosos con niveles de batería por debajo de un umbral.

// COMMAND ----------

val filtro = ds.filter(d => d.battery_level < 1)
filtro.show()

// COMMAND ----------

// MAGIC %md
// MAGIC   2. Identificar países infractores con altos niveles de emisiones de CO2.

// COMMAND ----------

val filtro = ds.filter(d => d.c02_level > 1400)
filtro.show()

// COMMAND ----------

3. Calcule los valores mínimo y máximo de temperatura, nivel de batería, CO2 y humedad.

// COMMAND ----------

import org.apache.spark.sql.functions._

ds.select(min("temp").alias("min_temp"),
          max("temp").alias("max_temp"),
          min("battery_level").alias("min_bateria"),
          max("battery_level").alias("max_bateria"),
          min("c02_level").alias("min_co2"),
          max("c02_level").alias("min_co2"),
          min("humidity").alias("min_humidity"),
          max("humidity").alias("max_humidity"))
                                                .show()


// COMMAND ----------

4. Ordene y agrupe por temperatura promedio, CO2, humedad y país.

// COMMAND ----------

defsort(sortExprs: Columna*): Conjunto de datos[T]
Devuelve un nuevo conjunto de datos ordenado por las expresiones dadas. Por ejemplo:

ds.sort($"col1", $"col2".desc)

// COMMAND ----------



ds.groupBy("cn").avg("temp", "c02_level", "humidity").show()