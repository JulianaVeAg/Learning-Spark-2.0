// Databricks notebook source
// In Scala
case class DeviceTempByCountry(temp: Long, device_name: String, device_id: Long,
cca3: String)



// COMMAND ----------

val ds = spark.read
.json("/databricks-datasets/learning-spark-v2/iot-devices/iot_devices.json")
.as[DeviceTempByCountry]

// COMMAND ----------


val dsTemp = ds
.filter(d => {d.temp > 25})
.map(d => (d.temp, d.device_name, d.device_id, d.cca3))
.toDF("temp", "device_name", "device_id", "cca3")
.as[DeviceTempByCountry]
dsTemp.show(5, false)

// COMMAND ----------

val device = dsTemp.first()
println(device)

// COMMAND ----------

Alternativamente, puede expresar la misma consulta usando nombres de columna y luego convertir a un conjunto de datos [DeviceTempByCountry].

// COMMAND ----------

// In Scala
val dsTemp2 = ds
 .select($"temp", $"device_name", $"device_id", $"device_id", $"cca3")
 .where("temp > 25")
 .as[DeviceTempByCountry]
.show()


// COMMAND ----------

Semánticamente, select() es como map() en la consulta anterior, en el sentido de que ambas consultas seleccionan campos y generan resultados equivalentes.