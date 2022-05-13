// Databricks notebook source
Creamos dinámicamente un objeto Scala con tres campos: uid (identificación única para un usuario), uname (cadena de nombre de usuario generada aleatoriamente) y uso (minutos de uso del servidor o servicio).

// COMMAND ----------

import scala.util.Random._

// Our case class for the Dataset

case class Usage(uid:Int, uname:String, usage: Int)

val r = new scala.util.Random(42)

// Create 1000 instances of scala Usage class
// This generates data on the fly

val data = for (i <- 0 to 1000)yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""),r.nextInt(1000)))

// Create a Dataset of Usage typed data

val dsUsage = spark.createDataset(data)

dsUsage.show(10)


// COMMAND ----------

// In Scala
import org.apache.spark.sql.functions._
dsUsage
 .filter(d => d.usage > 900)
 .orderBy(desc("usage"))
 .show(5, false)



// COMMAND ----------

//Otra forma de hacerlo

def filterWithUsage(u: Usage) = u.usage > 900
dsUsage.filter(filterWithUsage(_)).orderBy(desc("usage")).show(5)

// COMMAND ----------

En el primer caso usamos una expresión lambda, {d.usage > 900}, como argumento para el método filter(), mientras que en el segundo caso definimos una función Scala, def filterWithUsage(u: Usage) = u.usage > 900. En ambos casos, el método filter() itera sobre cada fila del objeto Usage en el conjunto de datos distribuido y aplica la expresión o ejecuta la función, devolviendo un nuevo conjunto de datos de tipo Usage para las filas donde el valor de la expresión o función es verdad.
No todas las lambdas o argumentos funcionales deben evaluar valores booleanos; también pueden devolver valores calculados. Considere este ejemplo utilizando la función de orden superior map(), donde nuestro objetivo es averiguar el costo de uso para cada usuario cuyo valor de uso supera un cierto umbral para que podamos ofrecerles a esos usuarios un precio especial por minuto.

// COMMAND ----------

// In Scala
// Use an if-then-else lambda expression and compute a value

dsUsage.map(u => {if (u.usage > 750) u.usage * .15 else u.usage * .50 })
 .show(5, false)



// COMMAND ----------

// Define a function to compute the usage

def computeCostUsage(usage: Int): Double = {
 if (usage > 750) usage * 0.15 else usage * 0.50
}
// Use the function as an argument to map()
dsUsage.map(u => {computeCostUsage(u.usage)}).show(5, false)

// COMMAND ----------

// In Scala
// Create a new case class with an additional field, cost
case class UsageCost(uid: Int, uname:String, usage: Int, cost: Double)


// COMMAND ----------

// Compute the usage cost with Usage as a parameter
// Return a new object, UsageCost
def computeUserCostUsage(u: Usage): UsageCost = {
 val v = if (u.usage > 750) u.usage * 0.15 else u.usage * 0.50
 UsageCost(u.uid, u.uname, u.usage, v)
}

// COMMAND ----------

// Use map() on our original Dataset
dsUsage.map(u => {computeUserCostUsage(u)}).show(5)

// COMMAND ----------

Ahora tenemos un conjunto de datos transformado con una nueva columna, cost, calculado por la función en nuestra transformación map(), junto con todas las demás columnas.