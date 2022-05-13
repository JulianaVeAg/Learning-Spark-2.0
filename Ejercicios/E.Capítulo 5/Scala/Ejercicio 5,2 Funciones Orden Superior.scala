// Databricks notebook source
// Create DataFrame with two rows of two arrays (tempc1, tempc2)
val t1 = Array(35, 36, 32, 30, 40, 42, 38)
val t2 = Array(31, 32, 34, 55, 56)
val tC = Seq(t1, t2).toDF("celsius")
tC.createOrReplaceTempView("tC")
// Show the DataFrame
tC.show()


// COMMAND ----------

// MAGIC %md
// MAGIC transformar()
// MAGIC 
// MAGIC transformar(matriz<T>, función<T, U>): matriz<U>
// MAGIC 
// MAGIC La función transform() produce una matriz al aplicar una función a cada elemento de la matriz de entrada (similar a una función map()):

// COMMAND ----------

// In Scala/Python
// Calculate Fahrenheit from Celsius for an array of temperatures
spark.sql("""
SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit FROM tC """).show()

// COMMAND ----------

filtrar()

filtro(matriz<T>, función<T, booleana>): matriz<T>

La función filter() produce una matriz que consta solo de los elementos de la matriz de entrada para los que la función booleana es verdadera:

// COMMAND ----------

// Filter temperatures > 38C for array of temperatures
spark.sql(""" SELECT celsius, filter(celsius, t -> t > 38) as high FROM tC """).show()

// COMMAND ----------

existe()

existe(matriz<T>, función<T, V, Booleano>): Booleano

La función existe() devuelve verdadero si la función booleana se cumple para cualquier elemento en la matriz de entrada:

// COMMAND ----------

// Is there a temperature of 38C in the array of temperatures
spark.sql("""SELECT celsius, exists(celsius, t -> t = 38) as threshold FROM tC """).show()

// COMMAND ----------

reducir()
reducir(matriz<T>, B, función<B, T, B>, función<B, R>)

La función reduce() reduce los elementos de la matriz a un solo valor fusionando los elementos en un búfer B usando la función <B, T, B> y aplicando una función de acabado <B, R> en el búfer final:

// COMMAND ----------

// In Scala/Python
// Calculate average temperature and convert to F
spark.sql("""SELECT celsius, reduce(celsius, 0,(t, acc) -> t + acc, acc -> (acc div size(celsius) * 9 div 5) + 32) as avgFahrenheit FROM tC""").show()
