// Databricks notebook source


// COMMAND ----------


Class.forName("org.mariadb.jdbc.Driver")

val jdbcHostname = "community.cloud.databricks.com:443/default"
val jdbcPort = 3306
val jdbcDatabase = "employees"
val user = "root"
val p = "123456"
// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"${user}")
connectionProperties.put("password", s"${p}")


import java.sql.DriverManager
val connection = DriverManager.getConnection(jdbcUrl, user, p)
connection.isClosed()

// COMMAND ----------



// COMMAND ----------

 val employeesDF = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/employees").option("driver","com.mysql.jdbc.Driver").option("dbtable","employees").option("user","root").option("password","123456")
.load()