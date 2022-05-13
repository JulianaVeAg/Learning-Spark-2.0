// Databricks notebook source

variables de entorno: MYSQL_HOME y el PATH (en el path poner la carpeta bin)


despues en la consola (cmd) cd a la carpeta donde esta el sql

y despues escribir mysql -t < employees.sql (si dice que permiso denegado entonces escribir usuario y contraseña que seria -u"usuario" -p"contraseña")



spark-shell C:\Spark\spark-3.0.3-bin-hadoop2.7\jars\mysql-connector-java-8.0.29.jar



 employeesDF.join(salariesDF,employeesDF("emp_no") === salariesDF("emp_no"),"left").show(false)

// COMMAND ----------

//Cargar con spark datos de empleados y departamentos

 val employeesDF = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/employees").option("driver","com.mysql.jdbc.Driver").option("dbtable","employees").option("user","root").option("password","123456")
.load()

val salariesDF =spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/employees").option("driver","com.mysql.jdbc.Driver").option("dbtable","salaries").option("user","root").option("password","123456").load()


val titles =  spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/employees").option("driver","com.mysql.jdbc.Driver").option("dbtable","titles").option("user","root").option("password","123456").load()




//Join a las 3 tablas 


val join33 = employeesDF.join(salariesDF,employeesDF("emp_no") === salariesDF("emp_no"),"inner").join (titles,employeesDF("emp_no") === titles("emp_no"),"inner").select(employeesDF("*"),salariesDF("salary"),titles("title"))

join33.show()

// COMMAND ----------

val all = employeesDF.join(salariesDF,employeesDF("emp_no") === salariesDF("emp_no"),"inner").join (titles,employeesDF("emp_no") === titles("emp_no"),"inner").join(dept_noDF,employeesDF("emp_no")=== dept_noDF("emp_no"),"inner").select(employeesDF("emp_no"),salariesDF("salary"),titles("title"),dept_noDF("dept_no"))

// COMMAND ----------

val all2 = all.join(departments,all("dept_no") === departments("dept_no"),"inner").select(employeesDF("emp_no"),salariesDF("salary"),titles("title"),departments("dept_name"))

// COMMAND ----------

val dept_noDF = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/employees").option("driver","com.mysql.jdbc.Driver").option("dbtable","dept_emp").option("user","root").option("password","123456").load()

val departments = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/employees").option("driver","com.mysql.jdbc.Driver").option("dbtable","departments").option("user","root").option("password","123456").load()


// COMMAND ----------

//Mediante Joins mostrar toda la información de los empleados además de su título y salario


 val empSal= employeesDF.join(salariesDF,employeesDF("emp_no") === salariesDF("emp_no"),"left_outer").select(employeesDF("emp_no"),employeesDF("birth_date"),employeesDF("first_name"),employeesDF("last_name"),employeesDF("gender"),employeesDF("hire_date"),salariesDF("salary"))



empSal.show()

val empSalTit = empSal.join(titles,empSal("emp_no") === titles("emp_no"),"left_outer").select(empSal("emp_no"),empSal("birth_date"),empSal("first_name"),empSal("last_name"),empSal("gender"),empSal("hire_date"),empSal("salary"),titles("title"))


//Forma resumida

val empSalTit2 = empSal.join(titles,empSal("emp_no") === titles("emp_no"),"left_outer").select(empSal("*"),titles("title"))