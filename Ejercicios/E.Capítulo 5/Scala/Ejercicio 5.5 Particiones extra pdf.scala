// Databricks notebook source
//Cargar con spark datos de empleados y departamentos

val employeesDF = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/employees").option("driver","com.mysql.jdbc.Driver").option("dbtable","employees").option("user","root").option("password","123456").load()


val salariesDF =spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/employees").option("driver","com.mysql.jdbc.Driver").option("dbtable","salaries").option("user","root").option("password","123456").load()

val titles =  spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/employees").option("driver","com.mysql.jdbc.Driver").option("dbtable","titles").option("user","root").option("password","123456").load()


 val dept_noDF = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/employees").option("driver","com.mysql.jdbc.Driver").option("dbtable","dept_emp").option("user","root").option("password","123456").load()

val departments = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/employees").option("driver","com.mysql.jdbc.Driver").option("dbtable","departments").option("user","root").option("password","123456").load()



// COMMAND ----------

val window2 = Window.partitionBy("emp_no")



val salActual1 = salariesDF.withColumn("row",row_number.over(window2)).withColumn("maxSalary",max(col("salary")).over(window2)).where(col("row")===1).select("emp_no","salary","maxSalary").show()

// COMMAND ----------

val w1 = Window.partitionBy("emp_no").orderBy("con.to_date")



val prueba = employeesDF.alias("emp").join(salariesDF.as("sal"), "emp_no").join(titles,"emp_no").join(dept_noDF.as("con"),"emp_no").join(departments,"dept_no").withColumn("rank",rank().over(w1)).withColumn("maxrank",max("rank").over(w1)).withColumn("sumSalary", sum("salary").over(w1)).select("first_name","sumSalary","dept_name","con.from_date","con.to_date","rank", "maxrank","title").distinct().where("rank == maxrank").show(60)

// COMMAND ----------

val w1 = Window.partitionBy("emp_no").orderBy("con.to_date")



val prueba = employeesDF.alias("emp")
.join(salariesDF.as("sal"), "emp_no")
.join(title,"emp_no")
.join(dept_empDF.as("con"),"emp_no")
.join(departmentsDF,"dept_no")
.withColumn("rank",rank().over(w1))
.withColumn("maxrank",max("rank").over(w1))
.withColumn("sumSalary", sum("salary").over(w1))
.select("first_name","sumSalary","dept_name","con.from_date","con.to_date","rank", "maxrank","title").distinct()
.where("rank == maxrank")
.show(60)