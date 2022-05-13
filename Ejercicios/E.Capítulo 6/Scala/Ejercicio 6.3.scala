// Databricks notebook source
// In Scala
case class Person(id: Integer, firstName: String, middleName: String, lastName: String,gender: String, birthDate: String, ssn: String, salary: String)


// COMMAND ----------

import java.util.Calendar
val earliestYear = Calendar.getInstance.get(Calendar.YEAR) - 40




 // Everyone above 40: lambda-1
 personDS.filter(x => x.birthDate.split("-")(0).toInt > earliestYear)
 
 // Everyone earning more than 80K
 personDS.filter($"salary" > 80000)
