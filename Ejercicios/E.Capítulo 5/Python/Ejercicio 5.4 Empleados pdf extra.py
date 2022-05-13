# Databricks notebook source
jdbcDF = (spark
 .read
 .format("jdbc")
 .option("url", "jdbc:mysql://localhost:3306/employees")
 .option("dbtable", "employees")
 .option("user", "root")
 .option("password", "123456")
 .load())