# Databricks notebook source
dbutils.notebook.run("/Workspace/bootcamp_2/module_11_framework/bronze/bronze_fw",0,{"pipeline_name":"shop_name"})
dbutils.notebook.run("/Workspace/bootcamp_2/module_11_framework/silver/silver_fw",0,{"pipeline_name":"shop_name"})

# COMMAND ----------

dbutils.notebook.run("/Workspace/bootcamp_2/module_11_framework/bronze/bronze_fw",0,{"pipeline_name":"fact_sales"})
dbutils.notebook.run("/Workspace/bootcamp_2/module_11_framework/silver/silver_fw",0,{"pipeline_name":"fact_sales"})
