# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
dbutils.notebook.run("/Workspace/Users/chaiyapat.uthit@gmail.com/project/resources/project/project_bronze_dynamic_fw.py",0,{"pipeline_name":"shop_name"})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/chaiyapat.uthit@gmail.com/project/resources/project/project_silver_dynamic_fw.py",0,{"pipeline_name":"shop_name"})

# COMMAND ----------

spark.table("session_11_framework.shop_name_bronze").display()
spark.table("session_11_framework.shop_name_silver").display()

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/chaiyapat.uthit@gmail.com/project/resources/project/project_bronze_dynamic_fw.py",0,{"pipeline_name":"fact_sales"})


# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/chaiyapat.uthit@gmail.com/project/resources/project/project_silver_dynamic_fw.py",0,{"pipeline_name":"fact_sales"})

# COMMAND ----------

spark.table("session_11_framework.fact_sales_bronze").display()
spark.table("session_11_framework.fact_sales_silver").display()
spark.table("session_11_framework.fact_sales_bad_rec").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

sales_fact_df = (spark.table("session_11_framework.fact_sales_silver")
                 .select("shop_id","sales_qty")
                 .filter(col("sales_date") == "2025-11-30")
                 )
shop_name_df = (spark.table("session_11_framework.shop_name_silver")
                .select("shop_id","shop_name")
                )

# COMMAND ----------

final_result_df = (sales_fact_df.alias("sales_fact")
                   .join(shop_name_df.alias("shop_name"),[col("sales_fact.shop_id") == col("shop_name.shop_id")],"left")
                   .select(
                       col("sales_fact.shop_id")
                       ,col("sales_fact.sales_qty")
                       ,col("shop_name.shop_name")
                       ,
                   )
                   .groupBy("shop_name")
                   .agg(sum("sales_qty").alias("total_sales"))
                   .filter(col("shop_name").isNotNull())
                   .select(
                       col("shop_name")
                       ,col("total_sales")
                   )
                   )
final_result_df.display()

# COMMAND ----------

final_result_df.write.mode("overwrite").saveAsTable("session_11_framework.final_result_gold")

# COMMAND ----------

spark.table("session_11_framework.final_result_gold").display()