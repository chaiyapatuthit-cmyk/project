# Databricks notebook source
spark.sql("create schema if not exists workspace.session_11_framework")
spark.sql("create volume if not exists session_11_framework.manual_file_folder")

print("schema and volume session_11_framework.manual_file_folder is created successfully")

spark.sql("drop table session_11_framework.shop_name_bronze")
spark.sql("drop table session_11_framework.shop_name_silver")

# COMMAND ----------

def copy_file_to_volume(file_name) -> str:
    current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    current_path = "/".join(current_path.split("/")[0:-1])
    from_path = f"Workspace{current_path}/file_to_volume/{file_name}"
    to_path = f"/Volumes/workspace/session_11_framework/manual_file_folder/{file_name}"
    dbutils.fs.cp(from_path, to_path)
    print(f"copy file from {from_path} to {to_path} successfully")

copy_file_to_volume(file_name = "shop_name_20251121.csv")
copy_file_to_volume(file_name = "fact_sales_20260417.parquet")
