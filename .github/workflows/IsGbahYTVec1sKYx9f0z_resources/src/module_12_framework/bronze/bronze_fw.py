# Databricks notebook source
# MAGIC %run ../fw

# COMMAND ----------

dbutils.widgets.text("pipeline_name","")
pipeline_name = dbutils.widgets.get("pipeline_name")

# COMMAND ----------

conf = spark.table("session_11_framework.config_table").filter(col("pipeline_name") == pipeline_name).first()

# COMMAND ----------

b = BronzeLayer(
    file_path = conf.file_path
    ,header = conf.header
    ,delimiter = conf.delimiter
    ,table_name = conf.table_name
)
bronze_df = b.read_from_file()
b.load_to_bronze_table(bronze_df)
