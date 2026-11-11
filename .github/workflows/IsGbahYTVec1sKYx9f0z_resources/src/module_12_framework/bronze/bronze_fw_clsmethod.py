# Databricks notebook source
# MAGIC %run ../fw

# COMMAND ----------

dbutils.widgets.text("pipeline_name","")
pipeline_name = dbutils.widgets.get("pipeline_name")

# COMMAND ----------

b = BronzeLayer.from_config_table("shop_name")

bronze_df = b.read_from_file()

b.load_to_bronze_table(bronze_df)
