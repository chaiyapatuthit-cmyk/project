# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

file_path = "/Volumes/workspace/session_11_framework/manual_file_folder/shop_name_20251121.csv"
header = True
delimeter = "|"
format_type = "csv"
target_table = "session_11_framework.shop_name"
target_table_bronze = "session_11_framework.shop_name_bronze"

# COMMAND ----------

#bronze layer
raw_df = (
    spark.read.format(format_type)
    .option("header",header)
    .option("delimiter",delimeter)
    .load(file_path)
    )

raw_df = (
    raw_df
    .withColumn("_load_dt",current_date())
    .withColumn("_load_dttm",current_timestamp())
    .withColumn("_file_name",col("_metadata.file_name"))
    .withColumn("_file_path",col("_metadata.file_path"))
    .withColumn("_file_size",col("_metadata.file_size"))
    .withColumn("_file_mod",col("_metadata.file_modification_time"))
    )
raw_df.display()

# COMMAND ----------

raw_df.write.mode("overwrite").saveAsTable("target_table_bronze")
