# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

#bronze layer
raw_df = (
    spark.read.format("csv")
    .option("header",True)
    .option("delimiter","|")
    .load("/Volumes/workspace/session_11_framework/manual_file_folder/shop_name_20251121.csv")
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

raw_df.write.mode("overwrite").saveAsTable("session_11_framework.shop_name_bronze")
