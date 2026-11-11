# Databricks notebook source
# MAGIC %run ../fw

# COMMAND ----------

dbutils.widgets.text("pipeline_name","")
pipeline_name = dbutils.widgets.get("pipeline_name")

# COMMAND ----------

s = SilverLayer.from_config_table(pipeline_name)

bronze_df = s.read_add_sk_from_bronze_table()
invalid_df = s.get_invalid_record(bronze_df)
key_null_df = s.get_key_null_record(bronze_df)
dup_df = s.get_dup_record(bronze_df,key_null_df)
all_bad_df = s.get_all_bad_record(invalid_df,key_null_df,dup_df)
final_result_df = s.get_final_result(bronze_df,all_bad_df)

s.load_bad_record(all_bad_df)
s.load_to_silver_layer(final_result_df)
