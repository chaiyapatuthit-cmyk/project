# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./project_fw

# COMMAND ----------

dbutils.widgets.text("pipeline_name","")
pipeline_name = dbutils.widgets.get("pipeline_name")

# COMMAND ----------

s = SilverLayer.from_config_table(pipeline_name)

# COMMAND ----------

bronze_df = s.read_from_bronze_sk()
invalid_df = s.get_invalid_df(bronze_df)
key_null_df = s.get_key_null_df(bronze_df)
dup_df = s.get_dup_df(bronze_df)
bad_rec_df = s.union_all_bad_rec(invalid_df,key_null_df,dup_df)
final_result = s.get_final_result(bronze_df,bad_rec_df)

s.load_bad_rec_to_table(bad_rec_df)
s.load_to_silver_table(final_result)