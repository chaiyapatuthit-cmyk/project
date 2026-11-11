# Databricks notebook source
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table session_11_framework.config_table (
# MAGIC     pipeline_name string
# MAGIC   , file_path string
# MAGIC   , header string
# MAGIC   , delimiter string
# MAGIC   , table_name string
# MAGIC   , schema_detail map<string,string>
# MAGIC   , keys array<string>
# MAGIC   , write_mode string
# MAGIC )

# COMMAND ----------

def upsert_into(df:DataFrame,table_name:str,keys:list) -> DataFrame:
    delta_obj = DeltaTable.forName(spark,table_name)
    return (
        delta_obj.alias("t").merge(
            df.alias("s")," AND ".join([f"t.{key} = s.{key}" for key in keys])
            )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
        )

# COMMAND ----------

data = [{
    "pipeline_name": "shop_name",
    "file_path": "dbfs:/Volumes/workspace/session_11_framework/manual_file_folder/shop_name_20251121.csv",
    "header": "true",
    "delimiter": "|",
    "table_name": "session_11_framework.shop_name",
    "schema_detail": {"shop_id": "int", "shop_name": "string", "branch_name": "string", "file_dt": "date"},
    "keys": ["shop_id"],
    "write_mode": "overwrite"
}]

schema = StructType([
    StructField("pipeline_name", StringType(), True),
    StructField("file_path", StringType(), True),
    StructField("header", StringType(), True),
    StructField("delimiter", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("schema_detail", MapType(StringType(), StringType()), True),
    StructField("keys", ArrayType(StringType()), True),
    StructField("write_mode", StringType(), True)
])

mock_df = spark.createDataFrame(data, schema)
upsert_into(mock_df,"session_11_framework.config_table",["pipeline_name"])

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType

data = [{
    "pipeline_name": "fact_sales",
    "file_path": "/Volumes/workspace/session_11_framework/manual_file_folder/fact_sales_20260417.parquet",
    "header": None,
    "delimiter":None,
    "table_name": "session_11_framework.fact_sales",
    "schema_detail": {"transaction_id": "int","shop_id": "int","sales_qty": "int","sales_amt": "string","sales_date": "date"},
    "keys": ["transaction_id","shop_id"],
    "write_mode": "overwrite"
}]

schema = StructType([
    StructField("pipeline_name", StringType(), True),
    StructField("file_path", StringType(), True),
    StructField("header", StringType(), True),
    StructField("delimiter", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("schema_detail", MapType(StringType(), StringType()), True),
    StructField("keys", ArrayType(StringType()), True),
    StructField("write_mode", StringType(), True)
])

mock_df = spark.createDataFrame(data, schema)
upsert_into(mock_df,"session_11_framework.config_table",["pipeline_name"])
