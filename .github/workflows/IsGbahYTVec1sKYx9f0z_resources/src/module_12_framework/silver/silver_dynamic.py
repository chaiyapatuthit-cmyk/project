# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

table_name = "session_11_framework.shop_name"
bronze_table_name = "session_11_framework.shop_name_bronze"
silver_table_name = "session_11_framework.shop_name_silver"
bad_record_table_name = "session_11_framework.shop_name_bad_record"

schema_detail = {"shop_id": "int","shop_name": "string","branch_name": "string","file_dt": "date"}
data_col = [col_name for col_name in schema_detail.keys()]
keys = ['shop_id']
invalid_rule = {"int":"^[0-9]+$" , "date":"^\\d{4}-\\d{2}-\\d{2}$"}
write_mode = "overwrite"

# COMMAND ----------

bronze_df = (
    spark.table(bronze_table_name)
    .select(*data_col,monotonically_increasing_id().alias("_sk"))
    )
bronze_df.display()

# COMMAND ----------

def get_reason(df:DataFrame) -> DataFrame:
    control_col = [col_name for col_name in df.columns if col_name.startswith("_") and col_name != "_sk"]
    data_col = [col_name for col_name in df.columns if not col_name.startswith("_")]
    or_statement = " OR ".join([col_name for col_name in control_col])
    return (
        df
        .filter(or_statement)
        .melt(
            ids = [*data_col,"_sk"]
            ,values = control_col
            ,variableColumnName= "reason"
            ,valueColumnName= "status"
            )
        .filter(col("status") == True)
        .groupBy(*data_col,"_sk")
        .agg(collect_list("reason").alias("reason"))
        )

# COMMAND ----------

invalid_col = {
    f"_is_{col_name}_invalid":coalesce(~col(col_name).rlike(invalid_rule[col_type]),lit(False)) 
    for col_name,col_type in schema_detail.items() if col_type not in ["string"]
    }

invalid_df = (
    bronze_df
    .withColumns(invalid_col)
    .transform(get_reason)
    )
invalid_df.display()

# COMMAND ----------

#is key null
key_null_statement = { f'_is_{col_name}_null':col(col_name).isNull() for col_name in keys}

key_null_df = (
    bronze_df.withColumns(key_null_statement)
    .transform(get_reason)
    )
key_null_df.display()

# COMMAND ----------

#is duplicate

partition_by_all = Window.partitionBy(*data_col).orderBy("_sk")
partition_by_key = Window.partitionBy(*keys)

bronze_not_null_df = bronze_df.join(key_null_df,['_sk'],"left_anti")

is_row_duplicate_df = (
    bronze_not_null_df
    .withColumn("rn",row_number().over(partition_by_all))
    .filter(col("rn") > 1)
    .drop("rn")
    .withColumn("reason",array(lit("_row_duplicate")))
    )

is_key_duplicate_df = (
    bronze_not_null_df
    .join(is_row_duplicate_df,['_sk'],"left_anti")
    .withColumn("count",count("*").over(partition_by_key))
    .filter(col("count") > 1)
    .drop("count")
    .withColumn("reason",array(lit("_key_duplicate")))
)
duplicate_df = (
    is_row_duplicate_df
    .unionByName(is_key_duplicate_df)
)

duplicate_df.display()

# COMMAND ----------

#combine bad record
bad_record_df = (
    invalid_df
    .unionByName(key_null_df)
    .unionByName(duplicate_df)
    .groupBy(*data_col,"_sk")
    .agg(flatten(collect_list("reason")).alias("reason"))
    )
bad_record_df.display()

# COMMAND ----------

add_control_col = {"load_dt":current_date(),"load_dttm":current_timestamp()}
cast_statement = [ col(col_name).cast(col_type) for col_name,col_type in schema_detail.items()]
final_result_df = (
    bronze_df
    .join(bad_record_df,['_sk'],"left_anti")
    .select(cast_statement)
    .withColumns(add_control_col)
    )
final_result_df.display()

# COMMAND ----------

final_result_df.write.mode(write_mode).saveAsTable(silver_table_name)
bad_record_df.write.mode(write_mode).saveAsTable(bad_record_table_name)
