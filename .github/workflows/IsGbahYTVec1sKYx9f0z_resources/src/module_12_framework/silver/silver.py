# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

bronze_df = (
    spark.table("session_11_framework.shop_name_bronze")
    .select(
         col("shop_id")
        ,col("shop_name")
        ,col("branch_name")
        ,col("file_dt")
        ,monotonically_increasing_id().alias("_sk")
        )
    )
bronze_df.display()

# COMMAND ----------

# invalid df
integer = "^[0-9]+$"
date = "^\\d{4}-\\d{2}-\\d{2}$"

invalid_df = (
    bronze_df
    .withColumn("_is_shop_id_invalid",coalesce(~col("shop_id").rlike(integer),lit(False)))
    .withColumn("_is_file_dt_invalid",coalesce(~col("file_dt").rlike(date),lit(False)))
    .filter(col("_is_shop_id_invalid") | col("_is_file_dt_invalid"))
    )

invalid_df = (
    invalid_df
    .melt(
         ids = ["shop_id","shop_name","branch_name","file_dt","_sk"]
        ,values = ["_is_shop_id_invalid","_is_file_dt_invalid"]
        ,variableColumnName= "reason"
        ,valueColumnName= "status"
        )
    .filter(col("status") == True)
    .groupBy("shop_id","shop_name","branch_name","file_dt","_sk")
    .agg(collect_list("reason").alias("reason"))
    )

invalid_df.display()

# COMMAND ----------

#is key null
key_null_df = (
    bronze_df.withColumn("_is_shop_id_null",col("shop_id").isNull())
    .melt(
        ids = ["shop_id","shop_name","branch_name","file_dt","_sk"]
        ,values = ["_is_shop_id_null"]
        ,variableColumnName= "reason"
        ,valueColumnName= "status"
        )
    .filter(col("status") == True)
    .groupBy("shop_id","shop_name","branch_name","file_dt","_sk")
    .agg(collect_list("reason").alias("reason"))
    )
key_null_df.display()

# COMMAND ----------

#is duplicate
partition_by_all = Window.partitionBy("shop_id","shop_name","branch_name","file_dt").orderBy("_sk")
partition_by_key = Window.partitionBy("shop_id")

bronze_not_null_df = bronze_df.filter(col("shop_id").isNotNull())
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
    .groupBy("shop_id","shop_name","branch_name","file_dt","_sk")
    .agg(flatten(collect_list("reason")).alias("reason"))
    )
bad_record_df.display()

# COMMAND ----------

final_result_df = (
    bronze_not_null_df
    .join(bad_record_df,['_sk'],"left_anti")
    .select(
         col("shop_id").cast("int")
        ,col("shop_name").cast("string")
        ,col("branch_name").cast("string")
        ,col("file_dt").cast("date")
        ,current_date().alias("load_dt")
        ,current_timestamp().alias("load_dttm")
        )
    )
final_result_df.display()

# COMMAND ----------

write_mode = "overwrite"
final_result_df.write.mode("overwrite").saveAsTable("session_11_framework.shop_name_silver")

# COMMAND ----------

spark.table("session_11_framework.shop_name_silver").display()
