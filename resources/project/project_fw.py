# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from dataclasses import dataclass
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (col, current_date, current_timestamp, collect_list, monotonically_increasing_id,
    coalesce, lit, row_number, array, count, flatten)
from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor
import os

# COMMAND ----------

def clear_folder(folder_name:str) -> None:
    dbutils.fs.rm(f'/Volumes/session_11_streaming/default/{folder_name}/',recurse=True)
    dbutils.fs.mkdirs(f'/Volumes/session_11_streaming/default/{folder_name}/')
    print(f'clear /Volumes/session_11_streaming/default/{folder_name}/ successfully')

def clear_all() -> None:
    with ThreadPoolExecutor(max_workers=6) as executor:
        executor.map(clear_folder,["source","checkpoint","schemalocation","archive"])

# COMMAND ----------

@dataclass
class BronzeLayer:
    header: bool
    delimiter: str
    file_path: str 
    table_name: str
   
    def __post_init__(self) -> None:
        self.format_type = self.file_path.split('_')[-1]
        self.target_table_bronze = f'{self.table_name}_bronze'

    @classmethod
    def from_config_table(cls,pipeline_name:str) -> "BronzeLayer":
        conf = (
            spark.table("session_11_framework.config_table")
            .filter(col("pipeline_name") == pipeline_name)
            .select("header","delimiter","file_path","table_name")
            .first()
            )
        return cls(header = conf.header
                    ,delimiter = conf.delimiter
                    ,file_path = conf.file_path
                    ,table_name = conf.table_name
                    )        

    def read_from_file(self) -> DataFrame:
        sample_schema = (
            spark.read
            .format(self.format_type)
            .option("header", self.header)
            .option("delimiter", self.delimiter)
            .load(self.file_path)
            .schema
        )

        df = (
            spark.readStream
            .format(self.format_type)
            .schema(sample_schema)
            .option("header", self.header)
            .option("delimiter", self.delimiter) 
            .load(self.file_path)
            .withColumn("_load_dt", current_date())
            .withColumn("_load_dttm", current_timestamp())
            .withColumn("_file_name", col("_metadata.file_name"))
            .withColumn("_file_path", col("_metadata.file_path"))
            .withColumn("_file_size", col("_metadata.file_size"))
            .withColumn("_file_mod", col("_metadata.file_modification_time"))
     )

        return (
            df.writeStream
            .outputMode("append")
            .option("checkpointLocation", f"/Volumes/session_11_streaming/default/checkpoint/{self.table_name}")
            .trigger(availableNow=True)
            .table(self.target_table_bronze)
        )


# COMMAND ----------

def get_reason (df:DataFrame) -> DataFrame:
    control_col =[col_name for col_name in df.columns if col_name.startswith("_") and col_name != "_sk"]
    data_col =[col_name for col_name in df.columns if not col_name.startswith("_") or col_name == "_sk"]
    or_statement = " or ".join(control_col)

    return (
        df
        .filter(or_statement)
        .melt(
            ids = data_col
            ,values = control_col
            ,variableColumnName = "reason"
            ,valueColumnName = "status"
         )
        .filter(col("status") == True)
        .groupBy(data_col)
        .agg(collect_list(col("reason")).alias("reason"))
        
    )    
    

# COMMAND ----------

@dataclass
class SilverLayer:
    table_name: str
    schema_detail:dict[str,str]
    keys:list[str]
    mode_type:str

    def __post_init__(self)-> None:
        self.bronze_table_name = f"{self.table_name}_bronze"
        self.silver_table_name = f"{self.table_name}_silver"
        self.bad_table_name = f"{self.table_name}_bad_rec"
        self.data_col = list(self.schema_detail.keys())

    @classmethod
    def from_config_table(cls,pipeline_name:str) -> "SilverLayer":
        conf = (
            spark.table("session_11_framework.config_table")
            .filter(col("pipeline_name") == pipeline_name)
            .select("table_name","schema_detail","keys","write_mode")
            .first()
            )
        return cls(table_name = conf.table_name
                    ,schema_detail = conf.schema_detail
                    ,keys = conf.keys
                    ,mode_type = conf.write_mode
                    )        

    def read_from_bronze_sk(self) -> DataFrame:
        return (
            spark.table(self.bronze_table_name)
            .filter(col("_load_dt") == current_date())
            .select(
                monotonically_increasing_id().alias("_sk")
                ,*self.data_col
                )
            )
        
    def get_invalid_df(self,bronze_df:DataFrame) -> DataFrame:
        invalid_rules = {
            "int":"^[0-9]+$"
            ,"date":"^\\d{4}-\\d{2}-\\d{2}$"
        }        
        invalid_statement = {f"_is_{col_name}_invalid" : ~coalesce(col(col_name).rlike(invalid_rules[col_type]),lit(False)) for col_name,col_type in self.schema_detail.items() if col_type != "string"}

        return (
            bronze_df
            .withColumns(invalid_statement)
            .transform(get_reason)
        )

    def get_key_null_df(self,bronze_df:DataFrame) -> DataFrame:
        keys_null_statement = {f"_is_{col_name}_Null" :col(col_name).isNull() for col_name in self.keys}

        return (
            bronze_df
            .withColumns(keys_null_statement)
            .transform(get_reason)
        )

    def get_dup_df(self,bronze_df:DataFrame) -> DataFrame:
        partition_by_all = Window.partitionBy(*self.data_col).orderBy("_sk")
        partition_by_key = Window.partitionBy(*self.keys)
        key_null_df = self.get_key_null_df(bronze_df)

        bronze_key_not_null_df = bronze_df.join(key_null_df, ["_sk"],"left_anti")

        row_dup_df = (
            bronze_df
            .withColumn("rn",row_number().over(partition_by_all))
            .filter(col("rn") > 1)
            .withColumn("reason",array(lit("_row_dup")))
            .drop("rn")
            )

        key_dup_df = (
            bronze_key_not_null_df
            .join(row_dup_df,["_sk"],"left_anti")
            .withColumn("rn",count("*").over(partition_by_key))
            .filter(col("rn") > 1)
            .withColumn("reason",array(lit("_key_dup")))
            .drop("rn")
                )

        return row_dup_df.unionByName(key_dup_df)
    
    def union_all_bad_rec(self,invalid_df:DataFrame,key_null_df:DataFrame,dup_df:DataFrame) -> DataFrame:
        return(
        invalid_df
            .unionByName(key_null_df)
            .unionByName(dup_df)
            .groupBy("_sk",*self.data_col)
            .agg(flatten(collect_list(col("reason"))).alias("reason"))
            )
        
    def get_final_result(self,bronze_df:DataFrame,all_bad_rec_df:DataFrame)-> DataFrame:
        cast_statement = [col(col_name).cast(col_type).alias(col_name) for col_name,col_type in self.schema_detail.items()]

        return (
            bronze_df
            .join(all_bad_rec_df,["_sk"],"left_anti")
            .select(
                    *cast_statement
                    ,current_date().alias("_load_dt")
                    ,current_timestamp().alias("_load_dttm")
                    )
            )

    def load_bad_rec_to_table(self,bad_rec_df:DataFrame) -> None:
        bad_rec_df.write.mode("overwrite").saveAsTable(self.bad_table_name)
        print(f"Data written to {self.bad_table_name}")       
      
    def load_to_silver_table(self,final_result_df:DataFrame) -> None:
        (
            final_result_df
            .write
            .mode(self.mode_type)
            .saveAsTable(self.silver_table_name)
        )
        print(f"Data written to {self.silver_table_name}")
      