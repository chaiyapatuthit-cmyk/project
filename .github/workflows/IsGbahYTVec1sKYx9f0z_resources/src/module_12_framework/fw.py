# Databricks notebook source
from dataclasses import dataclass
from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

@dataclass
class BronzeLayer:
    file_path:str
    header:bool
    delimiter:str
    table_name:str

    def __post_init__(self) -> None:
        self.format_type = self.file_path.split('.')[-1]
        self.target_table_bronze = f'{self.table_name}_bronze'

    @classmethod
    def from_config_table(cls,pipeline_name:str) -> "BronzeLayer":
        conf = spark.table("session_11_framework.config_table").filter(col("pipeline_name") == pipeline_name).select("pipeline_name","pipeline_name","file_path","header","delimiter","table_name").first()
        return cls(
            pipeline_name = conf.pipeline_name
            ,file_path = conf.file_path
            ,header = conf.header
            ,delimiter = conf.delimiter
            ,table_name = conf.table_name
        )
        
    def read_from_file(self) -> DataFrame:
        df =  (
            spark.read.format(self.format_type)
            .option("header",self.header)
            .option("delimiter",self.delimiter)
            .load(self.file_path)
            )
        return (
            df
            .withColumn("_load_dt",current_date())
            .withColumn("_load_dttm",current_timestamp())
            .withColumn("_file_name",col("_metadata.file_name"))
            .withColumn("_file_path",col("_metadata.file_path"))
            .withColumn("_file_size",col("_metadata.file_size"))
            .withColumn("_file_mod",col("_metadata.file_modification_time"))
        )
    def load_to_bronze_table(self,raw_df:DataFrame) -> None:
        raw_df.write.mode("overwrite").saveAsTable(self.target_table_bronze)
        print(f"Table {self.target_table_bronze} loaded")

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

@dataclass
class SilverLayer:
    table_name:str
    schema_detail:dict[str,str]
    keys:list[str]
    write_mode:str

    def __post_init__(self) -> None:
        self.bronze_table_name = f'{self.table_name}_bronze'
        self.silver_table_name = f'{self.table_name}_silver'
        self.bad_record_table_name = f'{self.table_name}_bad_record'
        self.data_col = [col_name for col_name in self.schema_detail.keys()]
        self.invalid_rule = {"int":"^[0-9]+$" , "date":"^\\d{4}-\\d{2}-\\d{2}$"}
    
    @classmethod
    def from_config_table(cls,pipeline_name:str)-> 'SilverLayer':
        conf = spark.table("session_11_framework.config_table").filter(col("pipeline_name") == pipeline_name).select("pipeline_name","pipeline_name","table_name","schema_detail","keys","write_mode").first()
        return cls(
             pipeline_name = conf.pipeline_name
            ,table_name = conf.table_name
            ,schema_detail = conf.schema_detail
            ,keys = conf.keys
            ,write_mode = conf.write_mode
        )

    def read_add_sk_from_bronze_table(self) -> DataFrame:
        return (
            spark.table(self.bronze_table_name)
            .select(*self.data_col,monotonically_increasing_id().alias("_sk"))
            )

    def get_invalid_record(self,bronze_df:DataFrame) -> DataFrame:
        invalid_col = {
            f"_is_{col_name}_invalid":coalesce(~col(col_name).rlike(self.invalid_rule[col_type]),lit(False)) 
            for col_name,col_type in self.schema_detail.items() if col_type not in ["string"]
            }

        return (
            bronze_df
            .withColumns(invalid_col)
            .transform(get_reason)
            )
    
    def get_key_null_record(self,bronze_df:DataFrame) -> DataFrame:
        key_null_statement = { f'_is_{col_name}_null':col(col_name).isNull() for col_name in self.keys}
        
        return (
            bronze_df.withColumns(key_null_statement)
            .transform(get_reason)
            )
        
    def get_dup_record(self,bronze_df:DataFrame,key_null_df:DataFrame) -> DataFrame:
        partition_by_all = Window.partitionBy(*self.data_col).orderBy("_sk")
        partition_by_key = Window.partitionBy(*self.keys)

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
        return (
            is_row_duplicate_df
            .unionByName(is_key_duplicate_df)
        )

    def get_all_bad_record(self,invalid_df:DataFrame,key_null_df:DataFrame,duplicate_df:DataFrame) -> DataFrame:
        return (
            invalid_df
            .unionByName(key_null_df)
            .unionByName(duplicate_df)
            .groupBy(*self.data_col,"_sk")
            .agg(flatten(collect_list("reason")).alias("reason"))
            )
    
    def get_final_result(self,bronze_df:DataFrame,all_bad_df:DataFrame) -> DataFrame:
        add_control_col = {"load_dt":current_date(),"load_dttm":current_timestamp()}
        cast_statement = [ col(col_name).cast(col_type) for col_name,col_type in self.schema_detail.items()]
        return (
            bronze_df
            .join(all_bad_df,['_sk'],"left_anti")
            .select(cast_statement)
            .withColumns(add_control_col)
            )
    
    def load_bad_record(self,all_bad_df:DataFrame) -> None:
        (
            all_bad_df
            .write
            .mode("append")
            .saveAsTable(self.bad_record_table_name)
        )
        print(f"bad record loaded to {self.bad_record_table_name}")

    def load_to_silver_layer(self,final_result_df:DataFrame) -> None:
        (
            final_result_df
            .write
            .mode(self.write_mode)
            .saveAsTable(self.silver_table_name)
        )
        print(f"final result loaded to {self.silver_table_name}")
