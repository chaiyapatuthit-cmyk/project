# Databricks notebook source
# DBTITLE 1,Import transform function
try:
    from unified_transform_logic.gold import transform
    print("Import from wheel package")
except ModuleNotFoundError:
    import sys
    
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    start_index = notebook_path.find("src")
    project_dir = notebook_path[:start_index]
    base_directory = f"/Workspace{project_dir}"
    src_path = f"{base_directory}logic_packages/src"
    
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    
    from unified_transform_logic.gold import transform
    print("Import from local package")

# COMMAND ----------

shop_name_df = spark.read.csv(f"file:{base_directory}src/module_12_framework/file_to_volume/shop_name_20251121.csv", header=True, sep="|")
fact_sales_df = spark.read.parquet(f"file:{base_directory}src/module_12_framework/file_to_volume/fact_sales_20260417.parquet")

# COMMAND ----------

from pyspark.sql import functions 
final_result_df = transform(sales_transaction_df=fact_sales_df, shop_dimension_df=shop_name_df)\
    .withColumn("compute_date", functions.current_timestamp())

# COMMAND ----------

try:
    target_catalog = dbutils.widgets.get("catalog")
except KeyError:
    target_catalog = "dev"

final_result_df.write.saveAsTable(f"{target_catalog}.gold.transform_sales_transaction", mode="overwrite", mergeSchema=True)
