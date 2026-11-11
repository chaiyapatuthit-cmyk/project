from pyspark.sql import DataFrame
from pyspark.sql.functions import floor, count, avg, sum, max

def transform(sales_transaction_df: DataFrame, 
              shop_dimension_df: DataFrame) -> DataFrame:
    result_df = sales_transaction_df.join(shop_dimension_df,['shop_id'],"left")\
        .groupBy("sales_date")\
        .agg(floor(sum("sales_amt")).alias("sum_sales"),
             floor(avg("sales_amt")).alias("avg_sales"),
             count("sales_amt").alias("bucket_count"),
             max("sales_amt").alias("max_sales"))
    return result_df
