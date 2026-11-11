try:
    from unified_transform_logic.gold import transform
except ImportError:
    import os
    import sys
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from logic_packages.src.unified_transform_logic.gold import transform

import unittest
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType
from pyspark.sql.functions import col
from datetime import date
from pyspark.sql import SparkSession


class TestTierDiscount(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        try:
            cls.spark = SparkSession.builder \
                .master("local[1]") \
                .appName("unit-testing-unittest") \
                .getOrCreate()
        except:
            cls.spark = spark

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_apply_function(self):
        fact_sales_schema = StructType([
            StructField("transaction_id", IntegerType(), True),
            StructField("shop_id", IntegerType(), True),
            StructField("sales_qty", IntegerType(), True),
            StructField("sales_amt", FloatType(), True),
            StructField("sales_date", DateType(), True)
        ])
        fact_sales_raw = [(1, 1, 88, 5000.02, date(2025,1,1)),
                          (2, 1, 94, 318.44, date(2025,1,1)),
                          (2, 3, 32, 134.48, date(2025,1,1)),
                          (3, 2, 25, 8001.32, date(2025,12,1)),
                          (4, 2, 7, 205.12, date(2025,12,1)),
                          (5, 2, 98, 4959.73, date(2025,12,31)),
                          (6, 2, 100, 6000.25, date(2025,12,31))]
        fact_sales_df = self.spark.createDataFrame(fact_sales_raw, fact_sales_schema)

        shop_schema = StructType([
            StructField("shop_id", StringType(), True),
            StructField("shop_name", StringType(), True),
            StructField("branch_name", StringType(), True),
            StructField("file_dt", StringType(), True)
        ])
        shop_raw = [
            (1, "piti", "pharam2", date(2025,11,21)),
            (2, "valen", "bangyai", date(2025,11,21)),
            (3, "peter", "satupardit", date(2025,11,21))
        ]
        shop_df = self.spark.createDataFrame(shop_raw, shop_schema)

        expected_schema = StructType([
            StructField("sales_date", DateType(), True),
            StructField("sum_sales", IntegerType(), True),
            StructField("avg_sales", IntegerType(), True),
            StructField("bucket_count", IntegerType(), True),
            StructField("max_sales", FloatType(), True)
        ])

        expected_data = [
            (date(2025, 12, 31), 10959, 5479, 2, 6000.25),
            (date(2025, 12, 1), 8206, 4103, 2, 8001.32),
            (date(2025, 1, 1), 5452, 1817, 3, 5000.02)
        ]

        expected_result = self.spark.createDataFrame(expected_data, expected_schema).sort(col("sales_date")).collect()
        actual_result = transform(sales_transaction_df=fact_sales_df, shop_dimension_df=shop_df).sort(col("sales_date")).collect()

        self.assertEqual(first=actual_result, second=expected_result)

if __name__ == "__main__":
    unittest.main()


