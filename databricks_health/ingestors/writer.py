import pandas as pd
from pyspark.sql import SparkSession


class BronzeWriter:
    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "health",
        schema: str = "bronze",
    ):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema

    def write(self, table_name: str, df: pd.DataFrame):
        if df.empty:
            print(f"Skipping {self.catalog}.{self.schema}.{table_name}: empty DataFrame")
            return
        spark_df = self.spark.createDataFrame(df)
        full_name = f"{self.catalog}.{self.schema}.{table_name}"
        spark_df.write.mode("overwrite").saveAsTable(full_name)
        print(f"Wrote {len(df)} rows to {full_name}")
