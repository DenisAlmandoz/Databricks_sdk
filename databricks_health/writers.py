import pandas as pd
from pyspark.sql import DataFrame, SparkSession


class DeltaWriter:
    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema

    def write(self, table_name: str, df: DataFrame | pd.DataFrame):
        full_name = f"{self.catalog}.{self.schema}.{table_name}"
        if isinstance(df, pd.DataFrame):
            if df.empty:
                print(f"Skipping {full_name}: empty DataFrame")
                return
            spark_df = self.spark.createDataFrame(df)
        else:
            spark_df = df
        spark_df.write.mode("overwrite").saveAsTable(full_name)
        print(f"Wrote to {full_name}")
