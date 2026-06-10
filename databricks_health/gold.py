from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, sum as spark_sum, avg, count, when

from databricks_health.writers import DeltaWriter


class GoldAggregationOrchestrator:
    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "health_catalog",
    ):
        self.spark = spark
        self.catalog = catalog
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        for schema_name in ["bronze", "silver", "gold"]:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
        self.writer = DeltaWriter(spark, catalog, "gold")

    def _read(self, table: str):
        return self.spark.table(f"{self.catalog}.silver.{table}")

    def run(self):
        infra = self._read("infra_health")
        pipeline = self._read("pipeline_health")
        dlt = self._read("dlt_health")
        tables = self._read("table_accessibility")
        query_history = self._read("query_history")

        infra_agg = infra.groupBy().agg(
            (spark_sum("monitor_startup_success") / spark_sum("monitor_startup_attempts"))
            .alias("cluster_startup_success_rate"),
            spark_sum("monitor_startup_failures").alias("cluster_startup_failure_count"),
            avg("cluster_age_hours").alias("avg_cluster_age_hours"),
            spark_sum(when(col("state") == "ERROR", 1).otherwise(0)).alias("cluster_error_count"),
        )

        pipeline_agg = pipeline.groupBy("job_id", "name").agg(
            count("*").alias("total_runs"),
            spark_sum(when(col("result_state") == "SUCCESS", 1).otherwise(0)).alias("successful_runs"),
            spark_sum(when(col("result_state") == "FAILED", 1).otherwise(0)).alias("failed_runs"),
            avg("duration_minutes").alias("avg_duration_minutes"),
        )

        dlt_agg = dlt.groupBy("pipeline_id", "name").agg(
            count("*").alias("dlt_pipeline_count"),
            spark_sum("is_active").alias("active_dlt_pipelines"),
        )

        data_product_agg = tables.groupBy("catalog_name").agg(
            count("*").alias("table_count"),
            spark_sum(
                when(col("last_access_time") < expr("current_timestamp() - INTERVAL 7 DAYS"), 1).otherwise(0)
            ).alias("stale_table_count"),
        )

        query_perf_agg = query_history.groupBy().agg(
            avg("execution_time_ms").alias("avg_query_ms"),
            spark_sum(when(col("execution_time_ms") > 30000, 1).otherwise(0)).alias("slow_query_count"),
            count("*").alias("query_count"),
        )

        self.writer.write("infra_metrics", infra_agg)
        self.writer.write("pipeline_metrics", pipeline_agg)
        self.writer.write("dlt_metrics", dlt_agg)
        self.writer.write("data_product_metrics", data_product_agg)
        self.writer.write("query_performance_metrics", query_perf_agg)

        print("Gold aggregation complete.")
