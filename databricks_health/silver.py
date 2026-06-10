from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

from databricks_health.writers import DeltaWriter


class SilverEnrichmentOrchestrator:
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
        self.reader = DeltaWriter(spark, catalog, "bronze")  # reuse for read-like table access
        self.writer = DeltaWriter(spark, catalog, "silver")

    def _read(self, table: str):
        return self.spark.table(f"{self.catalog}.bronze.{table}")

    def run(self):
        clusters = self._read("clusters")
        job_runs = self._read("job_runs")
        pipelines = self._read("pipelines")
        dlt_pipelines = self._read("dlt_pipelines")
        azure_clusters = self._read("azure_monitor_clusters")
        query_history = self._read("sql_query_history")

        infra_health = (
            clusters
            .withColumn("startup_success", when(col("state") == "RUNNING", 1).otherwise(0))
            .withColumn("startup_failure", when(col("state") == "ERROR", 1).otherwise(0))
            .withColumn("cluster_age_hours", (col("terminate_time") - col("start_time")) / 3600000)
        )

        cluster_monitor = (
            azure_clusters
            .withColumnRenamed("ClusterId", "cluster_id")
            .withColumnRenamed("startup_attempts", "monitor_startup_attempts")
            .withColumnRenamed("startup_success", "monitor_startup_success")
        )

        infra_health = infra_health.join(cluster_monitor, on="cluster_id", how="left")

        job_runs_enriched = (
            job_runs
            .withColumn("duration_minutes", (col("duration_ms") / 1000) / 60)
            .withColumn("is_failed", when(col("result_state") == "FAILED", 1).otherwise(0))
        )

        pipeline_health = job_runs_enriched.join(pipelines, on="job_id", how="left")

        dlt_health = dlt_pipelines.withColumn("is_active", when(col("state") == "ACTIVE", 1).otherwise(0))

        query_history_enriched = (
            query_history
            .withColumn("execution_time_ms", col("duration_ms"))
            .withColumn("is_slow_query", when(col("duration_ms") > 30000, 1).otherwise(0))
        )

        table_accessibility = self.spark.sql(
            "SELECT catalog_name, schema_name, table_name, row_count, last_access_time "
            "FROM system.information_schema.tables WHERE table_type = 'BASE TABLE'"
        )

        self.writer.write("infra_health", infra_health)
        self.writer.write("pipeline_health", pipeline_health)
        self.writer.write("dlt_health", dlt_health)
        self.writer.write("query_history", query_history_enriched)
        self.writer.write("table_accessibility", table_accessibility)

        print("Silver enrichment complete.")
