from pyspark.sql import SparkSession

from .base import BaseIngestor


class SystemTablesIngestor(BaseIngestor):
    def __init__(self, spark: SparkSession, lookback_days: int = 30):
        super().__init__(lookback_days)
        self.spark = spark

    def _ingest_query_history(self):
        return self.spark.sql(f"""
            SELECT
                query_id,
                query_text,
                statement_type,
                duration_ms,
                user_email,
                session_id,
                warehouse_id,
                catalog_name,
                schema_name,
                start_time,
                end_time,
                error_message,
                rows_produced,
                data_scanned_bytes,
                current_timestamp() AS ingestion_ts
            FROM system.query.history
            WHERE start_time >= current_timestamp() - INTERVAL {self.lookback_days} DAYS
        """).toPandas()

    def _ingest_job_history(self):
        return self.spark.sql(f"""
            SELECT
                job_id,
                run_id,
                job_name,
                start_time,
                end_time,
                duration_ms,
                trigger_type,
                result_state,
                error_message,
                run_type,
                current_timestamp() AS ingestion_ts
            FROM system.job.history
            WHERE start_time >= current_timestamp() - INTERVAL {self.lookback_days} DAYS
        """).toPandas()

    def _ingest_cluster_metadata(self):
        return self.spark.sql("""
            SELECT
                cluster_id,
                cluster_name,
                state,
                node_type_id,
                driver_node_type_id,
                spark_version,
                autotermination_minutes,
                enable_elastic_disk,
                cluster_source,
                creator_user_name,
                first_start_time,
                last_start_time,
                last_activity_time,
                current_num_workers,
                target_num_workers,
                current_timestamp() AS ingestion_ts
            FROM system.compute.clusters
        """).toPandas()

    def _ingest_table_metadata(self):
        return self.spark.sql("""
            SELECT
                catalog_name,
                schema_name,
                table_name,
                table_type,
                data_source_format,
                row_count,
                table_size_bytes,
                created_at,
                last_altered_at,
                last_access_time,
                current_timestamp() AS ingestion_ts
            FROM system.information_schema.tables
            WHERE table_type = 'BASE TABLE'
        """).toPandas()

    def ingest(self) -> dict[str, "pd.DataFrame"]:
        import pandas as pd
        return {
            "sql_query_history": self._ingest_query_history(),
            "job_history": self._ingest_job_history(),
            "cluster_metadata": self._ingest_cluster_metadata(),
            "table_metadata": self._ingest_table_metadata(),
        }
