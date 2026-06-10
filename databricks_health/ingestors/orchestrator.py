from typing import Optional

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from .databricks_api import DatabricksApiIngestor
from .system_tables import SystemTablesIngestor
from .writer import BronzeWriter


class BronzeIngestionOrchestrator:
    def __init__(
        self,
        databricks_host: str,
        databricks_token: str,
        azure_log_analytics_workspace_id: Optional[str] = None,
        catalog: str = "health",
        schema: str = "bronze",
        lookback_days: int = 30,
    ):
        self.spark = SparkSession.builder.getOrCreate()
        self.client = WorkspaceClient(host=databricks_host, token=databricks_token)

        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        for schema_name in [schema, "silver", "gold"]:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")

        self.api_ingestor = DatabricksApiIngestor(self.client, lookback_days)
        self.system_ingestor = SystemTablesIngestor(self.spark, lookback_days)
        if azure_log_analytics_workspace_id:
            from azure.identity import DefaultAzureCredential
            from azure.monitor.query import LogsQueryClient
            from .azure_monitor import AzureMonitorIngestor

            credential = DefaultAzureCredential()
            logs_client = LogsQueryClient(credential)
            self.azure_ingestor = AzureMonitorIngestor(
                logs_client, azure_log_analytics_workspace_id, lookback_days
            )
        else:
            self.azure_ingestor = None
        self.writer = BronzeWriter(self.spark, catalog, schema)

    def run(self):
        print("Starting bronze ingestion...")

        print("\n--- Databricks REST API ---")
        api_data = self.api_ingestor.ingest()
        for name, df in api_data.items():
            print(f"  {name}: {len(df)} rows")
            self.writer.write(name, df)

        print("\n--- System Tables ---")
        system_data = self.system_ingestor.ingest()
        for name, df in system_data.items():
            print(f"  {name}: {len(df)} rows")
            self.writer.write(name, df)

        if self.azure_ingestor:
            print("\n--- Azure Monitor ---")
            azure_data = self.azure_ingestor.ingest()
            for name, df in azure_data.items():
                print(f"  {name}: {len(df)} rows")
                self.writer.write(name, df)
        else:
            print("\n--- Azure Monitor (skipped - no workspace ID configured) ---")

        print("\nBronze ingestion complete.")
