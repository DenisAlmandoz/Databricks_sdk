from datetime import timedelta

import pandas as pd

try:
    from azure.monitor.query import LogsQueryClient
except ModuleNotFoundError:
    LogsQueryClient = None  # type: ignore

from .base import BaseIngestor


class AzureMonitorIngestor(BaseIngestor):
    def __init__(
        self,
        logs_client: "LogsQueryClient",
        workspace_id: str,
        lookback_days: int = 30,
    ):
        if LogsQueryClient is None:
            raise ImportError(
                "AzureMonitorIngestor requires azure-monitor-query. "
                "Install it with: pip install azure-monitor-query azure-identity"
            )
        super().__init__(lookback_days)
        self.logs_client = logs_client
        self.workspace_id = workspace_id

    def _run_query(self, query: str) -> pd.DataFrame:
        response = self.logs_client.query_workspace(
            workspace_id=self.workspace_id,
            query=query,
            timespan=timedelta(days=self.lookback_days),
        )
        rows = []
        for table in response.tables:
            for row in table.rows:
                rows.append(dict(zip(table.columns, row)))
        return pd.DataFrame(rows)

    def _ingest_cluster_health(self) -> pd.DataFrame:
        query = f"""
            AzureDiagnostics
            | where Category == 'Clusters'
            | where TimeGenerated >= ago({self.lookback_days}d)
            | extend ClusterId = tostring(parse_json(tostring(properties_s)).ClusterId)
            | summarize
                startup_attempts = count(),
                startup_success = countif(Status_s == 'Succeeded'),
                startup_failures = countif(Status_s == 'Failed'),
                last_attempt = max(TimeGenerated)
              by ClusterId, _ResourceId
        """
        return self._run_query(query)

    def _ingest_storage_health(self) -> pd.DataFrame:
        query = f"""
            AzureDiagnostics
            | where Category == 'Storage'
            | where TimeGenerated >= ago({self.lookback_days}d)
            | extend StorageAccount = tostring(parse_json(tostring(properties_s)).storage_account_name)
            | summarize
                connectivity_attempts = count(),
                connectivity_errors = countif(Status_s == 'Failed'),
                last_check = max(TimeGenerated)
              by StorageAccount, _ResourceId
        """
        return self._run_query(query)

    def _ingest_workspace_health(self) -> pd.DataFrame:
        query = f"""
            AzureDiagnostics
            | where Category == 'Workspace'
            | where TimeGenerated >= ago({self.lookback_days}d)
            | summarize
                uptime_checks = count(),
                downtime_events = countif(Status_s == 'Unavailable'),
                last_check = max(TimeGenerated)
              by _ResourceId
        """
        return self._run_query(query)

    def ingest(self) -> dict[str, pd.DataFrame]:
        return {
            "azure_monitor_clusters": self._ingest_cluster_health(),
            "azure_monitor_storage": self._ingest_storage_health(),
            "azure_monitor_workspace": self._ingest_workspace_health(),
        }
