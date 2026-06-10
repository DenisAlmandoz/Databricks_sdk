from datetime import datetime

import pandas as pd
from databricks.sdk import WorkspaceClient

from .base import BaseIngestor


class DatabricksApiIngestor(BaseIngestor):
    def __init__(
        self,
        client: WorkspaceClient,
        lookback_days: int = 30,
    ):
        super().__init__(lookback_days)
        self.client = client

    def _ingest_clusters(self) -> pd.DataFrame:
        data = []
        for c in self.client.clusters.list():
            data.append({
                "cluster_id": c.cluster_id,
                "cluster_name": c.cluster_name,
                "state": c.state,
                "state_message": c.state_message,
                "start_time": c.start_time,
                "terminate_time": c.terminated_time,
                "cluster_source": c.cluster_source,
                "node_type_id": c.node_type_id,
                "driver_node_type_id": c.driver_node_type_id,
                "spark_version": c.spark_version,
                "num_workers": c.num_workers,
                "autotermination_minutes": c.autotermination_minutes,
                "enable_elastic_disk": c.enable_elastic_disk,
                "policy_id": c.policy_id,
                "single_user_name": c.single_user_name,
                "creator_user_name": c.creator_user_name,
                "ingestion_ts": self.ingestion_ts,
            })
        df = pd.DataFrame(data)
        if not df.empty:
            df["start_time"] = pd.to_datetime(df["start_time"], unit="ms", errors="coerce")
            df["terminate_time"] = pd.to_datetime(df["terminate_time"], unit="ms", errors="coerce")
        return df

    def _ingest_job_runs(self) -> pd.DataFrame:
        data = []
        for run in self.client.jobs.list_runs(completed_only=False, expand_tasks=False):
            data.append({
                "run_id": run.run_id,
                "job_id": run.job_id,
                "run_name": run.run_name,
                "result_state": run.state.result_state if run.state else None,
                "life_cycle_state": run.state.life_cycle_state if run.state else None,
                "state_message": run.state.state_message if run.state else None,
                "start_time": run.start_time,
                "end_time": run.end_time,
                "duration_ms": run.end_time - run.start_time if run.start_time and run.end_time else None,
                "trigger_type": run.trigger,
                "run_type": run.run_type,
                "attempt_number": run.attempt_number,
                "ingestion_ts": self.ingestion_ts,
            })
        df = pd.DataFrame(data)
        if not df.empty:
            df["start_time"] = pd.to_datetime(df["start_time"], unit="ms", errors="coerce")
            df["end_time"] = pd.to_datetime(df["end_time"], unit="ms", errors="coerce")
        return df

    def _ingest_pipelines(self) -> pd.DataFrame:
        data = []
        for job in self.client.jobs.list():
            data.append({
                "job_id": job.job_id,
                "name": job.settings.name if job.settings else None,
                "state": "ACTIVE",
                "schedule": str(job.settings.schedule) if job.settings and job.settings.schedule else None,
                "max_concurrent_runs": job.settings.max_concurrent_runs if job.settings else None,
                "timeout_seconds": job.settings.timeout_seconds if job.settings else None,
                "ingestion_ts": self.ingestion_ts,
            })
        return pd.DataFrame(data)

    def _ingest_dlt_pipelines(self) -> pd.DataFrame:
        data = []
        for p in self.client.pipelines.list_pipelines():
            data.append({
                "pipeline_id": p.pipeline_id,
                "name": p.name,
                "state": p.state,
                "cluster_id": p.cluster_id,
                "creator_user_name": p.creator_user_name,
                "latest_updates": str(p.latest_updates) if p.latest_updates else None,
                "channel": p.channel,
                "continuous": p.continuous,
                "development": p.development,
                "ingestion_ts": self.ingestion_ts,
            })
        return pd.DataFrame(data)

    def _ingest_sql_warehouses(self) -> pd.DataFrame:
        data = []
        for w in self.client.warehouses.list():
            data.append({
                "warehouse_id": w.id,
                "name": w.name,
                "state": w.state,
                "cluster_size": w.cluster_size,
                "min_num_clusters": w.min_num_clusters,
                "max_num_clusters": w.max_num_clusters,
                "auto_stop_mins": w.auto_stop_mins,
                "spot_instance_policy": w.spot_instance_policy,
                "enable_photon": w.enable_photon,
                "warehouse_type": w.warehouse_type,
                "num_active_sessions": w.num_active_sessions,
                "num_clusters": w.num_clusters,
                "ingestion_ts": self.ingestion_ts,
            })
        return pd.DataFrame(data)

    def ingest(self) -> dict[str, pd.DataFrame]:
        return {
            "clusters": self._ingest_clusters(),
            "job_runs": self._ingest_job_runs(),
            "pipelines": self._ingest_pipelines(),
            "dlt_pipelines": self._ingest_dlt_pipelines(),
            "sql_warehouses": self._ingest_sql_warehouses(),
        }
