# Databricks Data Platform Health Service

This project implements a three-layer Databricks health monitoring pipeline:

- **Bronze**: raw ingestion from Databricks REST APIs, system tables, and Azure Monitor.
- **Silver**: enrichment and joins between raw sources to build actionable datasets.
- **Gold**: aggregated metrics for dashboard consumption across infrastructure, pipelines, and data products.

## Architecture

1. `notebooks/01_bronze_health.ipynb` - ingestion of raw data.
2. `notebooks/02_silver_health.ipynb` - enrichment and joins.
3. `notebooks/03_gold_health.ipynb` - metric aggregation and dashboard-ready outputs.

## Dependencies

Install with Poetry:

```bash
cd /Users/Denis/Desktop/Databricks_sdk
poetry install
```

## Notes

- Use Databricks secrets for `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, Azure credentials, and workspace IDs.
- Schedule notebooks in Databricks Jobs: Bronze -> Silver -> Gold.
- Gold layer writes aggregated tables for dashboard tabs: Infrastructure, Pipeline, Data Product.
