# AWS Data Pipeline Samples

Serverless data-pipeline patterns on AWS from my data/analytics work:
S3 landing → Lambda validation → Glue transformation → curated data in
S3 → Athena queries for reporting, orchestrated by Step Functions.

These are pattern samples with synthetic data — the same architecture
shape I specced and validated as an analyst (source-to-target mapping,
DQ gates, reconciliation), expressed as code.

## Architecture

```
S3 (raw landing) ──event──> Lambda: validate + quarantine
                                  │ pass
                                  ▼
                           Step Functions ──> Glue job: transform,
                                  │            dedupe, partition
                                  ▼
                           S3 (curated, Parquet, partitioned)
                                  │
                                  ▼
                           Athena ──> BI (QuickSight / Power BI)
```

## Contents

| File | Purpose |
|---|---|
| `lambda/ingest_handler.py` | S3-event Lambda: validates incoming CSV records against rules, routes clean files to processing and bad records to a quarantine prefix with a reason report |
| `glue/etl_job.py` | Glue PySpark job: reads the landing zone, applies type casting and dedupe, writes partitioned Parquet to the curated zone |
| `stepfunctions/state_machine.json` | Orchestration: validation → Glue ETL → DQ check → publish/notify, with failure branches |
| `athena/queries.sql` | Curated-zone queries: partition pruning, monthly KPIs, DQ freshness checks |
| `infra/pipeline_overview.md` | Design notes: partitioning, idempotency, reprocessing, cost habits |

## Design habits baked in

- **Quarantine, never drop** — bad records land in `quarantine/` with a
  reason file, mirroring the DQ-gate pattern in my
  `data-publishing-pipeline` repo
- **Idempotent loads** — batch id in every key; re-running a batch
  overwrites its partition instead of double-loading
- **Partition by business date** — Athena queries prune on
  `year/month/day` so reporting stays cheap
- **Manifest per batch** — counts and checksums travel with the data so
  consumers can verify before they trust

## License

MIT
