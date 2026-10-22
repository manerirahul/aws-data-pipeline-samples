# Pipeline Design Notes

## Zones
- **raw/** — exactly what arrived, never modified. Every investigation
  starts here; if curated and raw disagree, raw is the truth about what
  the source sent.
- **validated/** — files whose records passed the DQ gates.
- **quarantine/** — rejected records with reason codes, owned by the
  source-system team, not the pipeline team.
- **curated/** — publish-schema Parquet, partitioned by year/month,
  registered in the Glue Data Catalog for Athena.

## Idempotency
Every batch carries a `batch_id` end to end (S3 keys, Glue job argument,
manifest, curated column). Reprocessing a batch overwrites its
partitions (dynamic partition overwrite) instead of appending, so a
retry can never double-publish.

## Reprocessing
Fixes land in the source or the rules — never by hand-editing curated
data. Correct the config/rule, re-run the batch id, and the manifest
records the new checksum; downstream consumers re-verify before loading.

## Cost habits
- Parquet + partition pruning keep Athena scans small
- Glue job bookmarks on for incremental files; full reloads are
  explicit, scheduled events, not accidents
- Lifecycle rules age raw/ files to infrequent-access storage after
  90 days; quarantine/ is retained for audit

## Failure handling
Step Functions routes validation or DQ failures to quarantine handling
and an alert topic. A failed batch publishes nothing — a late report is
cheaper than a wrong one.
