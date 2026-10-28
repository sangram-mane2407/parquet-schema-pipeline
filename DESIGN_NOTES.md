# Design Notes — Parquet Schema Evolution Pipeline

## Goal
Process multiple Parquet batches, detect schema drift, validate records, and quarantine incompatible schemas.

## Schema Policy
Required columns are customer_id, event_id, name, amount, and event_ts. Approved optional columns are city and source_system. Missing required columns, incompatible types, or unapproved columns cause the batch to be quarantined.

## Data Quality
The pipeline checks missing or blank values, numeric amounts, negative amounts, timestamps, duplicate event IDs, and optional customer reference validity.

## Error Handling and Reconciliation
Rejected rows are written to JSONL with rejection reasons. Failed batches are reported in run_report.json. Reconciliation is false when a batch fails before its row counts are known.

## Idempotency and Recovery
Output files are overwritten on reruns rather than appended. Duplicate event IDs are tracked in memory within a single run; persistent cross-run deduplication is not implemented. Failed batches require manual review and rerun; automatic retries are not implemented.

## Performance and Trade-offs
The pipeline processed 100,000 rows in approximately 23.90 seconds in a local test. Records are collected in memory, which simplifies validation but can increase memory usage for larger datasets.

## Future Improvements
- Add automatic retries for transient failures.
- Add persistent deduplication across runs.
- Process very large datasets incrementally.
- Add monitoring and alerting.
