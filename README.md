# Parquet Schema Evolution Pipeline

A Python-based data engineering pipeline that processes multiple Parquet batches, detects schema drift, applies approved schema evolution rules, and quarantines batches with breaking schema changes.

## Key Features

- Reads multiple Parquet files.
- Detects missing columns and incompatible data types.
- Supports approved optional columns such as `city` and `source_system`.
- Quarantines batches with breaking schema changes.
- Validates required fields and data quality.
- Detects duplicate event IDs.
- Validates customer IDs against a reference dataset when provided.
- Exports accepted records to Parquet.
- Stores rejected records in JSON Lines format.
- Generates a JSON execution report with batch metrics.
- Supports automated tests using pytest.

## Technology Stack

- Python
- Pandas
- PyArrow
- NumPy
- Pytest
- Apache Parquet
## Data Dictionary

| Column | Data Type | Required | Description |
|---|---|---|---|
| customer_id | String | Yes | Unique customer identifier; optionally validated against a reference dataset |
| event_id | String | Yes | Event identifier used for duplicate detection |
| name | String | Yes | Customer name; cannot be blank |
| amount | Numeric | Yes | Transaction amount; must be valid and non-negative |
| event_ts | Timestamp | Yes | Event timestamp used to validate and sort records |
| city | String | No | Optional customer city; approved schema evolution |
| source_system | String | No | Optional source system name |
| source_file | String | Generated | Name of the source Parquet batch |
| run_id | String | Generated | Identifier for the pipeline execution |

## Architecture

```text
Parquet Input Batches
        |
        v
Schema Inspection & Validation
        |
        +---- Breaking Schema ----> Quarantine
        |
        v
Approved Schema Evolution
        |
        v
Data Quality Validation
        |
        +---- Invalid Records ----> Rejected JSONL
        |
        v
Accepted Records
        |
        v
Sorted Target Parquet + Run Report
```

## Error Handling

- Breaking schema changes are quarantined with explicit reasons.
- Invalid records are rejected with row-level reasons.
- Duplicate event IDs are rejected within a pipeline run.
- Processing failures are logged and reported in the run report.
- Reconciliation checks whether input rows equal accepted plus rejected rows when counts are known.

## Idempotency Note

The pipeline overwrites its generated output files when the same output directory is used again. This avoids appending duplicate output records on reruns. Duplicate event IDs are tracked during each run; cross-run deduplication is not currently persisted.
