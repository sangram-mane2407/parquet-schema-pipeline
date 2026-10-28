
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from schema_pipeline.pipeline import (
    validate_schema,
    run_pipeline,
)


def test_compatible_schema():
    schema = {
        "customer_id": "string",
        "event_id": "string",
        "name": "string",
        "amount": "numeric",
        "event_ts": "timestamp",
        "city": "string",
    }

    assert validate_schema(schema) == []


def test_breaking_schema():
    schema = {
        "customer_id": "string",
        "event_id": "string",
        "name": "string",
        "amount": "string",
        "event_ts": "timestamp",
    }

    errors = validate_schema(schema)

    assert any("incompatible_type:amount" in e for e in errors)


def test_missing_required_column():
    schema = {
        "customer_id": "string",
        "event_id": "string",
        "name": "string",
        "amount": "numeric",
    }

    errors = validate_schema(schema)

    assert "missing_required_column:event_ts" in errors


def test_empty_input(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "outputs"
    input_dir.mkdir()

    report = run_pipeline(input_dir, output_dir, "empty-test")

    assert report["input_rows"] == 0
    assert report["accepted_rows"] == 0
    assert report["rejected_rows"] == 0
    assert report["reconciliation_ok"] is True


def test_duplicate_event_is_quarantined(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "outputs"
    input_dir.mkdir()

    df = pd.DataFrame({
        "customer_id": ["C001", "C002"],
        "event_id": ["E001", "E001"],
        "name": ["Asha", "Ravi"],
        "amount": [100.0, 200.0],
        "event_ts": pd.to_datetime(
            ["2026-01-01T10:00:00Z", "2026-01-01T11:00:00Z"],
            utc=True,
        ),
    })

    pq.write_table(
        pa.Table.from_pandas(df, preserve_index=False),
        input_dir / "duplicates.parquet",
    )

    report = run_pipeline(input_dir, output_dir, "duplicate-test")

    assert report["accepted_rows"] == 1
    assert report["rejected_rows"] == 1

    rejects = (
        output_dir / "quarantine" / "rejected_records.jsonl"
    ).read_text(encoding="utf-8")

    assert "duplicate_event_id" in rejects

def test_rerun_same_input_is_idempotent(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "outputs"
    input_dir.mkdir()

    df = pd.DataFrame({
        "customer_id": ["C001", "C002"],
        "event_id": ["E001", "E002"],
        "name": ["Asha", "Ravi"],
        "amount": [100.0, 200.0],
        "event_ts": pd.to_datetime(
            ["2026-01-01T10:00:00Z", "2026-01-01T11:00:00Z"],
            utc=True,
        ),
    })

    df.to_parquet(input_dir / "batch.parquet", index=False)

    first = run_pipeline(input_dir, output_dir, "rerun-test")
    first_target = pd.read_parquet(
        output_dir / "target" / "accepted_records.parquet"
    )

    second = run_pipeline(input_dir, output_dir, "rerun-test")
    second_target = pd.read_parquet(
        output_dir / "target" / "accepted_records.parquet"
    )

    assert first["accepted_rows"] == 2
    assert second["accepted_rows"] == 2
    assert len(first_target) == len(second_target) == 2
    assert second["reconciliation_ok"] is True

def test_malformed_parquet_file_is_reported(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "outputs"
    input_dir.mkdir()

    # Deliberately create a file that is not a valid Parquet file.
    (input_dir / "broken.parquet").write_text(
        "this is not a parquet file",
        encoding="utf-8",
    )

    report = run_pipeline(
        input_dir,
        output_dir,
        "malformed-test",
    )

    assert report["input_files"] == 1
    assert len(report["batches"]) == 1
    assert report["batches"][0]["status"] == "failed"
    assert report["batches"][0]["errors"]

def test_invalid_customer_reference_is_rejected(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "outputs"
    input_dir.mkdir()

    df = pd.DataFrame({
        "customer_id": ["C001", "C999"],
        "event_id": ["E001", "E002"],
        "name": ["Asha", "Unknown Customer"],
        "amount": [100.0, 200.0],
        "event_ts": pd.to_datetime(
            ["2026-01-01T10:00:00Z", "2026-01-01T11:00:00Z"],
            utc=True,
        ),
    })

    df.to_parquet(input_dir / "reference_test.parquet", index=False)

    # Expected behavior:
    # C001 exists in the customer reference dataset.
    # C999 does not exist and must be quarantined.

    report = run_pipeline(
        input_dir,
        output_dir,
        "reference-test",
    )

    assert report["accepted_rows"] == 1
    assert report["rejected_rows"] == 1
def test_invalid_customer_reference_is_rejected(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "outputs"
    input_dir.mkdir()

    df = pd.DataFrame({
        "customer_id": ["C001", "C999"],
        "event_id": ["E001", "E002"],
        "name": ["Asha", "Unknown Customer"],
        "amount": [100.0, 200.0],
        "event_ts": pd.to_datetime(
            ["2026-01-01T10:00:00Z", "2026-01-01T11:00:00Z"],
            utc=True,
        ),
    })

    df.to_parquet(
        input_dir / "reference_test.parquet",
        index=False,
    )

    reference_df = pd.DataFrame({
        "customer_id": ["C001"],
    })

    reference_path = tmp_path / "customers.parquet"
    reference_df.to_parquet(reference_path, index=False)

    report = run_pipeline(
        input_dir,
        output_dir,
        "reference-test",
        reference_customers_path=reference_path,
    )

    assert report["accepted_rows"] == 1
    assert report["rejected_rows"] == 1
    assert report["reconciliation_ok"] is True
def test_null_values_are_rejected(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "outputs"
    input_dir.mkdir()

    df = pd.DataFrame({
        "customer_id": ["C001", "C002"],
        "event_id": ["E001", "E002"],
        "name": ["Asha", None],
        "amount": [100.0, None],
        "event_ts": pd.to_datetime(
            ["2026-01-01T10:00:00Z", None],
            utc=True,
        ),
    })

    df.to_parquet(input_dir / "nulls.parquet", index=False)

    report = run_pipeline(input_dir, output_dir, "null-test")

    assert report["accepted_rows"] == 1
    assert report["rejected_rows"] == 1
    assert report["reconciliation_ok"] is True


def test_out_of_order_timestamps_are_sorted(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "outputs"
    input_dir.mkdir()

    df = pd.DataFrame({
        "customer_id": ["C001", "C002", "C003"],
        "event_id": ["E003", "E001", "E002"],
        "name": ["Asha", "Ravi", "Neha"],
        "amount": [300.0, 100.0, 200.0],
        "event_ts": pd.to_datetime(
            [
                "2026-01-03T10:00:00Z",
                "2026-01-01T10:00:00Z",
                "2026-01-02T10:00:00Z",
            ],
            utc=True,
        ),
    })

    df.to_parquet(input_dir / "out_of_order.parquet", index=False)

    report = run_pipeline(input_dir, output_dir, "timestamp-test")

    target = pd.read_parquet(
        output_dir / "target" / "accepted_records.parquet"
    )

    assert report["accepted_rows"] == 3
    assert report["reconciliation_ok"] is True
    assert target["event_id"].tolist() == ["E001", "E002", "E003"]
    
def test_partial_failure_does_not_claim_reconciliation(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "outputs"
    input_dir.mkdir()

    # One valid batch
    df = pd.DataFrame({
        "customer_id": ["C001"],
        "event_id": ["E001"],
        "name": ["Asha"],
        "amount": [100.0],
        "event_ts": pd.to_datetime(
            ["2026-01-01T10:00:00Z"],
            utc=True,
        ),
    })
    df.to_parquet(input_dir / "batch_01.parquet", index=False)

    # One malformed batch
    (input_dir / "batch_02.parquet").write_text(
        "invalid parquet data",
        encoding="utf-8",
    )

    report = run_pipeline(
        input_dir,
        output_dir,
        "partial-failure-test",
    )

    assert report["input_files"] == 2
    assert report["accepted_rows"] == 1
    assert report["batches"][1]["status"] == "failed"
    assert report["reconciliation_ok"] is False
