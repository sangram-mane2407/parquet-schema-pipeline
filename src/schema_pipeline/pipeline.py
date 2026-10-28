
from pathlib import Path
from datetime import datetime, timezone
import json
import logging
import time
import uuid

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

REQUIRED_COLUMNS = {
    "customer_id": "string",
    "event_id": "string",
    "name": "string",
    "amount": "numeric",
    "event_ts": "timestamp",
}

OPTIONAL_COLUMNS = {
    "city": "string",
    "source_system": "string",
}


def inspect_schema(file_path):
    schema = pq.read_schema(file_path)
    result = {}

    for field in schema:
        dtype = field.type

        if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
            kind = "string"
        elif (
            pa.types.is_integer(dtype)
            or pa.types.is_floating(dtype)
            or pa.types.is_decimal(dtype)
        ):
            kind = "numeric"
        elif pa.types.is_timestamp(dtype) or pa.types.is_date(dtype):
            kind = "timestamp"
        else:
            kind = "unknown"

        result[field.name] = kind

    return result


def validate_schema(schema):
    errors = []

    for column, expected_type in REQUIRED_COLUMNS.items():
        if column not in schema:
            errors.append(f"missing_required_column:{column}")
        elif schema[column] != expected_type:
            errors.append(
                f"incompatible_type:{column}:"
                f"expected_{expected_type}:got_{schema[column]}"
            )

    allowed_columns = set(REQUIRED_COLUMNS) | set(OPTIONAL_COLUMNS)

    for column, actual_type in schema.items():
        if column not in allowed_columns:
            errors.append(f"unapproved_column:{column}")
        elif column in OPTIONAL_COLUMNS:
            if actual_type != OPTIONAL_COLUMNS[column]:
                errors.append(
                    f"incompatible_optional_type:{column}:"
                    f"expected_{OPTIONAL_COLUMNS[column]}:got_{actual_type}"
                )

    return errors


def json_safe_record(row):
    return json.loads(
        pd.Series(row).to_json(
            date_format="iso",
            default_handler=str,
        )
    )


def run_pipeline(
    input_dir,
    output_dir,
    run_id=None,
    reference_customers_path=None,
):
    start_time = time.monotonic()
    started_at = datetime.now(timezone.utc)
    run_id = run_id or str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    input_dir = Path(input_dir)
    output_dir = Path(output_dir)

    target_dir = output_dir / "target"
    quarantine_dir = output_dir / "quarantine"

    target_dir.mkdir(parents=True, exist_ok=True)
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    accepted_records = []
    rejected_records = []
    batch_reports = []
    seen_event_ids = set()
    input_rows = 0
    schema_drift = []

    files = sorted(input_dir.glob("*.parquet"))
    valid_customer_ids = None

    if reference_customers_path is not None:
        reference_df = pd.read_parquet(reference_customers_path)

        if "customer_id" not in reference_df.columns:
            raise ValueError(
                "Reference dataset must contain customer_id column"
            )

        valid_customer_ids = set(
            reference_df["customer_id"]
            .dropna()
            .astype(str)
            .str.strip()
        )

    for file_path in files:
        batch_start = time.monotonic()

        try:
            schema = inspect_schema(file_path)
            schema_errors = validate_schema(schema)

            df = pq.read_table(file_path).to_pandas()
            batch_row_count = len(df)
            input_rows += batch_row_count

            added_columns = sorted(
                set(schema) - set(REQUIRED_COLUMNS)
            )

            if added_columns:
                schema_drift.append({
                    "source_file": file_path.name,
                    "added_columns": added_columns,
                    "approved": not schema_errors,
                })

            if schema_errors:
                for row_number, row in df.iterrows():
                    rejected_records.append({
                        "run_id": run_id,
                        "source_file": file_path.name,
                        "source_row": int(row_number),
                        "reject_reason": "|".join(schema_errors),
                        "record_json": json.dumps(
                            json_safe_record(row)
                        ),
                    })

                batch_reports.append({
                    "source_file": file_path.name,
                    "status": "quarantined_schema",
                    "input_rows": batch_row_count,
                    "accepted_rows": 0,
                    "rejected_rows": batch_row_count,
                    "errors": schema_errors,
                    "duration_ms": round(
                        (time.monotonic() - batch_start) * 1000, 2
                    ),
                })
                continue

            for column in OPTIONAL_COLUMNS:
                if column not in df.columns:
                    df[column] = pd.NA

            df["amount"] = pd.to_numeric(
                df["amount"], errors="coerce"
            )

            df["event_ts"] = pd.to_datetime(
                df["event_ts"],
                errors="coerce",
                utc=True,
            )

            df["source_file"] = file_path.name
            df["run_id"] = run_id

            accepted_count = 0
            rejected_count = 0

            for row_number, row in df.iterrows():
                reasons = []
                if valid_customer_ids is not None:
                    customer_value = row["customer_id"]

                    if pd.notna(customer_value):
                        customer_id = str(customer_value).strip()

                        if (
                            customer_id
                            and customer_id not in valid_customer_ids
                        ):
                            reasons.append(
                                "invalid_customer_reference"
                            )

                for column in ("customer_id", "event_id", "name"):
                    value = row[column]

                    if pd.isna(value) or not str(value).strip():
                        reasons.append(f"missing_or_blank_{column}")

                if pd.isna(row["amount"]):
                    reasons.append("invalid_or_missing_amount")
                elif row["amount"] < 0:
                    reasons.append("negative_amount")

                if pd.isna(row["event_ts"]):
                    reasons.append("invalid_or_missing_event_timestamp")

                event_id = (
                    str(row["event_id"]).strip()
                    if pd.notna(row["event_id"])
                    else ""
                )

                if not reasons and event_id in seen_event_ids:
                    reasons.append("duplicate_event_id")

                if reasons:
                    rejected_count += 1

                    rejected_records.append({
                        "run_id": run_id,
                        "source_file": file_path.name,
                        "source_row": int(row_number),
                        "reject_reason": "|".join(reasons),
                        "record_json": json.dumps(
                            json_safe_record(row)
                        ),
                    })
                else:
                    seen_event_ids.add(event_id)
                    accepted_count += 1

                    # FIX: store a dictionary, not a DataFrame.
                    accepted_records.append(row.to_dict())

            batch_reports.append({
                "source_file": file_path.name,
                "status": "processed",
                "input_rows": batch_row_count,
                "accepted_rows": accepted_count,
                "rejected_rows": rejected_count,
                "errors": [],
                "duration_ms": round(
                    (time.monotonic() - batch_start) * 1000, 2
                ),
            })

        except Exception as exc:
            logging.exception(
                "Failed to process %s", file_path.name
            )

            batch_reports.append({
                "source_file": file_path.name,
                "status": "failed",
                "input_rows": None,
                "accepted_rows": 0,
                "rejected_rows": None,
                "errors": [
                    f"{type(exc).__name__}: {exc}",
                    "row_counts_unknown_due_to_processing_failure",
                ],
                "duration_ms": round(
                    (time.monotonic() - batch_start) * 1000, 2
                ),
    })

    target_columns = [
        "customer_id",
        "event_id",
        "name",
        "amount",
        "event_ts",
        "city",
        "source_system",
        "source_file",
        "run_id",
    ]

    # FIX: construct the target from a list of dictionaries.
    if accepted_records:
        target = pd.DataFrame.from_records(accepted_records)
        target = target.reindex(columns=target_columns)

        target["amount"] = pd.to_numeric(
            target["amount"], errors="coerce"
        )

        target["event_ts"] = pd.to_datetime(
            target["event_ts"],
            errors="coerce",
            utc=True,
        )

        target = target.sort_values(
            ["event_ts", "event_id"],
            kind="stable",
        ).reset_index(drop=True)
    else:
        target = pd.DataFrame(columns=target_columns)

    target.to_parquet(
        target_dir / "accepted_records.parquet",
        index=False,
    )

    reject_path = quarantine_dir / "rejected_records.jsonl"

    with reject_path.open("w", encoding="utf-8") as output_file:
        for record in rejected_records:
            output_file.write(
                json.dumps(record, default=str) + "\n"
            )

    accepted_rows = len(target)
    rejected_rows = len(rejected_records)
    counts_known = all(
    batch["input_rows"] is not None
    and batch["rejected_rows"] is not None
    for batch in batch_reports
    )

    reconciliation_ok = (
    counts_known
    and input_rows == accepted_rows + rejected_rows
)

    report = {
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "input_files": len(files),
        "input_rows": input_rows,
        "accepted_rows": accepted_rows,
        "rejected_rows": rejected_rows,
        "reconciliation_ok": reconciliation_ok,
        "schema_drift_events": schema_drift,
        "batches": batch_reports,
        "duration_ms": round(
            (time.monotonic() - start_time) * 1000, 2
        ),
    }

    (output_dir / "run_report.json").write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )

    logging.info("Run completed: %s", json.dumps(report))
    return report
