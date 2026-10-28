
import argparse
import json

from schema_pipeline.pipeline import run_pipeline


def main():
    parser = argparse.ArgumentParser(
        description="Parquet Schema Evolution Pipeline"
    )

    parser.add_argument("--input", default="data/input")
    parser.add_argument("--output", default="outputs")
    parser.add_argument("--run-id", default="local-run-001")
    parser.add_argument("--reference", default=None)

    args = parser.parse_args()

    report = run_pipeline(
    input_dir=args.input,
    output_dir=args.output,
    run_id=args.run_id,
    reference_customers_path=args.reference,
    )

    print(json.dumps(report, indent=2))

    if not report["reconciliation_ok"]:
        raise SystemExit("Reconciliation failed")


if __name__ == "__main__":
    main()
