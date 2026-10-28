
from pathlib import Path
import argparse

import numpy as np
import pandas as pd


def generate_data(output_dir: str, rows: int = 100, seed: int = 42) -> None:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(seed)

    # Batch 1: Original schema
    batch1_rows = max(1, rows // 3)
    batch2_rows = max(1, rows // 3)
    batch3_rows = max(1, rows - batch1_rows - batch2_rows)

    def make_batch(start: int, count: int) -> pd.DataFrame:
        return pd.DataFrame({
            "customer_id": [
                f"C{i:06d}" for i in range(start, start + count)
            ],
            "event_id": [
                f"E{i:06d}" for i in range(start, start + count)
            ],
            "name": [
                f"Customer {i}" for i in range(start, start + count)
            ],
            "amount": rng.uniform(10, 1000, count).round(2),
            "event_ts": pd.date_range(
                "2026-01-01",
                periods=count,
                freq="min",
                tz="UTC",
            ),
        })

    batch1 = make_batch(0, batch1_rows)
    batch1.to_parquet(
        output_path / "batch_01_base.parquet",
        index=False,
    )

    # Batch 2: Approved optional column added
    batch2 = make_batch(batch1_rows, batch2_rows)
    batch2["city"] = rng.choice(
        ["Pune", "Mumbai", "Karad"],
        size=batch2_rows,
    )
    batch2.to_parquet(
        output_path / "batch_02_compatible.parquet",
        index=False,
    )

    # Batch 3: Breaking schema change
    batch3 = make_batch(
        batch1_rows + batch2_rows,
        batch3_rows,
    )
    batch3["amount"] = batch3["amount"].map(
        lambda value: f"INR {value:.2f}"
    )
    batch3.to_parquet(
        output_path / "batch_03_breaking.parquet",
        index=False,
    )

    print(f"Sample batches created in: {output_path.resolve()}")
    print(f"Configured rows: {rows}")
    print("Created: base, compatible, and breaking-schema batches")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="data/input")
    parser.add_argument("--rows", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)

    args = parser.parse_args()

    generate_data(
        output_dir=args.output,
        rows=args.rows,
        seed=args.seed,
    )
