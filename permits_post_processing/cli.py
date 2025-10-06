"""Terminal UI for running post-processors on datasets."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from .models import PostProcessingResult
from .registry import ProcessorRegistry


def _prompt(text: str) -> str:
    return input(text).strip()

def run() -> None:
    root = Path(__file__).resolve().parent
    print(root)
    registry = ProcessorRegistry(root)
    registry.discover()

    entries = registry.list()
    if not entries:
        print("No post-processors found. Please add modules under processors/<STATE>/<CITY>/post_processor.py")
        sys.exit(1)

    print("Available post-processors:")
    for idx, (key, cls) in enumerate(entries, start=1):
        print(f"  {idx}. {key.display()} — {getattr(cls, 'name', cls.__name__)}")

    while True:
        raw_idx = _prompt("Select processor by number: ")
        try:
            sel = int(raw_idx)
            if 1 <= sel <= len(entries):
                break
        except ValueError:
            pass
        print("Invalid selection. Try again.")

    selected_cls = entries[sel - 1][1]
    processor = selected_cls()

    in_path = Path(_prompt("Enter input DataFrame path (csv/parquet): "))
    if not in_path.exists():
        print(f"Input path not found: {in_path}")
        sys.exit(2)

    out_path = Path(_prompt("Enter output path (csv/parquet): "))

    # Load input
    if in_path.suffix.lower() == ".csv":
        df_in = pd.read_csv(in_path)
    elif in_path.suffix.lower() in {".parquet", ".pq"}:
        df_in = pd.read_parquet(in_path)
    else:
        print("Unsupported input format. Use .csv or .parquet")
        sys.exit(3)

    before_rows = int(df_in.shape[0])
    result = processor.process(df_in, out_path)
    after_rows = int(result.df.shape[0])
    removed_rows = before_rows - after_rows
    print(f"Rows removed: {removed_rows}")
    print(f"Permits before: {result.permits_number_before}")
    print(f"Permits in output: {result.permits_number_after}")


if __name__ == "__main__":
    run()


