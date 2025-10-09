"""UI utilities: logging setup, IO helpers, chunking, and date parsing."""

from __future__ import annotations

import logging
import math
from pathlib import Path
from typing import Any, Dict, List, Tuple, Type
from datetime import datetime, date, timedelta
import pandas as pd
from pydantic import BaseModel
from pydantic_core import PydanticUndefined


GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


def setup_file_logging(log_file: Path) -> None:
    """Configure file-only logging for the CLI."""
    log_file.touch(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[logging.FileHandler(log_file, encoding="utf-8")],
        force=True,
    )


def chunk_evenly(items: List[Any], chunks: int) -> List[List[Any]]:
    """Split a list into ``chunks`` contiguous slices with near-even sizes.

    Examples
    --------
    >>> chunk_evenly([1,2,3,4,5], 2)
    [[1, 2, 3], [4, 5]]
    """
    n = max(1, chunks)
    size, remainder = divmod(len(items), n)
    result: List[List[Any]] = []
    start = 0
    for i in range(n):
        extra = 1 if i < remainder else 0
        end = start + size + extra
        result.append(items[start:end])
        start = end
    return result


def parse_date_flexible(s: str) -> date:
    s = s.strip()
    fmts = [
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d.%m.%Y",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m-%d-%Y",
    ]
    for f in fmts:
        try:
            return datetime.strptime(s, f).date()
        except ValueError:
            continue
    raise ValueError(f"Unsupported date format: {s}")


def format_ddmmyyyy(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def iter_range_by_parts(start_d: date, end_d: date, parts: int) -> List[Tuple[date, date]]:
    """
    Split a date range into non-overlapping contiguous subranges.

    Each chunk covers a consecutive, non-overlapping range. The last chunk
    always ends at `end_d`.

    Parameters
    ----------
    start_d : date
        Start date (inclusive).
    end_d : date
        End date (inclusive).
    parts : int
        Number of chunks to split the range into.

    Returns
    -------
    List[Tuple[date, date]]
        List of (start, end) tuples for each chunk.

    Examples
    --------
    >>> from datetime import date
    >>> iter_range_by_parts(date(2024, 1, 1), date(2024, 1, 10), 3)
    [(datetime.date(2024, 1, 1), datetime.date(2024, 1, 4)), (datetime.date(2024, 1, 5), datetime.date(2024, 1, 7)), (datetime.date(2024, 1, 8), datetime.date(2024, 1, 10))]
    """
    if parts <= 0:
        raise ValueError("parts must be greater than 0")
    total_days = (end_d - start_d).days + 1
    base = total_days // parts
    rem = total_days % parts
    result: List[Tuple[date, date]] = []
    current = start_d
    for i in range(parts):
        days = base + (1 if i < rem else 0)
        chunk_start = current
        chunk_end = chunk_start + timedelta(days=days - 1)
        if chunk_end > end_d or i == parts - 1:
            chunk_end = end_d
        result.append((chunk_start, chunk_end))
        current = chunk_end + timedelta(days=1)
        if current > end_d:
            break
    return result



def compute_chunk_count(start_d: date, end_d: date, days_per_step: int | None) -> int:
    """Compute number of chunks consistent with scraper semantics.

    When ``days_per_step`` is -1/None, the entire range is a single chunk.
    Otherwise, each chunk spans ``days_per_step + 1`` inclusive days.
    """
    if days_per_step is None or days_per_step == -1:
        return 1
    total_days = (end_d - start_d).days + 1
    span = max(0, days_per_step) + 1
    return math.ceil(total_days / span)


def read_permit_numbers(csv_path: Path, column: str) -> List[str]:
    """Read and return unique permit numbers from a CSV column, preserving order.

    Parameters
    ----------
    csv_path : Path
        Path to CSV file.
    column : str
        Column name containing permit IDs.

    Returns
    -------
    List[str]
        Deduplicated permit IDs as strings.
    """
    df = pd.read_csv(csv_path)
    if column not in df.columns:
        raise KeyError(f"Column {column!r} not found. Available: {list(df.columns)}")
    series = df[column].dropna().astype(str).map(str.strip)
    seen: Dict[str, None] = {}
    return [x for x in series.tolist() if not (x in seen or seen.setdefault(x, None))]



def prompt_for_model(model: Type[BaseModel]) -> BaseModel:
    """Prompt user for fields of a Pydantic model and return an instance.

    Parameters
    ----------
    model : Type[BaseModel]
        The Pydantic model class describing the required inputs.

    Returns
    -------
    BaseModel
        An instance populated from user input.
    """
    values: Dict[str, Any] = {}
    for name, field in model.model_fields.items():
        desc = field.description or name
        has_default = field.default is not PydanticUndefined
        default_repr = f" (default: {field.default})" if has_default else ""
        raw = input(f"{desc}{default_repr}: ").strip()
        if raw == "" and has_default:
            values[name] = field.default
            continue
        anno = field.annotation
        try:
            if anno is bool:
                # Always prompt for boolean; accept y/n/true/false/1/0, default when blank
                if raw == "" and has_default:
                    values[name] = bool(field.default)
                else:
                    values[name] = raw.lower() in {"y", "yes", "true", "1"}
            elif anno is int:
                values[name] = int(raw)
            elif anno is float:
                values[name] = float(raw)
            elif anno is date:
                values[name] = parse_date_flexible(raw)
            elif anno is Path:
                values[name] = Path(raw).expanduser().resolve()
            else:
                values[name] = raw
        except Exception:
            values[name] = raw
    return model(**values)

