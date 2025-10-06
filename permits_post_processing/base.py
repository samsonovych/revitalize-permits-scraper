"""Abstract base class for DataFrame post-processors.

Defines the contract each post-processor must implement.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from pydantic import BaseModel, Field
from permits_post_processing.models import PostProcessingResult
import pandas as pd
from typing import List, Any


class BasePostProcessor(ABC, BaseModel):
    """Abstract post-processor.

    A post-processor receives a pandas ``DataFrame`` and returns a new
    ``DataFrame`` with transformations applied.

    Attributes
    ----------
    name : str
        Human-readable name for display in UIs.

    Notes
    -----
    Subclasses must be importable by the registry and implement
    :meth:`process`.
    """

    name: str = Field(default="Unnamed Post-Processor", description="Human-readable name for display in UIs.")

    @abstractmethod
    def process(self, df: pd.DataFrame, output_path: Path) -> PostProcessingResult:
        """Transform a DataFrame, write it to ``output_path``, and return summary.

        Parameters
        ----------
        df : pandas.DataFrame
            Input DataFrame.
        output_path : pathlib.Path
            Destination path for the processed dataset. Implementations
            should infer the file format from the extension (``.csv`` or
            ``.parquet`` are recommended).

        Returns
        -------
        PostProcessingResult
            Summary including the processed DataFrame, output path, and
            permit counts before/after.
        """

    def concatenate_values(self, values: List[Any]) -> str:
        values = [str(v) for v in values]
        return " \n\n<AND> ".join(values)

    def _infer_unique_permit_count(self, df: pd.DataFrame) -> int:
        candidates: List[str] = [
            "permit_number",
            "permit id",
            "permit_id",
            "Permit Number",
            "Permit_ID",
            "application_number",
        ]
        for col in candidates:
            if col in df.columns:
                return int(df[col].astype(str).str.strip().nunique())
        return int(df.drop_duplicates().shape[0])