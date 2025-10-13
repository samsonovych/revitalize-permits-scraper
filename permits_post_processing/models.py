"""Pydantic models for post-processing CLI outputs."""

from __future__ import annotations

import pandas as pd
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional


class PostProcessingResult(BaseModel):
    """
    Result of a post-processing operation.

    Parameters
    ----------
    df : pandas.DataFrame
        Post-processed DataFrame.
    output_path : str | None
        Path to the post-processed dataset written by the processor.
    permits_number_before : int
        Number of unique permits in the input dataset.
    permits_number_after : int
        Number of unique permits in the result dataset.
    """

    df: pd.DataFrame = Field(description="Post-processed DataFrame.")
    output_path: Optional[str] = Field(description="Path to the post-processed dataset.")
    permits_number_before: int = Field(description="Number of unique permits in the input dataset.")
    permits_number_after: int = Field(description="Number of unique permits in the result dataset.")
    model_config = ConfigDict(arbitrary_types_allowed=True)
