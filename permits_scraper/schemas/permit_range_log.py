"""Permit range log schemas for permit-related data.

This module defines Pydantic models representing permit range log information.

The models are intentionally simple and reusable across regions.
"""

from typing import Optional

from pydantic import BaseModel, Field, ConfigDict


class PermitRangeLog(BaseModel):
    """Permit range log information.

    Parameters
    ----------
    number_of_permits : int
        Number of permits.
    start_date : str
        Start date.
    end_date : str
        End date.
    output_path : Optional[str], default=None
        Output path.
    ... : Any, default=None
        Additional permit range log data.

    Examples
    --------
    >>> PermitRangeLog(number_of_permits=100, start_date="2024-01-01", end_date="2024-01-31", output_path="output.json")
    PermitRangeLog(number_of_permits=100, start_date='2024-01-01', end_date='2024-01-31', output_path='output.json')
    """

    number_of_permits: int = Field(description="Number of permits")
    start_date: str = Field(description="Start date")
    end_date: str = Field(description="End date")
    output_path: Optional[str] = Field(default=None, description="Output path")

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")
