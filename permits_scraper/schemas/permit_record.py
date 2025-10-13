"""Permit record schemas for permit-related data.

This module defines Pydantic models representing contact information
commonly found in permit details, including applicant and owner data.

The models are intentionally simple and reusable across regions.
"""


from pydantic import BaseModel, Field, ConfigDict


class PermitRecord(BaseModel):
    """Permit record information.

    Parameters
    ----------
    permit_number : str
        Permit number.
    ... : Any, default=None
        Additional permit record data.

    Examples
    --------
    >>> PermitRecord(permit_number="1234567890")
    PermitRecord(permit_number='1234567890')
    """

    permit_number: str = Field(description="Permit number")

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")
