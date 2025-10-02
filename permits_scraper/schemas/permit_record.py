"""Permit record schemas for permit-related data.

This module defines Pydantic models representing contact information
commonly found in permit details, including applicant and owner data.

The models are intentionally simple and reusable across regions.
"""

from typing import Optional

from pydantic import BaseModel, Field

from permits_scraper.schemas.contacts import ApplicantData
from permits_scraper.schemas.contacts import OwnerData


class PermitRecord(BaseModel):
    """Permit record information.

    Parameters
    ----------
    permit_number : str
        Permit number.
    description : Optional[str], default=None
        Permit description.
    applicant : ApplicantData
        Applicant data.
    owner : OwnerData
        Owner data.
    ... : Any, default=None
        Additional permit record data.

    Examples
    --------
    >>> PermitRecord(permit_number="1234567890", description="Permit description", applicant=ApplicantData(first_name="Jane", last_name="Doe"), owner=OwnerData(first_name="John", last_name="Smith"))
    PermitRecord(permit_number='1234567890', description='Permit description', applicant=ApplicantData(first_name='Jane', last_name='Doe'), owner=OwnerData(first_name='John', last_name='Smith'))
    """

    permit_number: str = Field(description="Permit number")
    applicant: ApplicantData = Field(description="Applicant data")
    owner: OwnerData = Field(description="Owner data")

    class Config:
        extra = "allow"