"""Contacts schemas for permit-related data.

This module defines Pydantic models representing contact information
commonly found in permit details, including applicant and owner data.

The models are intentionally simple and reusable across regions.
"""

from typing import Optional

from pydantic import BaseModel


class ApplicantData(BaseModel):
    """Applicant contact information.

    Parameters
    ----------
    first_name : Optional[str], default=None
        Applicant's first name.
    last_name : Optional[str], default=None
        Applicant's last name.
    email : Optional[str], default=None
        Applicant's email address.
    phone_number : Optional[str], default=None
        Applicant's phone number in any captured format.
    address : Optional[str], default=None
        Applicant's mailing or physical address consolidated as a single string.

    Examples
    --------
    >>> ApplicantData(first_name="Jane", last_name="Doe")
    ApplicantData(first_name='Jane', last_name='Doe', email=None, phone_number=None, address=None)
    """

    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone_number: Optional[str] = None
    address: Optional[str] = None

    class Config:
        extra = "allow"


class OwnerData(BaseModel):
    """Owner contact information.

    Parameters
    ----------
    first_name : Optional[str], default=None
        Owner's first name.
    last_name : Optional[str], default=None
        Owner's last name.
    company_name: Optional[str], default=None
        Owner's company name.
    address : Optional[str], default=None
        Owner's mailing or physical address consolidated as a single string.

    Examples
    --------
    >>> OwnerData(last_name="SMITH", first_name="JOHN")
    OwnerData(first_name='JOHN', last_name='SMITH', phone_number=None, address=None)
    """

    first_name: Optional[str] = None
    last_name: Optional[str] = None
    company_name: Optional[str] = None
    address: Optional[str] = None

    class Config:
        extra = "allow"