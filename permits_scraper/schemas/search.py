"""Search result schemas.

This module defines result models returned by scraping operations for
permit details. It composes contact models to produce a cohesive
return type.
"""

from typing import Optional

from pydantic import BaseModel

from .contacts import ApplicantData, OwnerData


class SearchResult(BaseModel):
    """Result of a permit detail search.

    Parameters
    ----------
    applicant : Optional[ApplicantData], default=None
        Parsed applicant information, if present.
    owner : Optional[OwnerData], default=None
        Parsed owner information, if present.

    Examples
    --------
    >>> SearchResult(applicant=ApplicantData(first_name="Jane"))
    SearchResult(applicant=ApplicantData(first_name='Jane', last_name=None, email=None, phone_number=None, address=None), owner=None)
    """

    applicant: Optional[ApplicantData] = None
    owner: Optional[OwnerData] = None
