"""Associated person schema for Arlington (TX).

This module defines a simple Pydantic model used to represent an
associated person entry in Arlington permit details. The model is kept
intentionally small and permissive so that new, unexpected attributes
can be accommodated without breaking the pipeline.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class AssociatedPerson(BaseModel):
    """Associated person data model.

    Summary line.

    Extended description of function.

    Parameters
    ----------
    type : Optional[str]
        Person's role/type in the permit (e.g., Applicant, Owner).
    name : Optional[str]
        Full name or company name, as rendered in the table.
    address : Optional[str]
        Address line(s) as a single normalized string.
    email : Optional[str]
        Contact email address.
    phone_number : Optional[str]
        Contact phone number.

    Returns
    -------
    AssociatedPerson
        Pydantic model carrying associated person attributes.

    Examples
    --------
    >>> AssociatedPerson(type="Applicant", name="Jane Doe")
    AssociatedPerson(type='Applicant', name='Jane Doe', address=None, email=None, phone_number=None)
    """

    type: Optional[str] = None
    name: Optional[str] = None
    address: Optional[str] = None
    email: Optional[str] = None
    phone_number: Optional[str] = None

    class Config:
        extra = "allow"


