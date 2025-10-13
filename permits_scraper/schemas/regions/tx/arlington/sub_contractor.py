"""Sub-contractor schema for Arlington (TX).

This module defines a Pydantic model for sub-contractor entries that are
listed within Arlington permit details. The model allows extra fields to
support forward-compatibility with portal changes.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict


class SubContractor(BaseModel):
    """Sub-contractor data model.

    Summary line.

    Extended description of function.

    Parameters
    ----------
    type : Optional[str]
        Contractor category/type.
    company_name : Optional[str]
        Company or business name.
    point_of_contact : Optional[str]
        Designated point of contact.
    phone_number : Optional[str]
        Primary phone number.
    email : Optional[str]
        Contact email address.
    city_registration_number : Optional[str]
        City registration identifier, if provided.
    effective_from : Optional[str]
        Effective date (start).
    effective_to : Optional[str]
        Effective date (end).
    inspection_notifications : Optional[str]
        Notes or flags about inspection notifications.

    Returns
    -------
    SubContractor
        Pydantic model carrying sub-contractor attributes.

    Examples
    --------
    >>> SubContractor(type="Electrical", company_name="ACME Electric")
    SubContractor(type='Electrical', company_name='ACME Electric', point_of_contact=None, phone_number=None, email=None, city_registration_number=None, effective_from=None, effective_to=None, inspection_notifications=None)
    """

    type: Optional[str] = None
    company_name: Optional[str] = None
    point_of_contact: Optional[str] = None
    phone_number: Optional[str] = None
    email: Optional[str] = None
    city_registration_number: Optional[str] = None
    effective_from: Optional[str] = None
    effective_to: Optional[str] = None
    inspection_notifications: Optional[str] = None

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")
