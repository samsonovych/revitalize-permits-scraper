"""Schema package for permits scraper.

Exposes commonly used models for convenient imports.
"""

from .contacts import ApplicantData, OwnerData
from .permit_record import PermitRecord

__all__ = [
    "ApplicantData",
    "OwnerData",
    "PermitRecord",
]
