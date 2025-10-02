"""Schema package for permits scraper.

Exposes commonly used models for convenient imports.
"""

from .contacts import ApplicantData, OwnerData
from .search import SearchResult

__all__ = [
    "ApplicantData",
    "OwnerData",
    "SearchResult",
]
