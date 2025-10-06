"""Post-processing framework for permits datasets.

This package provides a base class contract, a registry with auto-discovery,
and a small CLI to run post-processors over scraped datasets.
"""

from .models import PostProcessingResult

__all__ = ["PostProcessingResult"]


