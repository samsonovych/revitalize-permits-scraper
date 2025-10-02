"""Base class for all scrapers."""

from abc import ABC, abstractmethod
from typing import Any
from pydantic import BaseModel


class BaseScraper(ABC, BaseModel):
    """Base class for all scrapers."""

    @abstractmethod
    def scrape(self, **kwargs: Any) -> BaseModel:
        """Scrape the data from the URL."""
        pass
