"""Base class for Playwright permit details scrapers."""

from permits_scraper.scrapers.base.permit_details import PermitDetailsBaseScraper
from permits_scraper.scrapers.base.playwright import PlaywrightBaseScraper


class PlaywrightPermitDetailsBaseScraper(PermitDetailsBaseScraper, PlaywrightBaseScraper):
    """Base class for Playwright permit details scrapers."""

