"""Base class for Playwright permit list scrapers."""

from permits_scraper.scrapers.base.permit_list import PermitListBaseScraper
from permits_scraper.scrapers.base.playwright import PlaywrightBaseScraper

    
class PlaywrightPermitListBaseScraper(PermitListBaseScraper, PlaywrightBaseScraper):
    """Base class for Playwright permit list scrapers."""

