"""Base class for all scrapers."""

from permits_scraper.scrapers.base import PermitsBaseScraper
from playwright.async_api import BrowserContext, Route
from typing import List, Dict
from permits_scraper.schemas.permit_record import PermitRecord
from pydantic import PrivateAttr
import asyncio

class PlaywrightBaseScraper(PermitsBaseScraper):
    """Base class for all scrapers."""

    _headless: bool = PrivateAttr(default=True)
    _base_url: str = PrivateAttr(...)

    def scrape(self, permit_numbers: List[str]) -> Dict[str, PermitRecord]:  # type: ignore[override]
        """Scrape permit details for a single permit number.

        Parameters
        ----------
        permit_numbers : List[str]
            The permit number to search for on the Accela portal.

        Returns
        -------
        Dict[str, PermitRecord]
            Parsed applicant and owner contact data.
        """
        try:
            return asyncio.run(self.scrape_async(permit_numbers))
        except RuntimeError as exc:
            if "asyncio.run() cannot be called from a running event loop" in str(exc):
                raise RuntimeError(
                    "scrape() cannot be called from an active event loop; "
                    "use `await scrape_async(permit_numbers)` instead."
                ) from exc
            raise

    @property
    def headless(self) -> bool:
        return self._headless

    @property
    def base_url(self) -> str:
        return self._base_url

    def set_headless(self, value: bool) -> None:
        self._headless = value

    def set_base_url(self, value: str) -> None:
        self._base_url = value

    async def _configure_network_blocking(self, context: BrowserContext) -> None:
        """Block non-essential resources to reduce bandwidth usage.

        Parameters
        ----------
        context : BrowserContext
            The Playwright browser context to configure.

        Notes
        -----
        Blocks resource types: ``image``, ``media``, ``font``, ``stylesheet``.
        Keeps ``document``, ``script``, ``xhr``, and ``fetch`` to ensure
        dynamic content and the DOM are still rendered.
        """
        blocked_types = {"image", "media", "font", "stylesheet"}

        async def handler(route: Route):  # type: ignore[no-untyped-def]
            try:
                if route.request.resource_type in blocked_types:
                    await route.abort()
                else:
                    await route.continue_()
            except Exception:
                try:
                    await route.continue_()
                except Exception:
                    pass

        await context.route("**/*", handler)

