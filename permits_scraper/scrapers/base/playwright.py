"""Base class for Playwright scrapers."""

from pydantic import BaseModel, PrivateAttr
from playwright.async_api import BrowserContext, Route
from abc import ABC

class PlaywrightBaseScraper(ABC, BaseModel):
    """Base class for Playwright scrapers."""

    _headless: bool = PrivateAttr(default=True)
    _base_url: str = PrivateAttr(...)

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

