"""Texas San Antonio permit details scraper.

This module implements a SOLID, class-based scraper for extracting applicant
and owner contact data for permits from the City of San Antonio Accela portal.

Design
------
- Single Responsibility: the class focuses solely on fetching and parsing
  details for a single permit by application number.
- Open/Closed: parsing helpers are small, cohesive, and can be extended
  without modifying the high-level scraping flow.
- Liskov Substitution: the scraper conforms to the `BaseScraper` interface
  and returns a Pydantic `BaseModel` (`SearchResult`).
- Interface Segregation: the base interface remains minimal; consumers depend
  only on what they need.
- Dependency Inversion: the scraping logic depends on abstractions (selectors
  and parsing helpers) rather than tightly-coupled code paths.
"""

from __future__ import annotations

import asyncio
import re
from typing import Optional, Dict, List
import json
import os
from pathlib import Path
from uuid import uuid4

from dotenv.main import logger
from pydantic import PrivateAttr
from playwright.async_api import Browser, BrowserContext, Locator, Page, async_playwright

from permits_scraper.schemas.contacts import ApplicantData, OwnerData
from permits_scraper.schemas.search import SearchResult
from permits_scraper.scrapers.base import BaseScraper


class PermitDetailsScraper(BaseScraper):
    """Scraper for San Antonio (TX) Accela permit details.

    Parameters
    ----------
    None
        All runtime configuration is managed via private attributes.

    Private Attributes
    ------------------
    _headless : bool
        Whether the browser runs in headless mode. Defaults to ``True``.
    _base_url : str
        Base URL of the Accela search page used to look up permits.

    Notes
    -----
    - This class provides a synchronous ``scrape`` method that internally
      runs asynchronous Playwright logic.
    - When executed in environments with an active event loop (e.g., Jupyter),
      calling ``scrape`` may raise a runtime error. In that case, prefer using
      ``await scrape_async(...)``.

    See Also
    --------
    BaseScraper : Minimal scraping interface.
    SearchResult : Return schema combining parsed contacts.

    Examples
    --------
    >>> scraper = PermitDetailsScraper()
    >>> result = scraper.scrape("MEP-TRD-APP25-33127895")
    >>> isinstance(result, SearchResult)
    True
    """

    _headless: bool = PrivateAttr(default=True)
    _base_url: str = PrivateAttr(
        default=(
            "https://aca-prod.accela.com/COSA/Cap/CapHome.aspx?module=Building&TabName=Building"
        )
    )

    def scrape(self, application_numbers: List[str]) -> Dict[str, SearchResult]:  # type: ignore[override]
        """Scrape permit details for a single application number.

        Parameters
        ----------
        application_numbers : List[str]
            The application number to search for on the Accela portal.

        Returns
        -------
        Dict[str, SearchResult]
            Parsed applicant and owner contact data.
        """
        try:
            return asyncio.run(self.scrape_async(application_numbers))
        except RuntimeError as exc:
            if "asyncio.run() cannot be called from a running event loop" in str(exc):
                raise RuntimeError(
                    "scrape() cannot be called from an active event loop; "
                    "use `await scrape_async(application_numbers)` instead."
                ) from exc
            raise

    async def scrape_async(self, application_numbers: List[str]) -> Dict[str, SearchResult]:
        """Asynchronously scrape permit details for a single application.

        Parameters
        ----------
        application_numbers : List[str]
            The application number to search for on the Accela portal.

        Returns
        -------
        Dict[str, SearchResult]
            Parsed applicant and owner contact data.
        """
        async with async_playwright() as playwright:
            browser: Browser = await playwright.chromium.launch(headless=self._headless)
            context = await browser.new_context()
            await self._configure_network_blocking(context)
            page: Page = await context.new_page()

            try:
                results: Dict[str, SearchResult] = {}
                for application_number in application_numbers:
                    await self._goto_search_page(page)
                    await self._submit_search(page, application_number)

                    # Wait until the page title appears
                    await page.wait_for_selector('#ctl00_PlaceHolderMain_shPermitDetail_lblSectionTitle', state='visible')

                    applicant: Optional[ApplicantData] = await self._extract_applicant(page)
                    owner: Optional[OwnerData] = await self._extract_owner(page)

                    result = SearchResult(applicant=applicant, owner=owner)
                    results[application_number] = result

                    # Persist per-permit result immediately as a crash-safe fallback
                    self._persist_result(application_number, result)

                return results
            finally:
                await browser.close()

    @property
    def headless(self) -> bool:
        """Return current headless mode setting."""
        return self._headless

    @property
    def base_url(self) -> str:
        """Return the configured base URL for searches."""
        return self._base_url

    def set_headless(self, value: bool) -> None:
        """Set headless mode.

        Parameters
        ----------
        value : bool
            Whether to run the Chromium browser in headless mode.
        """
        self._headless = value

    def set_base_url(self, value: str) -> None:
        """Set the base search URL.

        Parameters
        ----------
        value : str
            Fully qualified search page URL for the Accela portal.
        """
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

        async def handler(route):  # type: ignore[no-untyped-def]
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

    def _result_output_dir(self) -> Path:
        """Return the output directory for per-permit results.

        Returns
        -------
        Path
            Directory path ``permits_scraper/data/regions/tx/san_antonio`` relative
            to the package root. The directory is created if it does not exist.
        """
        pkg_root = Path(__file__).resolve().parents[4]
        out_dir = pkg_root / "data" / "regions" / "tx" / "san_antonio"
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir

    def _persist_result(self, application_number: str, result: SearchResult) -> None:
        """Atomically persist a single permit result to a JSON file.

        This writes one JSON file per permit (``<permit_id>.json``) using an
        atomic replace to avoid partial writes and cross-process corruption.

        Parameters
        ----------
        application_number : str
            The permit/application identifier used as the filename stem.
        result : SearchResult
            The parsed result to serialize and persist.
        """
        try:
            out_dir = self._result_output_dir()
            final_path = out_dir / f"{application_number}.json"

            # Serialize result to JSON (pydantic v2)
            payload = json.dumps(result.model_dump(mode="json"), ensure_ascii=False, sort_keys=True, indent=2)

            # Write to a temp file in the same directory, then atomically replace
            tmp_name = f".{application_number}.{uuid4().hex}.tmp"
            tmp_path = out_dir / tmp_name
            tmp_path.write_text(payload, encoding="utf-8")
            os.replace(tmp_path, final_path)
        except Exception as e:
            # Best-effort persistence; do not fail the scrape due to IO errors
            try:
                logger.error(f"Failed to persist result for {application_number}: {e}")
            except Exception:
                pass

    async def _goto_search_page(self, page: Page) -> None:
        """Navigate to the base search page and wait for network idle."""
        await page.goto(self._base_url, wait_until="domcontentloaded")

    async def _submit_search(self, page: Page, application_number: str) -> None:
        """Fill the permit number and submit the search form."""
        # Wait for the form to be ready
        await page.wait_for_selector('input[name="ctl00$PlaceHolderMain$generalSearchForm$txtGSPermitNumber"]', state='visible')

        application_number_field: Locator = page.locator(
            'input[name="ctl00$PlaceHolderMain$generalSearchForm$txtGSPermitNumber"]'
        )
        await application_number_field.fill(application_number)

        # Try with different selectors
        search_button = page.locator('a[id="ctl00_PlaceHolderMain_btnNewSearch"]')
        if await search_button.count() == 0:
            search_button = page.locator('a:has-text("Search")')

        await search_button.click()

    async def _extract_applicant(self, page: Page) -> Optional[ApplicantData]:
        """Extract applicant data from the page, if present."""
        try:
            section = page.locator('h1:has-text("Applicant:")').locator("..").locator("..")
            if await section.count() == 0:
                return None
            data = ApplicantData()

            # Names
            fn = section.locator('.contactinfo_firstname')
            ln = section.locator('.contactinfo_lastname')
            if await fn.count():
                data.first_name = (await fn.inner_text()).strip()
            if await ln.count():
                data.last_name = (await ln.inner_text()).strip()

            txt = await section.inner_text()

            # Email
            m = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', txt)
            if m:
                data.email = m.group(0)

            # Phone: use label "Primary Phone" dt/dd structure
            phone_dd = section.locator('td:has-text("Primary Phone")')
            if await phone_dd.count():
                text_parts = (await phone_dd.first.inner_text()).splitlines()
                # Find the next part after "Primary Phone"
                phone_number = None
                for part in text_parts:
                    if "Primary Phone" in part:
                        phone_number = text_parts[text_parts.index(part) + 1]
                        break
                if phone_number:
                    data.phone_number = phone_number.strip()

            # Mailing address
            addr_dd = section.locator('td:has-text("Mailing")')
            if await addr_dd.count():
                text_parts = (await addr_dd.first.inner_text()).splitlines()
                # Find the next part after "Mailing"
                mailing_address = None
                for part in text_parts:
                    if "Mailing" in part:
                        mailing_address = " ".join(text_parts[text_parts.index(part) + 1:-1])
                        break
                data.address = mailing_address.strip()

            return data
        except Exception as e:
            logger.error(f"Error extracting applicant: {e}")
            return None

    async def _extract_owner(self, page: "Page") -> Optional[OwnerData]:
        """
        Extract owner data from the Owner block using Playwright's Page.

        This method locates the owner information section in the permit details page,
        extracts the owner's name and address, and returns an OwnerData object.

        Parameters
        ----------
        page : Page
            Playwright Page object representing the loaded permit details page.

        Returns
        -------
        Optional[OwnerData]
            Extracted owner data, or None if not found or extraction fails.

        See Also
        --------
        OwnerData : Pydantic model for owner contact information

        Examples
        --------
        >>> owner = await self._extract_owner(page)
        >>> print(owner)
        OwnerData(first_name='JOHN', last_name='SMITH', company_name=None, address='123 Main St, San Antonio, TX')
        """
        try:
            # Find the Owner label span by stable id prefix, then its sibling span holding the table content
            owner_label = page.locator('span[id^="ctl00_PlaceHolderMain_PermitDetailList1_per_permitdetail_label_owner"]')
            if await owner_label.count() == 0:
                return None

            # The owner section is two ancestors up from the label, then the first descendant table
            section = owner_label.locator('..').locator('..')
            if await section.count() == 0:
                return None

            # The innermost table has two rows: first row = name cell, second row = address cell
            table = section.locator('xpath=.//table//table//table')
            if await table.count() == 0:
                return None

            td_elements = table.locator('td')
            if await td_elements.count() < 2:
                return None

            name_cell = td_elements.nth(0)
            addr_cell = td_elements.nth(1)
            name_text = (await name_cell.inner_text()).strip()
            # Remove the trailing asterisk and anything after it
            name_text = re.sub(r'\s*\*.*$', '', name_text).strip()
            # Heuristic: content looks like "LASTNAME FIRSTNAME"
            parts = [p for p in name_text.split() if p]
            data = OwnerData()
            if len(parts) == 2:
                data.last_name = parts[0].strip()
                data.first_name = " ".join(parts[1:]).strip()
            elif len(parts) > 2:
                data.company_name = " ".join(parts).strip()
            else:
                data.first_name = name_text.strip()

            # Address: join lines and normalize excessive commas/spaces
            raw_addr = (await addr_cell.inner_text()).strip()
            addr = " ".join(line.strip() for line in raw_addr.splitlines() if line.strip())
            addr = re.sub(r'\s+,', ',', addr)
            addr = re.sub(r',\s*,', ', ', addr)
            data.address = addr.strip()

            return data
        except Exception as e:
            logger.error(f"Error extracting owner from page: {page.url}. Error: {e}")
            return None
