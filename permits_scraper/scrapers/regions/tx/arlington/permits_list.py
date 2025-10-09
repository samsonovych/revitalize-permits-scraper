"""Arlington (TX) Public Search visitor.

This module provides a minimal list scraper that only navigates to the
Arlington public search page. It intentionally avoids implementing any
site-specific Playwright interactions other than visiting the URL.

Interface mirrors other list scrapers so it can be wired into the same
CLI/registry flows.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import re
from typing import List, Optional, Callable
import pandas as pd
from pandas.util.version import Tuple
from pydantic import PrivateAttr

from playwright.async_api import Browser, BrowserContext, Page, Locator, async_playwright, expect

from permits_scraper.scrapers.base.playwright_permit_list import PlaywrightPermitListBaseScraper
from permits_scraper.schemas.permit_range_log import PermitRangeLog
import logging


class PermitListScraper(PlaywrightPermitListBaseScraper):
    """Minimal Arlington (TX) list scraper that only opens the public search URL.

    Parameters
    ----------
    None
        This scraper accepts the standard list-scraper parameters when invoked
        via ``scrape``/``scrape_async`` but ignores date inputs on purpose.

    Notes
    -----
    This class avoids any Playwright actions beyond navigation. It is useful
    as a connectivity/availability check or as a scaffold to extend later.

    Methods
    -------
    scrape(start_date, end_date, progress_callback) -> List[PermitRangeLog]
        Synchronous wrapper that delegates to the async implementation.
    scrape_async(start_date, end_date, progress_callback) -> List[PermitRangeLog]
        Launches a headless browser (by default) and navigates to the Arlington
        public search page, returning a single result entry with zero permits
        and no output path.
    set_headless(value)
        Toggle headless mode.
    set_base_url(value)
        Override base URL if needed.

    Examples
    --------
    >>> from datetime import date
    >>> scraper = PermitListScraper()
    >>> scraper.set_headless(True)
    >>> results = scraper.scrape(date(2024, 1, 1), date(2024, 1, 31))
    >>> len(results) >= 1
    True
    """

    _region: str = "tx"
    _city: str = "arlington"
    _base_url: str = "https://ap.arlingtontx.gov/AP/sfjsp?interviewID=PublicSearch&btnclk=search"
    _permit_types: List[str] = PrivateAttr(default=["Residential Permit"])
    _delay_between_chunks: int = PrivateAttr(default=1000)

    def scrape(
        self,
        start_date: date,
        end_date: date,
        days_per_step: int = -1,
        progress_callback: Optional[Callable[[int, int, Optional[int]], None]] = None,
    ) -> List[PermitRangeLog]:
        """Sync wrapper to call the async method.

        Parameters
        ----------
        start_date : date
            Inclusive start date.
        end_date : date
            Inclusive end date.
        days_per_step : int, default=-1
            Chunk size in days (-1 for full range).
        progress_callback : Optional[Callable[[int, int, Optional[int]], None]], default=None
            Optional progress callback receiving (success_inc, failed_inc, total_chunks).

        Returns
        -------
        List[PermitRangeLog]
            A single entry indicating navigation success with zero permits.
        """
        return super().scrape(start_date, end_date, days_per_step, progress_callback)

    async def scrape_async(
        self,
        start_date: date,
        end_date: date,
        days_per_step: int = -1,
        progress_callback: Optional[Callable[[int, int, Optional[int]], None]] = None,
    ) -> List[PermitRangeLog]:
        """Navigate to the Arlington public search page.

        Extended description of function.

        Parameters
        ----------
        start_date : date
            Inclusive start date.
        end_date : date
            Inclusive end date.
        days_per_step : int, default=-1
            Chunk size in days (-1 for full range).
        progress_callback : Optional[Callable[[int, int, Optional[int]], None]], default=None
            Optional progress callback receiving (success_inc, failed_inc, total_chunks).

        Returns
        -------
        List[PermitRangeLog]
            One item capturing the attempted date window and navigation result.
        """
        try:
            async with async_playwright() as pw:
                browser: Browser = await pw.chromium.launch(headless=self._headless)
                context: BrowserContext = await browser.new_context()
                page: Page = await context.new_page()

                # No further interactions — this scraper only visits the page.
                await self._goto_search_page(page)
                await self._goto_search_permit_page(page)
                outputs: List[PermitRangeLog] = []

                skip_application_date_selection = False

                for permit_type in self._permit_types:
                    await self._select_permit_type(page, permit_type)
                    chunks = self._iter_chunks(start_date, end_date, days_per_step)

                    for chunk in chunks:
                        try:
                            chunk_start, chunk_end = chunk

                            await self._set_date_range(page, chunk_start, chunk_end, skip_application_date_selection)
                            await self._click_search_permits_button(page)
                            export_path, number_of_permits = await self._export_results(
                                page, chunk_start, chunk_end, permit_type)
                            result = PermitRangeLog(
                                number_of_permits=number_of_permits,
                                start_date=chunk_start.strftime('%Y-%m-%d'),
                                end_date=chunk_end.strftime('%Y-%m-%d'),
                                output_path=export_path,
                            )
                            outputs.append(result)

                            # Progress reporting and persistence
                            self.process_progress_callback(progress_callback, 1, 0, len(chunks) * len(self._permit_types))
                            self.persist_result(chunk_start, chunk_end, result)

                            skip_application_date_selection = True
                            await page.wait_for_timeout(self._delay_between_chunks)
                        except Exception as e:
                            logging.exception(
                                "Arlington list chunk failed: %s-%s:\n%s",
                                chunk_start.strftime('%Y-%m-%d'),
                                chunk_end.strftime('%Y-%m-%d'),
                                e,
                            )
                            self.process_progress_callback(progress_callback, 0, 1, len(chunks) * len(self._permit_types))
                            await page.wait_for_timeout(self._delay_between_chunks)
                            continue

            return outputs
        except Exception as e:
            logging.exception(
                "Arlington scrape_async fatal error: %s to %s:\n%s",
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d'),
                e,
            )
            raise
        finally:
            await browser.close()

    async def _goto_search_page(self, page: Page) -> None:
        try:
            await page.goto(self._base_url, wait_until="domcontentloaded")
        except Exception as e:
            logging.exception("Arlington navigate failed: %s:\n%s", self._base_url, e)
            raise

    async def _goto_search_permit_page(self, page: Page, timeout_ms: int = 30000) -> None:
        selector = page.get_by_text("Search for a Permit", exact=False)
        await selector.click()
        await page.wait_for_load_state("networkidle", timeout=timeout_ms)

    async def _select_permit_type(self, page: Page, permit_type: str, timeout_ms: int = 30000) -> None:        # Get element by xpath
        selector = page.get_by_label("Permit Type", exact=False)
        await expect(selector).to_be_visible(timeout=timeout_ms)
        await selector.select_option(permit_type)
        await page.wait_for_load_state("networkidle", timeout=timeout_ms)

    async def _set_date_range(
        self,
        page: Page,
        start_date: date,
        end_date: date,
        skip_application_date_selection: bool = False,
        timeout_ms: int = 30_000
    ) -> None:
        # Ensure the correct tab is active so hidden duplicates don’t match
        await page.get_by_role("tab", name="Search for a Permit").click()  # activates the tabpanel [accessible role]

        if not skip_application_date_selection:
            # Set "Application Date" to Custom Range (value="custom")
            app_date_mode = page.get_by_label("Application Date")
            await expect(app_date_mode).to_be_visible(timeout=timeout_ms)
            await app_date_mode.select_option(value="custom")

        # Find the Start/End inputs following the "Application Date" section (no ids/classes)
        start_input = page.locator(
            'xpath=//label[.//span[normalize-space()="Application Date"]]'
            '/following::label[.//span[normalize-space()="Start Date"]][1]'
            '/following::input[@type="text"][1]'
        )
        end_input = page.locator(
            'xpath=//label[.//span[normalize-space()="Application Date"]]'
            '/following::label[.//span[normalize-space()="End Date"]][1]'
            '/following::input[@type="text"][1]'
        )

        # Adjust end date if today
        if end_date == date.today():
            end_date = end_date - timedelta(days=1)

        await expect(start_input).to_be_visible(timeout=timeout_ms)
        await expect(end_input).to_be_visible(timeout=timeout_ms)

        # Set start and end date
        await start_input.evaluate("(el, date) => el.value = date", start_date.strftime('%m/%d/%Y'))
        await end_input.evaluate("(el, date) => el.value = date", end_date.strftime('%m/%d/%Y'))

        await page.wait_for_load_state("networkidle", timeout=timeout_ms)

    async def _click_search_permits_button(self, page: Page, timeout_ms: int = 30_000) -> None:
        # Scope to the div.row that contains BOTH buttons
        current_tab = await self._get_current_tab(page)
        row = current_tab.filter(has=page.locator("div.row")) \
            .filter(has=page.locator('button:has-text("Clear Search Criteria")')) \
            .filter(has=page.locator('button:has-text("Search")'))

        search_btn = row.locator("button").filter(has_text=re.compile(r"^Search$", re.I))
        await expect(search_btn).to_be_visible(timeout=timeout_ms)
        await search_btn.click()
        await page.wait_for_load_state("networkidle", timeout=timeout_ms)

    async def _export_results(
        self,
        page: Page,
        chunk_start: date,
        chunk_end: date,
        permit_type: str,
        timeout_ms: int = 30_000,
    ) -> Tuple[str, int]:
        await page.wait_for_load_state("networkidle", timeout=timeout_ms)
        # Find download button with EXACT text "CSV"
        current_tab = await self._get_current_tab(page)
        row = current_tab.filter(has=page.locator("div.row")) \
            .filter(has=page.locator('button:has-text("CSV")'))
        export_btn = row.locator('button:has-text("CSV")')

        await expect(export_btn).to_be_visible(timeout=timeout_ms)
        out_dir: Path = self._result_output_dir()

        async with page.expect_download(timeout=timeout_ms) as dl_info:
            await export_btn.click()
        download = await dl_info.value

        # Compose filename
        permit_type_stripped = permit_type.lower().replace(" ", "_")
        filename = f"list_chunk_{chunk_start:%Y%m%d}_{chunk_end:%Y%m%d}_{permit_type_stripped}.csv"
        # Avoid duplicating extension if suggested already has one
        dest = out_dir / filename

        await download.save_as(str(dest))
        df = pd.read_csv(str(dest))
        df["Permit Type"] = permit_type
        df.to_csv(str(dest), index=False)
        return str(dest), len(df)

    async def _get_current_tab(self, page: Page, timeout_ms: int = 30000) -> Locator:
        current_tab = page.locator("div.tab-pane.active")
        await expect(current_tab).to_be_visible(timeout=timeout_ms)
        return current_tab
