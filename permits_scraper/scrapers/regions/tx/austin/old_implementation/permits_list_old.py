"""Austin (TX) advanced date range scraper.

This module implements a SOLID, class-based scraper that:
- Opens the Austin Citizen Portal public search,
- Switches to "Property / Project Name / Types / Date Range",
- Fills Start/End Date for each chunk in a date range,
- Clicks the "Search" button for each chunk.

Design
------
- Single Responsibility: class focuses on executing chunked date-range searches.
- Open/Closed: selectors and waits are encapsulated; easy to extend with result parsing later.
- Liskov Substitution: mirrors the interface style of Playwright-based scrapers used elsewhere.
- Interface Segregation: minimal surface (headless/base_url, async scrape, helpers).
- Dependency Inversion: scraping depends on abstractions (locators, helpers) not rigid flows.
"""

from __future__ import annotations

from pathlib import Path
import re
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple, Callable

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    async_playwright,
    expect,
)

# If using the same base as other Austin scrapers:
from permits_scraper.scrapers.base.playwright_permit_list import PlaywrightPermitListBaseScraper
from permits_scraper.schemas.permit_range_log import PermitRangeLog
import logging


class PermitListScraper(PlaywrightPermitListBaseScraper):
    """Scraper for Austin (TX) public search advanced date-range navigation.

    Methods
    -------
    scrape(start_date, end_date, days_per_step) -> List[PermitRangeLog]:
        Sync wrapper to call the async method.
    scrape_async(start_date, end_date, days_per_step) -> List[PermitRangeLog]:
        Runs the searches over chunked date ranges and returns per-chunk metadata.
    set_headless(value: bool) -> None:
        Toggle headless mode.
    set_base_url(value: str) -> None:
        Override base public search URL.
    """

    _base_url: str = "https://abc.austintexas.gov/citizenportal/app/public-search"
    _region: str = "tx"
    _city: str = "austin"

    # ------------------------
    # Public API
    # ------------------------
    def scrape(
        self,
        start_date: date,
        end_date: date,
        days_per_step: int = -1,
        progress_callback: Optional[Callable[[int, int, Optional[int]], None]] = None,
    ) -> List[PermitRangeLog]:
        """Sync wrapper to call the async method."""
        return super().scrape(start_date, end_date, days_per_step, progress_callback)

    async def scrape_async(
        self,
        start_date: date,
        end_date: date,
        days_per_step: int = -1,
        progress_callback: Optional[Callable[[int, int, Optional[int]], None]] = None,
    ) -> List[PermitRangeLog]:
        """Execute advanced-date-range searches in chunks and return basic metadata per chunk."""
        async with async_playwright() as pw:
            browser: Browser = await pw.chromium.launch(headless=self._headless)
            context: BrowserContext = await browser.new_context()
            await self._configure_network_blocking(context)
            page: Page = await context.new_page()

            try:
                await self._goto_search_page(page)
                await self._open_advanced_tab(page)

                chunks = self._iter_chunks(start_date, end_date, days_per_step)

                outputs: List[PermitRangeLog] = []

                for chunk_start, chunk_end in chunks:
                    try:
                        success = False
                        await self._fill_dates_and_search(page, chunk_start, chunk_end)
                        total_count = await self._get_total_results_count(page)
                        if total_count is None:
                            success = False
                            continue
                        export_path = await self._export_results(page, chunk_start, chunk_end)

                        result = PermitRangeLog(
                            number_of_permits=total_count,
                            start_date=chunk_start.isoformat(),
                            end_date=chunk_end.isoformat(),
                            output_path=str(export_path) if export_path is not None else None
                        )
                        self.persist_result(chunk_start.isoformat(), chunk_end.isoformat(), result)

                        success = True
                        outputs.append(result)
                    except Exception as e:
                        logging.exception(
                            "Austin list chunk failed: %s-%s:\n%s",
                            chunk_start.isoformat(),
                            chunk_end.isoformat(),
                            e
                        )
                        success = False
                        continue
                    finally:
                        success_chunk = 1 if success else 0
                        failed_chunk = 1 if not success else 0
                        self.process_progress_callback(progress_callback, success_chunk, failed_chunk, len(chunks))

                return outputs
            except Exception as e:
                logging.exception("Austin scrape_async fatal error: %s to %s:\n%s", start_date, end_date, e)
                raise
            finally:
                await browser.close()

    # ------------------------
    # Navigation
    # ------------------------
    async def _goto_search_page(self, page: Page) -> None:
        try:
            await page.goto(self._base_url, wait_until="domcontentloaded")
        except Exception as e:
            logging.exception("Austin navigate failed: %s:\n%s", self._base_url, e)
            raise

    async def _open_advanced_tab(self, page: Page, timeout_ms: int = 20000) -> None:
        """
        Click the “Property / Project Name / Types / Date Range” tab.

        Note: In markup, the title may appear without slashes (e.g., "Property Project Name Types Date Range"),
        so use a regex that tolerates optional slashes and whitespace.
        """
        # Prefer role-based, with tolerant name matching
        tab = page.locator('a[title="Property / Project Name / Types / Date Range"]')

        try:
            await expect(tab).to_be_visible(timeout=timeout_ms)
            await tab.click()
        except Exception:
            logging.exception("Austin advanced tab open failed")
            raise

        # Wait for the Start/End Date inputs in the advanced panel
        await expect(
            page.locator('input[title="Start Date"]').or_(page.locator("#inDateFromID"))
        ).to_be_visible(timeout=timeout_ms)
        await expect(
            page.locator('input[title="End Date"]').or_(page.locator("#inDateToID"))
        ).to_be_visible(timeout=timeout_ms)

    # ------------------------
    # Actions
    # ------------------------
    async def _fill_dates_and_search(self, page: Page, start: date, end: date, timeout_ms: int = 20000) -> None:
        """Fill Start/End Date and submit Search for the current chunk."""
        # UI uses mm/dd/yyyy display format
        start_str = start.strftime("%m/%d/%Y")
        end_str = end.strftime("%m/%d/%Y")

        start_input = page.locator('input[title="Start Date"]').or_(page.locator("#inDateFromID"))
        end_input = page.locator('input[title="End Date"]').or_(page.locator("#inDateToID"))
        try:
            await expect(start_input).to_be_visible(timeout=timeout_ms)
            await expect(end_input).to_be_visible(timeout=timeout_ms)
        except Exception:
            logging.exception("Austin date inputs not visible")
            raise

        await start_input.fill("")
        await start_input.fill(start_str)
        await end_input.fill("")
        await end_input.fill(end_str)

        search_btn = page.get_by_role("button", name=re.compile(r"^Search$", re.I)).or_(
            page.locator('button[title="Search"]')
        )
        try:
            await expect(search_btn).to_be_visible(timeout=timeout_ms)
            await search_btn.click()
        except Exception:
            logging.exception("Austin Search click failed")
            raise

    async def _get_total_results_count(self, page: Page, timeout_ms: int = 30000) -> Optional[int]:
        """
        Read the total result count displayed after a search from span.total-result-count.
        """
        count_span = page.locator("span.total-result-count")
        try:
            await expect(count_span).to_be_visible(timeout=timeout_ms)
        except Exception:
            return
        if await count_span.count() > 1:
            count_span = count_span.first()
        elif await count_span.count() == 0:
            return
        raw = (await count_span.inner_text()).strip()
        m = re.search(r"(\d[\d,]*)", raw)
        if not m:
            return
        try:
            return int(m.group(1).replace(",", ""))
        except Exception as e:
            logging.exception("Austin total count parse failed: %s:\n%s", raw, e)
            return

    async def _export_results(
        self,
        page: Page,
        chunk_start: date,
        chunk_end: date,
        timeout_ms: int = 60_000,
    ) -> Optional[Path]:
        """
        Click "Export Results" and save the downloaded file into self._result_output_dir().
        """
        export_btn = page.locator('button[title="Export Results"]')
        await expect(export_btn).to_be_visible(timeout=timeout_ms)

        out_dir: Path = self._result_output_dir()

        # Expect a download on click
        async with page.expect_download(timeout=timeout_ms) as dl_info:
            await export_btn.click()
        download = await dl_info.value

        # Compose filename
        suggested = download.suggested_filename or "export.csv"
        # Prefix with chunk metadata
        prefix = f"list_chunk_{chunk_start:%Y%m%d}_{chunk_end:%Y%m%d}_"
        # Avoid duplicating extension if suggested already has one
        export_name = prefix + suggested
        dest = out_dir / export_name

        try:
            await download.save_as(str(dest))
            return dest
        except Exception:
            # Fallback: try default save
            try:
                await download.save_as(str(out_dir / suggested))
                return out_dir / suggested
            except Exception:
                return None
