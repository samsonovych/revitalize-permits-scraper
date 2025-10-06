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

from typing import List, Optional, Callable, Dict, Any
import pandas as pd
from pydantic import PrivateAttr, Field, BaseModel
from datetime import date

from sodapy import Socrata

# If using the same base as other Austin scrapers:
from permits_scraper.scrapers.base.permit_list import PermitListBaseScraper
from permits_scraper.schemas.permit_range_log import PermitRangeLog
import logging


class PermitListScraper(PermitListBaseScraper):
    """Scraper for Austin (TX) public search advanced date-range navigation.

    Methods
    -------
    scrape(start_date, end_date) -> List[PermitRangeLog]:
        Sync wrapper to call the async method.
    scrape_async(start_date, end_date) -> List[PermitRangeLog]:
        Runs the searches over chunked date ranges and returns per-chunk metadata.
    """

    _region: str = "tx"
    _city: str = "austin"

    _limit_per_request: int = 10_000
    _dataset_id: str = "3syk-w9eu"

    _client: Socrata = PrivateAttr(default=Socrata("data.austintexas.gov", None), init=True)

    # -------- Input schema override --------
    class Inputs(BaseModel):  # type: ignore[valid-type]
        start_date: date = Field(description="Start date (DD/MM/YYYY or YYYY-MM-DD)")
        end_date: date = Field(description="End date (DD/MM/YYYY or YYYY-MM-DD)")

    @classmethod
    def get_input_schema(cls):  # type: ignore[override]
        return cls.Inputs

    def scrape_with_inputs(self, inputs):  # type: ignore[override]
        return self.scrape(inputs.start_date, inputs.end_date)

    # ------------------------
    # Public API
    # ------------------------
    def scrape(
        self,
        start_date: date,
        end_date: date,
        progress_callback: Optional[Callable[[int, int, int], None]] = None,
    ) -> List[PermitRangeLog]:
        """Sync wrapper to call the async method."""
        return super().scrape(start_date, end_date, progress_callback)

    async def scrape_async(
        self,
        start_date: date,
        end_date: date,
        progress_callback: Optional[Callable[[int, int, int], None]] = None,
    ) -> List[PermitRangeLog]:
        """Fetch Austin permits via Socrata in date chunks.

        Parameters
        ----------
        start_date : date
            Inclusive start date. Flexible formats supported (e.g., DD/MM/YYYY, YYYY-MM-DD).
        end_date : date
            Inclusive end date. Flexible formats supported (e.g., DD/MM/YYYY, YYYY-MM-DD).
        progress_callback : Optional[Callable[[int, int, int], None]], default=None
            Callback invoked after each chunk with (success_inc, failed_inc, total_chunks).

        Returns
        -------
        List[PermitRangeLog]
            One entry per processed chunk containing the number of permits and metadata.
        """

        try:
            permits: List[Dict[str, Any]] = []
            offset = 0

            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')

            while True:
                page_permits = self._client.get(
                    dataset_identifier=self._dataset_id,
                    select="*",
                    where=(
                        f"applieddate >= '{start_date_str}' "
                        f"AND applieddate <= '{end_date_str}'"
                    ),
                    order="applieddate ASC",
                    limit=self._limit_per_request,
                    offset=offset
                )
                permits.extend(page_permits)
                if len(page_permits) < self._limit_per_request:
                    break
                offset += self._limit_per_request

            results_df = pd.DataFrame.from_records(permits)
            export_path = self._result_output_dir() / f"{start_date_str}_{end_date_str}.csv"
            results_df.to_csv(export_path, index=False)
            
            result = PermitRangeLog(
                number_of_permits=len(permits),
                start_date=start_date_str,
                end_date=end_date_str,
                output_path=str(export_path)
            )
            self.process_progress_callback(progress_callback, 1, 0, 1)
            self.persist_result(start_date, end_date, result)

            return [result]
        except Exception as e:
            logging.exception("Austin scrape_async fatal error: %s to %s:\n%s", start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), e)
            raise
        finally:
            try:
                self._client.close()  # type: ignore[attr-defined]
            except Exception:
                pass
