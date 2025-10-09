"""Arlington (TX) public permit details scraper.

This module implements a SOLID, class-based scraper that:
- Navigates to the Arlington public portal,
- Attempts to locate and open a permit details view for each permit number,
- Extracts data from Property Summary, Property Details, General Requirements,
  Associated People and Sub-Contractors sections,
- Returns a contract-compliant :class:`permits_scraper.schemas.permit_record.PermitRecord`
  with extra fields enabled to accommodate Arlington-specific attributes.

Design
------
- Single Responsibility: Each method handles extraction of specific permit data sections
- Open/Closed: Easy to extend with new data extraction methods
- Liskov Substitution: Subclass of the Playwright details base and discoverable by the registry
- Interface Segregation: Minimal surface with focused extraction methods
- Dependency Inversion: Depends on Playwright abstractions, not implementation details
"""

from __future__ import annotations

import os
import logging
import re
from typing import Dict, List, Optional, Callable, Union
from datetime import date, datetime, timedelta
from pathlib import Path

from pydantic import PrivateAttr, Field, BaseModel
from playwright.async_api import Page, Locator, expect, Browser, BrowserContext, async_playwright

import pandas as pd

from permits_scraper.schemas.contacts import ApplicantData, OwnerData
from permits_scraper.schemas.permit_record import PermitRecord
from permits_scraper.schemas.regions.tx.arlington.associated_person import AssociatedPerson
from permits_scraper.schemas.regions.tx.arlington.sub_contractor import SubContractor
from permits_scraper.scrapers.base.playwright_permit_details import PlaywrightPermitDetailsBaseScraper


class PermitDetailsScraper(PlaywrightPermitDetailsBaseScraper):
    """Scraper for Arlington (TX) permit details extraction.

    Parameters
    ----------
    None
        Uses the standard details-scraper configuration via base methods.

    Methods
    -------
    scrape(permit_numbers, progress_callback) -> Dict[str, PermitRecord]
        Sync wrapper to call the async implementation.
    scrape_async(permit_numbers, progress_callback) -> Dict[str, PermitRecord]
        Navigates to the portal, attempts to open each permit's detail page,
        extracts data and returns a mapping of permit_number to PermitRecord.
    """

    _region: str = "tx"
    _city: str = "arlington"
    _base_url: str = "https://ap.arlingtontx.gov/AP/sfjsp?interviewID=PublicSearch&btnclk=search"
    _sort_cols_order: List[str] = PrivateAttr(
        default=[
            "Permit Type",
            "Sub Type",
            "Work Type",
            "Address / Name",
            "Status",
        ]
    )

    # -------- Input schema override --------
    class Inputs(BaseModel):  # type: ignore[valid-type]
        """Inputs for the Arlington permit details scraper."""

        permits_csv_path: Path = Field(description="Path to CSV with permit IDs to scrape")
        permits_column: str = Field(default="Permit Number", description="CSV column containing permit IDs")
        permit_overview: Path = Field(description="Path to CSV file with permits overview for Arlington")
        headless: bool = Field(default=True, description="Do you want to run headless?")
        instances: int = Field(default=1, description="How many instances to run in parallel")

    @classmethod
    def get_input_schema(cls):  # type: ignore[override]
        """Return the input schema for the Arlington permit details scraper."""
        return cls.Inputs

    def scrape(
        self,
        permit_numbers: List[str],
        permit_overview: Union[pd.DataFrame, str, Path],
        progress_callback: Optional[Callable[[int, int, Optional[int]], None]] = None,
    ) -> Dict[str, PermitRecord]:
        """Sync wrapper to call the async method.

        Parameters
        ----------
        permit_numbers : List[str]
            Permit/application identifiers to open and extract.
        permit_overview : Union[pd.DataFrame, str, Path]
            Dataframe with such columns: Permit Number, Permit Type, Sub Type, Work Type,
            Address / Name, Status.
            Retrieve this dataframe from using of Arlington Permit List Scraper.
            (permits_scraper/scrapers/regions/tx/arlington/permits_list.py)
        progress_callback : Optional[Callable[[int, int, Optional[int]], None]], default=None
            Optional progress callback receiving (success_inc, failed_inc, total).

        Returns
        -------
        Dict[str, PermitRecord]
            Mapping from permit number to the parsed record.
        """
        return super().scrape(permit_numbers, permit_overview, progress_callback)

    async def scrape_async(
        self,
        permit_numbers: List[str],
        permit_overview: Union[pd.DataFrame, str, Path],
        progress_callback: Optional[Callable[[int, int, Optional[int]], None]] = None,
    ) -> Dict[str, PermitRecord]:
        """Asynchronously open and extract permit details for each permit number.

        Parameters
        ----------
        permit_numbers : List[str]
            Permit/application identifiers to open and extract.
        permit_overview : Union[pd.DataFrame, str, Path]
            Dataframe with such columns: Permit Number, Permit Type, Sub Type, Work Type,
            Address / Name, Status.
            Retrieve this dataframe from using of Arlington Permit List Scraper.
            (permits_scraper/scrapers/regions/tx/arlington/permits_list.py)
        progress_callback : Optional[Callable[[int, int, Optional[int]], None]], default=None
            Optional progress callback receiving (success_inc, failed_inc, total).

        Returns
        -------
        Dict[str, PermitRecord]
            Mapping from permit number to its extracted :class:`PermitRecord`.
        """
        async with async_playwright() as pw:
            browser: Browser = await pw.chromium.launch(headless=self._headless)
            context: BrowserContext = await browser.new_context()
            # await self._configure_network_blocking(context)
            page: Page = await context.new_page()

            outputs: Dict[str, PermitRecord] = {}

            if isinstance(permit_overview, str):
                permit_overview = Path(permit_overview)
            if isinstance(permit_overview, Path):
                permit_overview = pd.read_csv(permit_overview)
            permit_data = permit_overview.copy()
            permit_data.drop_duplicates(subset=["Permit Number"], inplace=True)
            permit_data.sort_values(by=self._sort_cols_order, inplace=True)
            permit_data: pd.DataFrame = permit_data[permit_data["Permit Number"].isin(permit_numbers)]

            total = len(permit_numbers)
            skip_application_date_selection = False
            await self._goto_search_page(page)
            await page.wait_for_load_state("networkidle")
            await self._goto_search_permit_page(page)
            try:
                for _, row in permit_data.iterrows():
                    success = False
                    try:
                        if os.path.exists(self._result_output_dir() / f"{row['Permit Number']}.json"):
                            success = True
                            continue
                        await self._select_permit_type(page, row["Permit Type"])
                        await self._select_sub_type(page, row["Sub Type"])
                        # await self._select_status(page, row["Status"])
                        await self._select_work_type(page, row["Work Type"])
                        await self._enter_address(page, row["Address / Name"])
                        # Date is in format "MM/DD/YYYY"
                        application_date = datetime.strptime(row["Application Date"], "%m/%d/%Y").date()
                        await self._set_date_range(
                            page,
                            application_date,
                            application_date,
                            skip_application_date_selection)
                        await self._click_search_permits_button(page)
                        await self._open_detail_for_permit(page, row["Permit Number"])
                        record = await self._extract_permit_details(page)

                        # Persist immediately per permit as a crash-safe fallback
                        self.persist_result(row["Permit Number"], record)
                        outputs[row["Permit Number"]] = record

                        await self._click_back_button(page)

                        skip_application_date_selection = True
                        success = True
                    except Exception as e:
                        logging.exception("Arlington details failure for %s: %s", row["Permit Number"], e)
                    finally:
                        success_chunk = 1 if success else 0
                        failed_chunk = 0 if success else 1
                        self.process_progress_callback(progress_callback, success_chunk, failed_chunk, total)

                return outputs
            finally:
                await browser.close()

    async def _extract_permit_summary(self, page: Page, timeout_ms: int = 30000) -> Dict[str, Optional[str]]:
        """Extract data from Permit Summary section.

        Parameters
        ----------
        page : Page
            Current Playwright page.

        Returns
        -------
        Dict[str, Optional[str]]
            Mapping of normalized field name to extracted value.
        """
        try:
            # Find Permit Summary section
            try:
                summary_section = await self._find_section_by_heading(page, "Permit Summary")
                await expect(summary_section).to_be_visible(timeout=timeout_ms)
            except Exception:
                summary_section = page.locator("div.main-content")
                await expect(summary_section).to_be_visible(timeout=timeout_ms)
            result = {}

            # Extract fields from Permit Summary
            field_mappings = {
                "permit_number": "Permit Number",
                "work_type": "Work",
                "status": "Status",
                "sub": "Sub",
                "application_date": "Application Date",
                "expires_date": "Expiry Date",
                "issued_date": "Issued",
                "description": "Description"
            }

            for field_key, field_label in field_mappings.items():
                try:
                    value = await self._extract_field_value(summary_section, field_label)
                except Exception as e:
                    logging.exception("Error extracting permit summary: %s", e)
                    value = None
                result[field_key] = value

            return result

        except Exception as e:
            logging.exception("Error extracting permit summary: %s", e)
            return {}

    async def _extract_property_details(self, page: Page) -> Dict[str, Optional[str]]:
        """Extract data from Permit Details section.

        Parameters
        ----------
        page : Page
            Current Playwright page.

        Returns
        -------
        Dict[str, Optional[str]]
            Mapping of normalized field name to extracted value.
        """
        try:
            # Find Property Details section
            details_section = await self._find_section_by_heading(page, "Property Details")
            await expect(details_section).to_be_visible()

            result = {}

            # Extract basic fields
            field_mappings = {
                "address": "Address",
                "legal_description": "Legal Description",
                "building_name": "Name",
                "building_area": "Area",
                "building_zoning": "Zoning",
                "building_lot": "Lot",
                "building_type": "Property Type",
                "building_zip_code": "Zip Code"
            }

            for field_key, field_label in field_mappings.items():
                value = await self._extract_field_value(details_section, field_label)
                result[field_key] = value

            # Extract coordinates (latitude/longitude from "X and Y Co-ordinates")
            coordinates = await self._extract_coordinates(details_section)
            result.update(coordinates)

            return result

        except Exception as e:
            logging.exception("Error extracting property details: %s", e)
            return {}

    async def _extract_general_requirements(self, page: Page) -> Dict[str, Optional[float]]:
        """Extract data from General Requirements section.

        Parameters
        ----------
        page : Page
            Current Playwright page.

        Returns
        -------
        Dict[str, Optional[float]]
            Mapping with key ``permit_valuation`` if parsable, else ``None``.
        """
        try:
            # Find General Requirements section
            requirements_section = await self._find_section_by_heading(page, "General Requirements")
            await expect(requirements_section).to_be_visible()

            # Extract Construction Valuation-Declared
            valuation_str = await self._extract_field_value(
                requirements_section, "Construction Valuation-Declared"
            )

            permit_valuation = None
            if valuation_str:
                # Parse numeric value from string (remove currency symbols, commas)
                cleaned_value = re.sub(r'[^\d.]', '', valuation_str)
                try:
                    permit_valuation = float(cleaned_value) if cleaned_value else None
                except ValueError:
                    logging.warning("Could not parse valuation: %s", valuation_str)

            return {"permit_valuation": permit_valuation}

        except Exception as e:
            logging.exception("Error extracting general requirements: %s", e)
            return {"permit_valuation": None}

    async def _extract_associated_people(self, page: Page) -> List[AssociatedPerson]:
        """Extract Associated People data.

        Parameters
        ----------
        page : Page
            Current Playwright page.

        Returns
        -------
        List[AssociatedPerson]
            List of parsed associated people entries.
        """
        try:
            # Find Associated People section
            people_section = await self._find_section_by_heading(page, "Associated People")
            await expect(people_section).to_be_visible()

            # Look for data table rows in the section
            people_rows = people_section.locator('tr').filter(has_text=re.compile(r'\w+'))
            row_count = await people_rows.count()

            people_list = []

            for i in range(row_count):
                row = people_rows.nth(i)

                # Skip header rows
                if await row.locator('th').count() > 0:
                    continue

                # Extract data from table cells
                cells = row.locator('td')
                cell_count = await cells.count()

                if cell_count >= 5:  # Expect: Type, Name, Address, Email, Phone
                    person_data = AssociatedPerson()

                    # Extract each field
                    person_data.type = await self._get_cell_text(cells.nth(0))
                    person_data.name = await self._get_cell_text(cells.nth(1))
                    person_data.address = await self._get_cell_text(cells.nth(2))
                    person_data.email = await self._get_cell_text(cells.nth(3))
                    person_data.phone_number = await self._get_cell_text(cells.nth(4))

                    people_list.append(person_data)

            return people_list

        except Exception as e:
            logging.exception("Error extracting associated people: %s", e)
            return []

    async def _extract_sub_contractors(self, page: Page) -> List[SubContractor]:
        """Extract Sub-Contractors data.

        Parameters
        ----------
        page : Page
            Current Playwright page.

        Returns
        -------
        List[SubContractor]
            List of parsed sub-contractor entries.
        """
        try:
            # Find Sub-Contractors section
            contractors_section = await self._find_section_by_heading(page, "Sub-Contractors")
            await expect(contractors_section).to_be_visible()

            # Look for data table rows in the section
            contractor_rows = contractors_section.locator('tr').filter(has_text=re.compile(r'\w+'))
            row_count = await contractor_rows.count()

            contractors_list = []

            for i in range(row_count):
                row = contractor_rows.nth(i)

                # Skip header rows
                if await row.locator('th').count() > 0:
                    continue

                # Extract data from table cells
                cells = row.locator('td')
                cell_count = await cells.count()

                if cell_count >= 8:  # Expected contractor fields
                    contractor_data = SubContractor()

                    # Extract each field based on typical sub-contractor table structure
                    contractor_data.type = await self._get_cell_text(cells.nth(0))
                    contractor_data.company_name = await self._get_cell_text(cells.nth(1))
                    contractor_data.point_of_contact = await self._get_cell_text(cells.nth(2))
                    contractor_data.phone_number = await self._get_cell_text(cells.nth(3))
                    contractor_data.email = await self._get_cell_text(cells.nth(4))
                    contractor_data.city_registration_number = await self._get_cell_text(cells.nth(5))
                    contractor_data.effective_from = await self._get_cell_text(cells.nth(6))
                    contractor_data.effective_to = await self._get_cell_text(cells.nth(7))

                    # Some tables might have inspection notifications column
                    if cell_count >= 9:
                        contractor_data.inspection_notifications = await self._get_cell_text(cells.nth(8))

                    contractors_list.append(contractor_data)

            return contractors_list

        except Exception as e:
            logging.exception("Error extracting sub-contractors: %s", e)
            return []

    async def _find_section_by_heading(self, page: Page, heading_text: str) -> Locator:
        """Find a section by its heading text.

        Parameters
        ----------
        page : Page
            Current Playwright page.
        heading_text : str
            Section heading text to anchor on.

        Returns
        -------
        Locator
            Locator representing the section container.
        """
        try:
            # Look for heading containing the text, then find its parent container
            heading = page.locator(f'text="{heading_text}"')

            if await heading.count() > 1:
                heading = heading.first()

            # Find the closest parent container (div, section, etc.)
            section = heading.locator('xpath=ancestor::div[contains(@class, "group") or contains(@class, "section") or contains(@class, "collapse")][1]')

            # If no specific container found, use a broader parent
            if await section.count() == 0:
                section = heading.locator('xpath=ancestor::div[1]')

                return section
        except Exception as e:
            logging.exception("Error finding section by heading: %s", e)
            return None

    async def _extract_field_value(self, container: Locator, field_label: str) -> Optional[str]:
        """Extract value for a specific field within a container.

        Parameters
        ----------
        container : Locator
            Section/container in which to search for the field.
        field_label : str
            Visible label text for the field.

        Returns
        -------
        Optional[str]
            The extracted string value, if present.
        """
        # Look for label containing field text, then find associated value
        # Pattern 1: Label followed by value in same row/container
        label_locator: Locator = container.filter(has_text=field_label)
        await expect(label_locator).to_be_visible()

        if await label_locator.count() == 0:
            return None
        elif await label_locator.count() > 1:
            label_locator = label_locator.first()

        # Try to find value in various patterns:
        # 1. In next sibling element
        inner_text = await label_locator.inner_text()
        lines = inner_text.splitlines()
        for line in lines:
            if f"{field_label}:" in line:
                value = line.split(':', 1)[1].strip()
                return value
        return None

    async def _extract_coordinates(self, container: Locator) -> Dict[str, Optional[str]]:
        """Extract latitude and longitude from X and Y Co-ordinates field.

        Parameters
        ----------
        container : Locator
            Section/container in which the coordinates field resides.

        Returns
        -------
        Dict[str, Optional[str]]
            Mapping with keys ``latitude`` and ``longitude``.
        """
        try:
            coordinates_text = await self._extract_field_value(container, "X and Y Co-ordinates")

            if not coordinates_text:
                return {"latitude": None, "longitude": None}

            # Parse coordinates - could be in various formats like "X: 123.456, Y: 789.012"
            # or "123.456, 789.012" or "123.456 789.012"

            # Extract numbers from the text
            numbers = re.findall(r'-?\d+\.?\d*', coordinates_text)

            if len(numbers) >= 2:
                # Assuming X is longitude, Y is latitude (common GIS convention)
                longitude = numbers[0] if numbers[0] else None
                latitude = numbers[1] if numbers[1] else None
                return {"latitude": latitude, "longitude": longitude}

            return {"latitude": None, "longitude": None}

        except Exception as e:
            logging.debug("Error extracting coordinates: %s", e)
            return {"latitude": None, "longitude": None}

    async def _get_cell_text(self, cell: Locator) -> Optional[str]:
        """Get text content from a table cell, handling empty cells gracefully.

        Parameters
        ----------
        cell : Locator
            Table cell locator.

        Returns
        -------
        Optional[str]
            Text content if non-empty after stripping.
        """
        try:
            text = await cell.inner_text()
            return text.strip() if text and text.strip() else None
        except Exception:
            return None

    async def _goto_search_page(self, page: Page) -> None:
        """Navigate to the Arlington public search portal.

        Parameters
        ----------
        page : Page
            Current Playwright page.
        """
        await page.goto(self._base_url, wait_until="domcontentloaded")

    async def _goto_search_permit_page(self, page: Page, timeout_ms: int = 30000) -> None:
        selector = page.get_by_text("Search for a Permit", exact=False)
        await selector.click()
        await page.wait_for_load_state("networkidle", timeout=timeout_ms)

    async def _select_permit_type(self, page: Page, permit_type: str, timeout_ms: int = 30000) -> None:        # Get element by xpath
        selector = page.get_by_label("Permit Type", exact=False)
        await expect(selector).to_be_visible(timeout=timeout_ms)
        await selector.select_option(permit_type)
        await page.wait_for_load_state("networkidle", timeout=timeout_ms)

    async def _select_sub_type(self, page: Page, sub_type: str, timeout_ms: int = 30000) -> None:        # Get element by xpath
        selector = page.get_by_label("Permit Sub Type", exact=False)
        await expect(selector).to_be_visible(timeout=timeout_ms)
        await selector.select_option(sub_type)
        await page.wait_for_load_state("networkidle", timeout=timeout_ms)

    async def _select_work_type(self, page: Page, work_type: str, timeout_ms: int = 30000) -> None:        # Get element by xpath
        selector = page.get_by_label("Permit Work Type", exact=False)
        await expect(selector).to_be_visible(timeout=timeout_ms)
        await selector.select_option(work_type)
        await page.wait_for_load_state("networkidle", timeout=timeout_ms)

    async def _select_status(self, page: Page, status: str, timeout_ms: int = 30000) -> None:        # Get element by xpath
        try:
            await page.wait_for_timeout(1000)
            current_tab = await self._get_current_tab(page)
            selector = current_tab.locator("xpath=.//label[.//span[normalize-space()='Status']]/following::select[1]")

            await expect(selector).to_be_visible(timeout=timeout_ms)
            await selector.select_option(status)
            await page.wait_for_load_state("networkidle", timeout=timeout_ms)
        except Exception as e:
            logging.exception("Error selecting status: %s", e)

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
        else:
            end_date = end_date + timedelta(days=1)

        await expect(start_input).to_be_visible(timeout=timeout_ms)
        await expect(end_input).to_be_visible(timeout=timeout_ms)

        # Set start and end date
        await start_input.evaluate("(el, date) => el.value = date", start_date.strftime('%m/%d/%Y'))
        await end_input.evaluate("(el, date) => el.value = date", end_date.strftime('%m/%d/%Y'))

        await page.wait_for_load_state("networkidle", timeout=timeout_ms)

    async def _enter_address(self, page: Page, address: str, timeout_ms: int = 30000) -> None:
        current_tab = await self._get_current_tab(page)

        # Remove multiple spaces and newlines
        address = re.sub(r'\s+', ' ', address).strip()

        # Get input by label "Address" in the current tab
        selector = current_tab.locator("xpath=.//label[.//span[normalize-space()='Address']]/following::input[@type='text'][1]")
        await expect(selector).to_be_visible(timeout=timeout_ms)
        await selector.clear()
        await selector.fill(address)
        await page.wait_for_timeout(1000)
        try:
            # Find suggestion div which has the first number in the address
            suggestion = page.locator(f"div.autocomplete-suggestion:has-text('{address.split()[0]}')")
            await expect(suggestion).to_be_visible(timeout=timeout_ms)
            await suggestion.click()
        except Exception as e:
            logging.exception("Error entering address: %s", e)
        await page.wait_for_load_state("networkidle", timeout=timeout_ms)

    async def _get_current_tab(self, page: Page, timeout_ms: int = 30000) -> Locator:
        current_tab = page.locator("div.tab-pane.active")
        await expect(current_tab).to_be_visible(timeout=timeout_ms)
        return current_tab

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

    async def _open_detail_for_permit(self, page: Page, permit_number: str, timeout_ms: int = 30_000) -> bool:
        """Best-effort attempt to open the detail view for a given permit.

        Parameters
        ----------
        page : Page
            Current Playwright page on the Arlington portal.
        permit_number : str
            Permit/application identifier.
        timeout_ms : int, default=30000
            Max time to wait for UI elements.

        Returns
        -------
        bool
            True if a details view appears to have been opened, else False.

        Notes
        -----
        Arlington portal does not expose a single stable selector across all
        environments. This function tries a few reasonable selectors; failures
        fall back to returning ``False`` so the caller can produce a minimal
        :class:`PermitRecord` without halting the batch.
        """
        # Find <a> tag with innet text equals to permit_number
        selector = page.locator(f"a:has-text('{permit_number}')")
        await expect(selector).to_be_visible(timeout=timeout_ms)
        await selector.click()
        await page.wait_for_load_state("networkidle", timeout=timeout_ms)

    async def _click_back_button(self, page: Page, timeout_ms: int = 30_000) -> None:
        selector = page.get_by_role("button", name=re.compile(r"^Back$"))
        await expect(selector).to_be_visible(timeout=timeout_ms)
        await selector.click()
        await page.wait_for_load_state("networkidle", timeout=timeout_ms)

    async def _debug_dump(self, page: Page, permit_number: str, tag: str) -> None:
        """Persist page HTML and screenshot for debugging failures.

        Parameters
        ----------
        page : Page
            Current Playwright page.
        permit_number : str
            Current permit number context.
        tag : str
            Arbitrary tag to distinguish failure points.
        """
        try:
            pkg_root = Path(__file__).resolve().parents[4]
            out_dir = pkg_root / "debug" / "arlington_details"
            out_dir.mkdir(parents=True, exist_ok=True)
            safe_id = re.sub(r"[^A-Za-z0-9_-]+", "_", permit_number)
            html_path = out_dir / f"{safe_id}_{tag}.html"
            png_path = out_dir / f"{safe_id}_{tag}.png"
            content = await page.content()
            html_path.write_text(content, encoding="utf-8")
            try:
                await page.screenshot(path=str(png_path), full_page=True)
            except Exception:
                pass
        except Exception:
            pass

    async def _extract_permit_details(self, page: Page) -> PermitRecord:
        """Extract all permit details from the current detail page.

        Parameters
        ----------
        page : Page
            Current Playwright page on the details view.

        Returns
        -------
        PermitRecord
            Contract-compliant record with Arlington-specific fields in ``extra``.
        """
        # Wait for page ready, then extract sections
        await page.wait_for_timeout(1000)

        permit_summary = await self._extract_permit_summary(page)
        property_details = await self._extract_property_details(page)
        general_requirements = await self._extract_general_requirements(page)
        associated_people = await self._extract_associated_people(page)
        sub_contractors = await self._extract_sub_contractors(page)

        # Prepare extra fields bag (accepted by PermitRecord.extra = "allow")
        extra_fields: Dict[str, object] = {
            "work_type": permit_summary.get("work_type"),
            "status": permit_summary.get("status"),
            "sub": permit_summary.get("sub"),
            "application_date": permit_summary.get("application_date"),
            "expires_date": permit_summary.get("expires_date"),
            "issued_date": permit_summary.get("issued_date"),
            "description": permit_summary.get("description"),
            "address": property_details.get("address"),
            "legal_description": property_details.get("legal_description"),
            "latitude": property_details.get("latitude"),
            "longitude": property_details.get("longitude"),
            "building_name": property_details.get("building_name"),
            "building_area": property_details.get("building_area"),
            "building_zoning": property_details.get("building_zoning"),
            "building_lot": property_details.get("building_lot"),
            "building_type": property_details.get("building_type"),
            "building_zip_code": property_details.get("building_zip_code"),
            "permit_valuation": general_requirements.get("permit_valuation"),
            "associated_people": associated_people,
            "sub_contractors": sub_contractors,
        }

        permit_number_value = permit_summary.get("permit_number") or ""

        record = PermitRecord(
            permit_number=permit_number_value,
            applicant=ApplicantData(),
            owner=OwnerData(),
            **extra_fields,
        )

        return record
