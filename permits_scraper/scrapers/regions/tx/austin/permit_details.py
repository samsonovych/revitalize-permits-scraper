"""Austin (TX) public search scraper.

This module implements a SOLID, class-based scraper that:
- Opens the Austin Citizen Portal public search,
- Fills the "Permit Number, FolderRSN / ROWID, or Case Number" field,
- Clicks the "Search" button,
- Waits for the result card to render,
- Clicks the "Detail" button within that results card (first match).
- Navigates through detail tabs to extract permit information.

Design
------
- Single Responsibility: class focuses on opening the detail view for a given permit.
- Open/Closed: selectors and waits are encapsulated; easy to extend parsing later.
- Liskov Substitution: mirrors the interface shape from the example and can fit a BaseScraper.
- Interface Segregation: minimal surface (headless/base_url, scrape wrappers).
- Dependency Inversion: scraping depends on abstractions (locators, helpers) not hard-coded flows.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional


from permits_scraper.schemas.permit_record import PermitRecord
from permits_scraper.schemas.contacts import ApplicantData
from permits_scraper.schemas.contacts import OwnerData
from permits_scraper.scrapers.playwright_scraper import PlaywrightBaseScraper
from playwright.async_api import (
    Browser,
    BrowserContext,
    Locator,
    Page,
    async_playwright,
    expect,
)

class PermitDetailsScraper(PlaywrightBaseScraper):
    """Scraper for Austin (TX) public search detail navigation.

    Methods
    -------
    scrape(permit_numbers) -> Dict[str, Optional[str]]:
        Sync wrapper to call the async method.
    scrape_async(permit_numbers) -> Dict[str, Optional[str]]:
        Navigate to detail for each permit; returns mapping to final detail URL (or None if not opened).
    set_headless(value: bool) -> None:
        Toggle headless mode.
    set_base_url(value: str) -> None:
        Override base public search URL.
    """

    _base_url: str = "https://abc.austintexas.gov/citizenportal/app/public-search"
    _region: str = "tx"
    _city: str = "austin"

    async def scrape_async(self, permit_numbers: List[str]) -> Dict[str, PermitRecord]:
        """Asynchronously open the detail page for each permit number and extract data."""
        async with async_playwright() as pw:
            browser: Browser = await pw.chromium.launch(headless=self._headless)
            context: BrowserContext = await browser.new_context()
            await self._configure_network_blocking(context)
            page: Page = await context.new_page()

            try:
                results: Dict[str, PermitRecord] = {}
                for permit_number in permit_numbers:
                    await self._goto_search_page(page)
                    await self._submit_search(page, permit_number)
                    details_opened = await self._open_first_detail_in_results(page)

                    if not details_opened:
                        continue

                    # Extract permit data from detail tabs
                    application_date = await self._extract_application_date(page)
                    issued_date = await self._extract_issued_date(page)
                    building_address = await self._extract_property_details(page)
                    people_details = await self._extract_people_details(page)


                    result = PermitRecord(
                        permit_number=permit_number,
                        applicant=people_details["applicant"],
                        owner=people_details["owner"],
                        building_address=building_address,
                        application_date=application_date,
                        issued_date=issued_date
                    )

                    # Persist per-permit result immediately as a crash-safe fallback
                    self.persist_result(permit_number, result)

                    results[permit_number] = result

                return results
            finally:
                await browser.close()

    async def _goto_search_page(self, page: Page) -> None:
        await page.goto(self._base_url, wait_until="domcontentloaded")

    async def _submit_search(self, page: Page, permit_number: str, timeout_ms: int = 20000) -> None:
        """Fill the search field and click Search."""
        field = page.locator("#searchTerm_ID").or_(page.locator('input[name="searchTerm"]'))
        await expect(field).to_be_visible(timeout=timeout_ms)
        await field.fill(permit_number)

        search_btn = page.get_by_role("button", name=re.compile(r"^Search$", re.I)).or_(
            page.locator('button[title="Search"]')
        )
        await expect(search_btn).to_be_visible(timeout=timeout_ms)
        await search_btn.click()

    async def _open_first_detail_in_results(self, page: Page, timeout_ms: int = 60_000) -> bool:
        """Wait for results card, click the first available Detail button within it, and return the destination URL."""
        results_card = page.locator("div.datatable-row-center.datatable-row-group")
        await expect(results_card).to_be_visible(timeout=timeout_ms)

        detail_btn: Locator = results_card.locator('button[title="Detail"]')
        if await detail_btn.count() == 0:
            return False
        elif await detail_btn.count() > 1:
            detail_btn = detail_btn.first()

        await expect(detail_btn).to_be_visible(timeout=timeout_ms)
        await detail_btn.click()
        return True

    async def _extract_application_date(self, page: Page, timeout_ms: int = 20000) -> Optional[str]:
        """
        Step 1-2: Extract Application Date from the Folder Details tab.
        The page should already be on the Folder Details tab (title="Folder Details").
        Find the div with "Application Date" text and get the span value in the same row.
        """
        try:
            # Wait for the Folder Details tab content to load
            folder_details_tab = page.locator('a.nav-link[title="Folder Details"]')
            await expect(folder_details_tab).to_be_visible(timeout=timeout_ms)
            
            # Ensure we're on the Folder Details tab (it should be active by default)
            if not await folder_details_tab.locator('..').locator('li.active').count():
                await folder_details_tab.click()
                await page.wait_for_timeout(1000)  # Brief wait for content to load

            # Find the form group containing "Application Date"
            # Structure: div.col-md-6 > div.form-group > div.col-md-4[font-weight:bold] + span.col-md-8
            application_date_row = page.locator('div.col-md-6:has(div.col-md-4:has-text("Application Date"))')
            await expect(application_date_row).to_be_visible(timeout=timeout_ms)
            
            # Extract the date value from the span.col-md-8 in the same row
            date_span = application_date_row.locator('span.col-md-8')
            await expect(date_span).to_be_visible(timeout=timeout_ms)
            
            application_date = await date_span.inner_text()
            return application_date.strip() if application_date else None
            
        except Exception as e:
            # Log error but don't fail the entire scrape
            print(f"Error extracting application date: {e}")
            return None


    async def _extract_issued_date(self, page: Page, timeout_ms: int = 20000) -> Optional[str]:
        """
        Step 1-2: Extract Issued Date from the Folder Details tab.
        The page should already be on the Folder Details tab (title="Folder Details").
        Find the div with "Issued Date" text and get the span value in the same row.
        """
        try:
            # Wait for the Folder Details tab content to load
            folder_details_tab = page.locator('a.nav-link[title="Folder Details"]')
            await expect(folder_details_tab).to_be_visible(timeout=timeout_ms)
            
            # Ensure we're on the Folder Details tab (it should be active by default)
            if not await folder_details_tab.locator('..').locator('li.active').count():
                await folder_details_tab.click()
                await page.wait_for_timeout(1000)  # Brief wait for content to load

            # Find the form group containing "Issued"
            # Structure: div.col-md-6 > div.form-group > div.col-md-4[font-weight:bold] + span.col-md-8
            issued_date_row = page.locator('div.col-md-6:has(div.col-md-4:has-text("Issued"))')
            await expect(issued_date_row).to_be_visible(timeout=timeout_ms)
            
            # Extract the date value from the span.col-md-8 in the same row
            date_span = issued_date_row.locator('span.col-md-8')
            if await date_span.count() == 0:
                return
            
            issued_date = await date_span.inner_text()
            return issued_date.strip() if issued_date else None
            
        except Exception as e:
            # Log error but don't fail the entire scrape
            print(f"Error extracting issued date: {e}")
            return None

    async def _extract_property_details(self, page: Page, timeout_ms: int = 20000) -> Optional[str]:
        """
        Navigate to Property Details tab and extract the address using only the 'Address:' label
        and the first line break (<br>) as the terminator.
        """
        try:
            # Open the Property Details tab
            property_tab = page.locator('a.nav-link[title="Property Details"]')
            await expect(property_tab).to_be_visible(timeout=timeout_ms)
            await property_tab.click()

            # Wait for the <td> that contains the Address label
            address_td: Locator = page.locator('td').filter(has_text='Address:')
            await expect(address_td).to_be_visible(timeout=timeout_ms)

            if await address_td.count() > 1:
                address_td = address_td.first()

            # Get the visible text (Playwright renders <br> as a newline in inner_text)
            full_text: str = await address_td.inner_text()

            # Find "Address:" anchor
            m = re.search(r'Address:\s*', full_text, flags=re.I)
            if not m:
                return

            # Take everything until the first newline (i.e., <br>) or end of string
            rest: str = full_text[m.end():]
            first_line: str = rest.splitlines()[0] if rest else ""
            addr: str = first_line.strip()

            # Normalize whitespace/commas
            addr: str = re.sub(r'\s+', ' ', addr).replace('\xa0', ' ')
            addr: str = re.sub(r'\s*,\s*', ', ', addr).strip()

            return addr or None

        except Exception as e:
            # Non-fatal
            print(f"Error extracting property address: {e}")
            return None

    async def _extract_people_details(self, page: Page, timeout_ms: int = 20000) -> Dict[str, ApplicantData | OwnerData]:
        """
        Steps 5-7: Navigate to People Details tab and extract applicant and owner information.
        
        Returns dict of (applicant_data, owner_data) where each is a dict with 'address', 'phone', 'email'.
        """
        try:
            # Step 5: Open the People Details tab
            people_tab = page.get_by_role("tab", name=re.compile(r"^People Details$", re.I)).or_(
                page.locator('a.nav-link[title="People Details"]')
            )
            await expect(people_tab).to_be_visible(timeout=timeout_ms)
            await people_tab.click()
            
            # Wait for the datatable to load
            datatable = page.locator("ngx-datatable .datatable-body")
            await expect(datatable).to_be_visible(timeout=timeout_ms)
            
            # Find all datatable-body-row elements
            rows = page.locator("datatable-body-row")
            row_count = await rows.count()
            
            applicant_data = None
            owner_data = None
            
            # Step 6-7: Extract data from rows where first column is "Applicant" or "Owner"
            for i in range(row_count):
                row = rows.nth(i)
                
                # Get all cells in the row
                cells = row.locator("datatable-body-cell")
                cell_count = await cells.count()
                
                if cell_count >= 4:  # Type, Name/Address, Phone, Email
                    # Get the type from first cell
                    type_cell = cells.nth(0)
                    type_text = await type_cell.inner_text()
                    type_text = type_text.strip().lower()
                    
                    if type_text in ["applicant", "owner"]:
                        # Extract Name/Address from second cell
                        name_address_cell = cells.nth(1)
                        name_address_text = await name_address_cell.inner_text()
                        
                        # Extract Phone from third cell
                        phone_cell = cells.nth(2)
                        phone_text = await phone_cell.inner_text()
                        
                        # Extract Email from fourth cell
                        email_cell = cells.nth(3)
                        email_text = await email_cell.inner_text()
                        
                        # Parse name and address from the name/address cell
                        # Format appears to be: "Name<br>Address"
                        lines = name_address_text.strip().split('\n')
                        name = lines[0].strip() if lines else ""
                        address = lines[1].strip() if len(lines) > 1 else ""
                        
                        if type_text == "applicant":
                            applicant_data = ApplicantData()
                            applicant_data.first_name = name or None
                            applicant_data.address = address or None
                            applicant_data.phone_number = phone_text.strip() or None
                            applicant_data.email = email_text.strip() or None
                        elif type_text == "owner":
                            owner_data = OwnerData()
                            owner_data.company_name = name or None
                            owner_data.address = address or None
                            owner_data.phone_number = phone_text.strip() or None
                            owner_data.email = email_text.strip() or None
            
            return {
                "applicant": applicant_data or ApplicantData(),
                "owner": owner_data or OwnerData()
            }
            
        except Exception as e:
            print(f"Error extracting people details: {e}")
            return {
                "applicant": ApplicantData(),
                "owner": OwnerData()
            }
