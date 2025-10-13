"""Test the El Paso permit details scraper."""

import pytest
from permits_scraper.scrapers.regions.tx.el_paso.permit_details import PermitDetailsScraper
from permits_scraper.schemas.contacts import OwnerData
from permits_scraper.schemas.permit_record import PermitRecord
from typing import List, Dict


@pytest.mark.parametrize(
    "permit_numbers,expected_output",
    [
        (
            ["BRNN25-00276"],
            {
                "BRNN25-00276": PermitRecord(
                    permit_number="BRNN25-00276",
                    licensed_professional="EDGAR MONTIEL\nPALO VERDE HOMES\nLCCR11-01020\n7100 WESTWIND STE. 250\nEL PASO, TX, 79912\nHome Phone:\t9155849090\nMobile Phone:\t9157906494\nContractor General 11-LP-00371",
                    owner=OwnerData(
                        company_name="PALO VERDE HOMES",
                        address="PALO VERDE HOMES",
                    ),
                    applicant="Edgar Lopez\nPALO VERDE HOMES\n7100 WEST WIND SUITE. 250\nEL PASO, TEXAS, 79912\nPhone:\t9157906494\nWork Phone:\t9155849090\npvhomar@hotmail.com",
                    job_value="$169,577.90"
                )
            }
        ),
        (
            ["TPRN25-00374", "BRNN25-00335"],
            {
                "TPRN25-00374": PermitRecord(
                    permit_number="TPRN25-00374",
                    licensed_professional="ARMANDO BARRON\nDIAMOND CREST CAPITAL LLC\n11 DIAMOND CREST\nEL PASO, TX, 79902\nHome Phone:\t4154256434\nContractor General 24-LP-00676",
                    owner=OwnerData(
                        company_name="DIAMOND CREST CAPITAL LLC",
                        address="13 DIAMOND CREST LN",
                    ),
                    third_party="Kelly Sorenson\nVision Consultants, Inc.\n9440 Viscount\nEL PASO, TX\nPhone:\t9152272100\nMobile Phone:\t9152272100\nE-mail:\tkelly@visionelp.com",
                    job_value="$336,678.52"
                ),
                "BRNN25-00335": PermitRecord(
                    permit_number="BRNN25-00335",
                    applicant="Javier Roque\nRoque Architecture\n10021 Buckwood\nEl Paso, Texas, 79925\nPhone:\t9152047129\nWork Phone:\t9152047129\nMobile Phone:\t9152047129\nroquearch1@gmail.com",
                    owner=OwnerData(
                        first_name="Fernando",
                        last_name="Mendivil",
                        address="2625 GOLD",
                    )
                )
            }
        )
    ]
)
def test_scrape_el_paso_permit_details(permit_numbers: List[str], expected_output: Dict[str, PermitRecord]):
    """Test the El Paso permit details scraper."""
    scraper = PermitDetailsScraper()
    output = scraper.scrape(permit_numbers)
    assert output == expected_output
