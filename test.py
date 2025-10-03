from permits_scraper.scrapers.regions.tx.austin.permit_details import PermitDetailsScraper

scraper = PermitDetailsScraper()
scraper.set_headless(False)

print(scraper.scrape(["2025-047005 PR"]))