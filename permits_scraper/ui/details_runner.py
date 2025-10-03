"""Permit Details runner with concurrent instances and progress callback pattern."""

from __future__ import annotations

from typing import List, Tuple
import asyncio

from tqdm import tqdm

from permits_scraper.ui.registry import select_scraper
from permits_scraper.ui.utils import chunk_evenly, GREEN, RED, RESET
from permits_scraper.scrapers.base.permit_details import PermitDetailsBaseScraper


async def _details_worker(
    region: str,
    city: str,
    headless_raw: str,
    permits: List[str],
    per_bar: tqdm,
    overall_bar: tqdm,
) -> Tuple[int, int, int]:
    scraper: PermitDetailsBaseScraper = select_scraper(region, city, type="details")  # type: ignore[assignment]
    if hasattr(scraper, "set_headless") and headless_raw in {"n", "no", "false", "0"}:
        try:
            scraper.set_headless(False)  # type: ignore[attr-defined]
        except Exception:
            pass

    success_chunks_total = 0
    failed_chunks_total = 0

    def on_progress(success_chunks_inc: int, failed_chunks_inc: int, total_chunks: int) -> None:
        nonlocal success_chunks_total, failed_chunks_total
        try:
            success_chunks_total += success_chunks_inc
            failed_chunks_total += failed_chunks_inc
            overall_bar.update(success_chunks_inc+failed_chunks_inc)
            per_bar.update(success_chunks_inc+failed_chunks_inc)
            per_bar.set_postfix(success=success_chunks_total, failed=failed_chunks_total)
        except Exception:
            pass

    result_map = await scraper.scrape_async(
        permits,
        progress_callback=on_progress,  # type: ignore[arg-type]
    )

    total_permits = len(result_map)
    return success_chunks_total, failed_chunks_total, total_permits


def run_details(
    region: str,
    city: str,
    permits: List[str],
    instances: int,
    headless_raw: str = "",
) -> None:
    instances = max(1, min(instances, len(permits)))
    chunks = chunk_evenly(permits, instances)

    overall_total = len(permits)
    overall_bar = tqdm(total=overall_total, position=0, desc="Overall", leave=True)
    per_bars = [
        tqdm(total=len(ch), position=i + 1, desc=f"Instance {i + 1}", leave=True)
        for i, ch in enumerate(chunks)
    ]

    async def runner() -> Tuple[int, int, int]:
        tasks = [
            _details_worker(region, city, headless_raw, chunks[i], per_bars[i], overall_bar)
            for i in range(len(chunks))
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_total = 0
        failed_total = 0
        permits_total = 0
        for res in results:
            if isinstance(res, tuple):
                success_total += res[0]
                failed_total += res[1]
                permits_total += res[2]
        return success_total, failed_total, permits_total

    try:
        success_chunks, failed_chunks, total_permits = asyncio.run(runner())
    finally:
        for b in per_bars:
            b.close()
        overall_bar.close()

    print(f"\n{GREEN}Successfully scraped: {success_chunks}{RESET} | {RED}Failed: {failed_chunks}{RESET} | Total permits: {total_permits}")


