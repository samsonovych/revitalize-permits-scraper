"""Permit Details runner with concurrent instances and progress callback pattern."""

from __future__ import annotations

from typing import List, Tuple, Optional, Dict, Any
import asyncio
import logging

from tqdm import tqdm

from permits_scraper.ui.registry import select_scraper
from permits_scraper.ui.utils import chunk_evenly, GREEN, RED, RESET
from permits_scraper.scrapers.base.permit_details import PermitDetailsBaseScraper


async def _details_worker(
    region: str,
    city: str,
    headless: bool,
    permits: List[str],
    extra_kwargs: Optional[Dict[str, Any]],
    per_bar: tqdm,
    overall_bar: tqdm,
) -> Tuple[int, int, int]:
    scraper: PermitDetailsBaseScraper = select_scraper(region, city, type="details")  # type: ignore[assignment]
    if hasattr(scraper, "set_headless") and headless == False:
        try:
            scraper.set_headless(False)  # type: ignore[attr-defined]
        except Exception:
            pass

    success_chunks_total = 0
    failed_chunks_total = 0
    announced_total = False

    def on_progress(success_chunks_inc: int, failed_chunks_inc: int, total_chunks: Optional[int] = None) -> None:
        nonlocal success_chunks_total, failed_chunks_total
        nonlocal announced_total
        try:
            success_chunks_total += success_chunks_inc
            failed_chunks_total += failed_chunks_inc
            overall_bar.update(success_chunks_inc+failed_chunks_inc)
            per_bar.update(success_chunks_inc+failed_chunks_inc)
            per_bar.set_postfix(success=success_chunks_total, failed=failed_chunks_total)
            if total_chunks is not None and not announced_total:
                per_bar.total = total_chunks
                overall_total = overall_bar.total or 0
                overall_bar.total = overall_total + total_chunks
                overall_bar.refresh()
                announced_total = True
        except Exception:
            pass

    try:
        kwargs = dict(extra_kwargs or {})
        result_map = await scraper.scrape_async(
            permits,
            progress_callback=on_progress,  # type: ignore[arg-type]
            **kwargs,
        )
    except Exception:
        logging.exception("PermitDetails worker failed: region=%s city=%s", region, city)
        # Mark remaining as failed to avoid stuck bars
        try:
            remaining = max(0, (per_bar.total or 0) - (success_chunks_total + failed_chunks_total))
            if remaining:
                per_bar.update(remaining)
                overall_bar.update(remaining)
                failed_chunks_total += remaining
                per_bar.set_postfix(success=success_chunks_total, failed=failed_chunks_total)
        except Exception:
            pass
        result_map = {}

    total_permits = len(result_map)
    return success_chunks_total, failed_chunks_total, total_permits


def run_details(
    region: str,
    city: str,
    permits: List[str],
    instances: int,
    headless: bool,
    extra_kwargs: Optional[Dict[str, Any]] = None,
) -> None:
    instances = max(1, min(instances, len(permits)))
    chunks = chunk_evenly(permits, instances)

    # Initialize bars with unknown totals; workers will announce totals via progress callback
    overall_bar = tqdm(total=0, position=0, desc="Overall", leave=True)
    per_bars = [
        tqdm(total=0, position=i + 1, desc=f"Instance {i + 1}", leave=True)
        for i, _ in enumerate(chunks)
    ]

    async def runner() -> Tuple[int, int, int]:
        tasks = [
            _details_worker(region, city, headless, chunks[i], extra_kwargs, per_bars[i], overall_bar)
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


