"""Permit List runner with concurrent instances and progress callback."""

from __future__ import annotations

import math
from typing import Any, Dict, Tuple, Optional
from datetime import date
import logging
import asyncio

from tqdm import tqdm

from permits_scraper.scrapers.base.playwright import PlaywrightBaseScraper
from permits_scraper.ui.registry import select_scraper
from permits_scraper.ui.utils import (
    iter_range_by_parts,
    parse_date_flexible,
    GREEN,
    RED,
    BOLD,
    RESET
)
from permits_scraper.scrapers.base.permit_list import PermitListBaseScraper


def calc_days_between(start_d: date, end_d: date, days_per_step: int) -> int:
    if days_per_step == -1:
        return 1
    return math.ceil(((end_d - start_d).days + 1) / days_per_step)


async def _list_worker(
    region: str,
    city: str,
    headless_raw: str,
    extra_kwargs: Dict[str, Any],
    chunk_group: Tuple[date, date],
    per_bar: tqdm,
    overall_bar: tqdm,
) -> Tuple[int, int, int]:

    start_s, end_s = chunk_group

    scraper: PermitListBaseScraper = select_scraper(region, city, type="list")  # type: ignore[assignment]
    if hasattr(scraper, "set_headless") and isinstance(scraper, PlaywrightBaseScraper) and headless_raw in {"n", "no", "false", "0"}:
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
                # Initialize totals lazily on first announcement from scraper
                per_bar.total = total_chunks
                # Bump overall total once per worker
                overall_total = overall_bar.total or 0
                overall_bar.total = overall_total + total_chunks
                overall_bar.refresh()
                announced_total = True
        except Exception:
            pass

    try:
        results = await scraper.scrape_async(
            start_s,
            end_s,
            progress_callback=on_progress,  # type: ignore[arg-type]
            **extra_kwargs,
        )
    except Exception:
        logging.exception(
            "PermitList list worker failed: region=%s city=%s range=%s-%s",
            region,
            city,
            start_s,
            end_s,
        )
        # Best-effort: mark remaining as failed to avoid stuck bars
        try:
            remaining = max(0, (per_bar.total or 0) - (success_chunks_total + failed_chunks_total))
            if remaining:
                per_bar.update(remaining)
                overall_bar.update(remaining)
                failed_chunks_total += remaining
                per_bar.set_postfix(success=success_chunks_total, failed=failed_chunks_total)
        except Exception:
            pass
        return success_chunks_total, failed_chunks_total, 0

    total_permits = sum(getattr(r, "number_of_permits", 0) for r in results) if isinstance(results, list) else 0
    return success_chunks_total, failed_chunks_total, total_permits


def run_list(
    region: str,
    city: str,
    start_date: str,
    end_date: str,
    instances: int,
    days_per_step: int = -1,
    headless_raw: str = "",
    extra_kwargs: Optional[Dict[str, Any]] = None,
) -> None:
    extra = extra_kwargs or {}
    # Ensure scraper receives the same days_per_step used to build UI chunks
    if days_per_step != -1:
        extra.setdefault("days_per_step", days_per_step)
    try:
        start_d = parse_date_flexible(start_date)
        end_d = parse_date_flexible(end_date)
    except ValueError as e:
        print(e)
        return
    if start_d > end_d:
        print("start_date must be on or before end_date")
        return
 
    actual_instances = min(max(1, instances), calc_days_between(start_d, end_d, days_per_step))
    all_chunks = iter_range_by_parts(start_d, end_d, actual_instances)

    # Initialize bars with unknown totals; each worker will announce its total via the progress callback
    overall_bar = tqdm(total=0, position=0, desc="Overall", leave=True)
    per_bars = [
        tqdm(total=0, position=i + 1, desc=f"Instance {i + 1}", leave=True)
        for i in range(len(all_chunks))
    ]

    async def runner() -> Tuple[int, int, int]:
        tasks = [
            _list_worker(region, city, headless_raw, extra, all_chunks[i], per_bars[i], overall_bar)
            for i in range(len(all_chunks))
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

    print(
        f"\n{GREEN}Successfully scraped: {success_chunks}{RESET} | {RED}Failed: {failed_chunks}{RESET} | "
        f"Total permits reported: {BOLD}{total_permits}{RESET}"
    )


