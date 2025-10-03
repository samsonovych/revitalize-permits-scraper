"""Base class for all scrapers."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Callable
from permits_scraper.schemas.permit_range_log import PermitRangeLog
from pydantic import BaseModel, PrivateAttr
import json
import os
from uuid import uuid4
import logging
import asyncio


class PermitListBaseScraper(ABC, BaseModel):
    """Base class for permits list scrapers."""

    _region: str = PrivateAttr(..., init=True)
    _city: str = PrivateAttr(..., init=True)

    def scrape(self, start_date: str, end_date: str, *args, **kwargs) -> List[PermitRangeLog]:
        """Scrape the data from the URL.

        Parameters
        ----------
        start_date : str
            The start date to scrape.
        end_date : str
            The end date to scrape.
        *args : Any
            Additional positional arguments to pass to the scraper.
        **kwargs : Any
            Additional keyword arguments to pass to the scraper.
        """
        try:
            return asyncio.run(self.scrape_async(start_date, end_date, *args, **kwargs))
        except RuntimeError as exc:
            if "asyncio.run() cannot be called from a running event loop" in str(exc):
                raise RuntimeError(
                    "scrape() cannot be called from an active event loop; "
                    "use `await scrape_async(start_date, end_date, *args, **kwargs)` instead."
                ) from exc
            raise

    @abstractmethod
    async def scrape_async(self, start_date: str, end_date: str, *args, **kwargs) -> List[PermitRangeLog]:
        """Scrape the data from the URL (Asynchronously)."""
        pass

    def _result_output_dir(self) -> Path:
        """Return the output directory for per-permit results.

        Returns
        -------
        Path
            Directory path ``permits_scraper/data/regions/tx/san_antonio`` relative
            to the package root. The directory is created if it does not exist.
        """
        pkg_root = Path(__file__).resolve().parents[2]
        out_dir = pkg_root / "data" / "regions" / "permits_list" / self._region / self._city
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir

    def persist_result(self, start_date: str, end_date: str, result: PermitRangeLog) -> str:
        """Atomically persist a single permit result to a JSON file.

        This writes one JSON file per permit (``<permit_id>.json``) using an
        atomic replace to avoid partial writes and cross-process corruption.

        Parameters
        ----------
        start_date : str
            The start date to scrape.
        end_date : str
            The end date to scrape.
        result : PermitRangeLog
            The parsed result to serialize and persist.

        Returns
        -------
        str
            The path to the persisted result.
        """
        try:
            out_dir = self._result_output_dir()
            final_path = out_dir / f"{start_date}_{end_date}.json"

            # Serialize result to JSON (pydantic v2)
            payload = json.dumps(result.model_dump(mode="json"), ensure_ascii=False, sort_keys=True, indent=2)

            # Write to a temp file in the same directory, then atomically replace
            tmp_name = f".{start_date}_{end_date}.{uuid4().hex}.tmp"
            tmp_path = out_dir / tmp_name
            tmp_path.write_text(payload, encoding="utf-8")
            os.replace(tmp_path, final_path)

            return final_path
        except Exception as e:
            # Best-effort persistence; do not fail the scrape due to IO errors
            try:
                logging.exception("Failed to persist result for %s: %s", start_date, end_date, e)
            except Exception:
                pass

    def process_progress_callback(self, progress_callback: Optional[Callable[[int, int, int], None]], success_chunks_inc: int, failed_chunks_inc: int, total_chunks: int) -> None:
        if progress_callback is not None:
            try:
                progress_callback(success_chunks_inc, failed_chunks_inc, total_chunks)
            except Exception:
                pass