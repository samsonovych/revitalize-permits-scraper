"""Base class for all scrapers."""

from abc import ABC, abstractmethod
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional, Callable, Type, Tuple
from permits_scraper.schemas.permit_range_log import PermitRangeLog
from pydantic import BaseModel, PrivateAttr, Field, ConfigDict
import json
import os
from uuid import uuid4
import logging
import asyncio


class PermitListBaseScraper(ABC, BaseModel):
    """Base class for permits list scrapers."""

    _region: str = PrivateAttr(..., init=True)
    _city: str = PrivateAttr(..., init=True)

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

    def scrape(self, start_date: date, end_date: date, *args, **kwargs) -> List[PermitRangeLog]:
        """Scrape the data from the URL.

        Parameters
        ----------
        start_date : date
            The start date to scrape.
        end_date : date
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
    async def scrape_async(self, start_date: date, end_date: date, *args, **kwargs) -> List[PermitRangeLog]:
        """Scrape the data from the URL (Asynchronously)."""
        pass

    # ------------------------
    # Input schema and adapter
    # ------------------------
    class DefaultInputs(BaseModel):
        """Default inputs for list scrapers.

        Parameters
        ----------
        start_date : date
            Inclusive start date.
        end_date : date
            Inclusive end date.
        days_per_step : int, default=-1
            Chunk size in days (-1 for full range).
        headless_raw : str, default=""
            Headless toggle string ("Y"/"n") if applicable.
        instances : int, default=1
            Parallel instances if supported.
        """

        start_date: date = Field(description="Start date (DD/MM/YYYY or YYYY-MM-DD)")
        end_date: date = Field(description="End date (DD/MM/YYYY or YYYY-MM-DD)")
        days_per_step: int = Field(default=-1, description="Chunk size in days (-1 for full range)")
        headless_raw: str = Field(default="", description="Run headless? [Y/n] (blank keeps default)")
        instances: int = Field(default=1, description="How many instances to run in parallel")

    @classmethod
    def get_input_schema(cls) -> Type[BaseModel]:
        """Return the Pydantic model describing CLI inputs for this scraper.

        Returns
        -------
        Type[pydantic.BaseModel]
            A model class used to prompt for inputs. Override in scrapers to customize.
        """
        return PermitListBaseScraper.DefaultInputs

    def scrape_with_inputs(self, inputs: BaseModel) -> List[PermitRangeLog]:
        """Run the scraper using a validated inputs object.

        Parameters
        ----------
        inputs : BaseModel
            Instance of the model returned by ``get_input_schema``.

        Returns
        -------
        List[PermitRangeLog]
            Scrape results.
        """
        payload = inputs.model_dump()
        start_date: date = payload.get("start_date")
        end_date: date = payload.get("end_date")
        other = {k: v for k, v in payload.items() if k not in {"start_date", "end_date"}}
        return self.scrape(start_date, end_date, **other)

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

    def persist_result(self, start_date: date, end_date: date, result: PermitRangeLog) -> str:
        """Atomically persist a single permit result to a JSON file.

        This writes one JSON file per permit (``<permit_id>.json``) using an
        atomic replace to avoid partial writes and cross-process corruption.

        Parameters
        ----------
        start_date : date
            The start date to scrape.
        end_date : date
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
            final_path = out_dir / f"{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.json"

            # Serialize result to JSON (pydantic v2)
            payload = json.dumps(result.model_dump(mode="json"), ensure_ascii=False, sort_keys=True, indent=2)

            # Write to a temp file in the same directory, then atomically replace
            tmp_name = f".{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.{uuid4().hex}.tmp"
            tmp_path = out_dir / tmp_name
            tmp_path.write_text(payload, encoding="utf-8")
            os.replace(tmp_path, final_path)

            return final_path
        except Exception as e:
            # Best-effort persistence; do not fail the scrape due to IO errors
            try:
                logging.exception("Failed to persist result for %s: %s", start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), e)
            except Exception:
                pass

    def process_progress_callback(self, progress_callback: Optional[Callable[[int, int, Optional[int]], None]], success_chunks_inc: int, failed_chunks_inc: int, total_chunks: Optional[int] = None) -> None:
        """Process the progress callback."""
        if progress_callback is not None:
            try:
                progress_callback(success_chunks_inc, failed_chunks_inc, total_chunks)
            except Exception:
                pass

    # ------------------------
    # Utilities
    # ------------------------
    def _iter_chunks(self, start: date, end: date, days_per_step: int) -> List[Tuple[date, date]]:
        """Iterate over the chunks."""
        if start > end:
            raise ValueError("start_date must be on or before end_date")

        if days_per_step is None or days_per_step == -1:
            return [(start, end)]

        # With days_per_step=N, include N days after the start in the first inclusive chunk, then continue after that end.
        step = timedelta(days=days_per_step)
        chunks: List[Tuple[date, date]] = []
        cur = start
        while cur <= end:
            cur_end = cur + step
            if cur_end > end:
                cur_end = end
            chunks.append((cur, cur_end))
            cur = cur_end + timedelta(days=1)
        return chunks
