"""Base class for all scrapers."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional, Callable, Type
from permits_scraper.schemas.permit_record import PermitRecord
from pydantic import BaseModel, PrivateAttr, Field
import asyncio
import json
import os
from uuid import uuid4
import logging


class PermitDetailsBaseScraper(ABC, BaseModel):
    """Base class for permits details scrapers."""

    _region: str = PrivateAttr(..., init=True)
    _city: str = PrivateAttr(..., init=True)

    def scrape(self, permit_numbers: List[str], *args, **kwargs) -> Dict[str, PermitRecord]:
        """Scrape the data from the URL.

        Parameters
        ----------
        permit_numbers : List[str]
            The permit numbers to scrape.
        """
        try:
            return asyncio.run(self.scrape_async(permit_numbers, *args, **kwargs))
        except RuntimeError as exc:
            if "asyncio.run() cannot be called from a running event loop" in str(exc):
                raise RuntimeError(
                    "scrape() cannot be called from an active event loop; "
                    "use `await scrape_async(permit_numbers)` instead."
                ) from exc
            raise

    @abstractmethod
    async def scrape_async(self, permit_numbers: List[str], *args, **kwargs) -> Dict[str, PermitRecord]:
        """Scrape the data from the URL (Asynchronously)."""
        pass

    # ------------------------
    # Input schema and adapter
    # ------------------------
    class DefaultInputs(BaseModel):
        """Default inputs for details scrapers.

        Parameters
        ----------
        permits_csv_path : Path
            Path to CSV containing permit IDs to scrape.
        permits_column : str, default="Permit Number"
            Column name in CSV that contains permit IDs.
        headless : bool, default=True
            Run browser in headless mode.
        instances : int, default=1
            Parallel instances if supported.
        """

        permits_csv_path: Path = Field(description="Path to CSV with permit IDs to scrape")
        permits_column: str = Field(default="Permit Number", description="CSV column containing permit IDs")
        headless: bool = Field(default=True, description="Do you want to run headless?")
        instances: int = Field(default=1, description="How many instances to run in parallel")

    @classmethod
    def get_input_schema(cls) -> Type[BaseModel]:
        """Return the Pydantic model describing CLI inputs for this details scraper.

        Returns
        -------
        Type[pydantic.BaseModel]
            A model class used to prompt for inputs. Override in scrapers to customize.
        """
        return PermitDetailsBaseScraper.DefaultInputs

    def scrape_with_inputs(self, permit_numbers: List[str], inputs: BaseModel) -> Dict[str, PermitRecord]:
        """Run the scraper using a validated inputs object.

        Parameters
        ----------
        permit_numbers : List[str]
            Permit/application identifiers to open and extract.
        inputs : BaseModel
            Instance of the model returned by ``get_input_schema``.

        Returns
        -------
        Dict[str, PermitRecord]
            Scrape results, keyed by permit number.
        """
        payload = inputs.model_dump()
        other = payload
        return self.scrape(permit_numbers, **other)

    def _result_output_dir(self) -> Path:
        """Return the output directory for per-permit results.

        Returns
        -------
        Path
            Directory path ``permits_scraper/data/regions/tx/san_antonio`` relative
            to the package root. The directory is created if it does not exist.
        """
        pkg_root = Path(__file__).resolve().parents[2]
        out_dir = pkg_root / "data" / "regions" / "permits_details" / self._region / self._city
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir

    def persist_result(self, permit_number: str, result: PermitRecord) -> str:
        """Atomically persist a single permit result to a JSON file.

        This writes one JSON file per permit (``<permit_id>.json``) using an
        atomic replace to avoid partial writes and cross-process corruption.

        Parameters
        ----------
        permit_number : str
            The permit/application identifier used as the filename stem.
        result : PermitRecord
            The parsed result to serialize and persist.

        Returns
        -------
        str
            The path to the persisted result.
        """
        try:
            out_dir = self._result_output_dir()
            final_path = out_dir / f"{permit_number}.json"

            # Serialize result to JSON (pydantic v2)
            payload = json.dumps(result.model_dump(mode="json"), ensure_ascii=False, sort_keys=True, indent=2)

            # Write to a temp file in the same directory, then atomically replace
            tmp_name = f".{permit_number}.{uuid4().hex}.tmp"
            tmp_path = out_dir / tmp_name
            tmp_path.write_text(payload, encoding="utf-8")
            os.replace(tmp_path, final_path)

            return final_path
        except Exception as e:
            # Best-effort persistence; do not fail the scrape due to IO errors
            try:
                logging.exception("Failed to persist result for %s: %s", permit_number, e)
            except Exception:
                pass

    def process_progress_callback(self, progress_callback: Optional[Callable[[int, int, Optional[int]], None]], success_chunks_inc: int, failed_chunks_inc: int, total_chunks: Optional[int] = None) -> None:
        """Process the progress callback."""
        if progress_callback is not None:
            try:
                progress_callback(success_chunks_inc, failed_chunks_inc, total_chunks)
            except Exception:
                pass

    class Config:
        """Config for the permit details scraper."""

        arbitrary_types_allowed = True
