"""Base class for all scrapers."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List
from permits_scraper.schemas.permit_record import PermitRecord
from pydantic import BaseModel, PrivateAttr
import json
import os
from uuid import uuid4
from dotenv.main import logger


class PermitsBaseScraper(ABC, BaseModel):
    """Base class for all scrapers."""

    _region: str = PrivateAttr(..., init=True)
    _city: str = PrivateAttr(..., init=True)

    @abstractmethod
    def scrape(self, permit_numbers: List[str]) -> Dict[str, PermitRecord]:
        """Scrape the data from the URL."""
        pass

    @abstractmethod
    async def scrape_async(self, permit_numbers: List[str]) -> Dict[str, PermitRecord]:
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
        pkg_root = Path(__file__).resolve().parents[1]
        out_dir = pkg_root / "data" / "regions" / self._region / self._city
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir

    def persist_result(self, permit_number: str, result: PermitRecord) -> None:
        """Atomically persist a single permit result to a JSON file.

        This writes one JSON file per permit (``<permit_id>.json``) using an
        atomic replace to avoid partial writes and cross-process corruption.

        Parameters
        ----------
        permit_number : str
            The permit/application identifier used as the filename stem.
        result : PermitRecord
            The parsed result to serialize and persist.
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
        except Exception as e:
            # Best-effort persistence; do not fail the scrape due to IO errors
            try:
                logger.error(f"Failed to persist result for {permit_number}: {e}")
            except Exception:
                pass

