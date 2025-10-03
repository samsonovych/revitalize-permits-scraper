"""Dynamic scraper registry for details and list scrapers."""

from __future__ import annotations

from typing import Any, Dict, Tuple, Type, Literal
from pathlib import Path
import importlib
import importlib.util
import inspect
import logging
import pkgutil
import sys

from permits_scraper.scrapers.base.permit_details import PermitDetailsBaseScraper
from permits_scraper.scrapers.base.permit_list import PermitListBaseScraper


_DETAILS_REGISTRY: Dict[Tuple[str, str], Type[Any]] = {}
_LIST_REGISTRY: Dict[Tuple[str, str], Type[Any]] = {}


def _normalize_city(city: str) -> str:
    return city.lower().strip().replace(" ", "_").replace("-", "_")


def _register_from_module(module: Any, region_token: str, city_token: str, kind: Literal["details", "list"]) -> None:
    module_name = getattr(module, "__name__", "")
    for _, obj in inspect.getmembers(module, inspect.isclass):
        if getattr(obj, "__module__", None) != module_name:
            continue
        if kind == "details" and issubclass(obj, PermitDetailsBaseScraper) and obj is not PermitDetailsBaseScraper:
            _DETAILS_REGISTRY.setdefault((region_token, city_token), obj)
        if kind == "list" and issubclass(obj, PermitListBaseScraper) and obj is not PermitListBaseScraper:
            _LIST_REGISTRY.setdefault((region_token, city_token), obj)


def _ensure_loaded() -> None:
    if _DETAILS_REGISTRY and _LIST_REGISTRY:
        return
    regions_pkg_name = "permits_scraper.scrapers.regions"
    # Strategy 1: package walk
    try:
        regions_pkg = importlib.import_module(regions_pkg_name)
        for modinfo in pkgutil.walk_packages(regions_pkg.__path__, prefix=f"{regions_pkg_name}."):
            name = modinfo.name
            if not (name.endswith(".permit_details") or name.endswith(".permits_list")):
                continue
            try:
                module = importlib.import_module(name)
            except Exception:
                logging.exception("Failed to import module during discovery: %s", name)
                continue
            parts = name.split(".")
            if len(parts) < 6:
                continue
            region_token = parts[-3].lower()
            city_token = parts[-2].lower()
            kind: Literal["details", "list"] = "details" if name.endswith(".permit_details") else "list"
            _register_from_module(module, region_token, city_token, kind)
    except Exception:
        logging.exception("Package-based scraper discovery failed")

    # Strategy 2: filesystem load
    try:
        regions_root = Path(__file__).resolve().parents[1] / "scrapers" / "regions"
        if regions_root.exists():
            for fp in regions_root.rglob("permit_details.py"):
                try:
                    region_token = fp.parent.parent.name.lower()
                    city_token = fp.parent.name.lower()
                    spec = importlib.util.spec_from_file_location(f"_scr_{region_token}_{city_token}_details", fp)
                    if spec and spec.loader:
                        module = importlib.util.module_from_spec(spec)
                        sys.modules[spec.name] = module  # type: ignore[arg-type]
                        spec.loader.exec_module(module)  # type: ignore[assignment]
                        _register_from_module(module, region_token, city_token, "details")
                except Exception:
                    logging.exception("Failed to load details module from %s", fp)
            for fp in regions_root.rglob("permits_list.py"):
                try:
                    region_token = fp.parent.parent.name.lower()
                    city_token = fp.parent.name.lower()
                    spec = importlib.util.spec_from_file_location(f"_scr_{region_token}_{city_token}_list", fp)
                    if spec and spec.loader:
                        module = importlib.util.module_from_spec(spec)
                        sys.modules[spec.name] = module  # type: ignore[arg-type]
                        spec.loader.exec_module(module)  # type: ignore[assignment]
                        _register_from_module(module, region_token, city_token, "list")
                except Exception:
                    logging.exception("Failed to load list module from %s", fp)
    except Exception:
        logging.exception("Filesystem-based scraper discovery failed")


def select_scraper(region: str, city: str, type: Literal["details", "list"]) -> PermitDetailsBaseScraper | PermitListBaseScraper:
    _ensure_loaded()
    r = region.lower().strip()
    c = _normalize_city(city)
    if type == "details":
        cls = _DETAILS_REGISTRY.get((r, c))
    else:
        cls = _LIST_REGISTRY.get((r, c))
    if cls is None:
        msg = f"No scraper available for region={region!r}, city={city!r}, type={type!r}."
        logging.error(msg)
        raise ValueError(msg)
    return cls()  # type: ignore[call-arg]


