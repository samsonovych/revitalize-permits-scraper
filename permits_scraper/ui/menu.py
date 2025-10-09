"""CLI menu entrypoint and shared flows."""

from __future__ import annotations

from typing import List
from pathlib import Path
import json

import pandas as pd

from permits_scraper.ui.registry import select_scraper
from permits_scraper.ui.utils import GREEN, RED, YELLOW, CYAN, BOLD, RESET, setup_file_logging, read_permit_numbers, prompt_for_model
from permits_scraper.ui.details_runner import run_details
from permits_scraper.ui.list_runner import run_list
from permits_scraper.scrapers.base.permit_list import PermitListBaseScraper
from permits_scraper.scrapers.base.permit_details import PermitDetailsBaseScraper


def print_banner() -> None:
    banner = f"""
{BOLD}{CYAN}===================================
   USA Building Permits Scrapper
==================================={RESET}
"""
    print(banner)


def prompt_menu() -> str:
    print("1. Scrape list of permits (by date range; DD/MM/YYYY)")
    print("2. Scrape details for permits")
    print("3. Convert JSON files to CSV")
    print("4. Exit")
    return input(f"\n{BOLD}Select an option [1-4]: {RESET}").strip()


def flatten(prefix: str, obj, out) -> None:  # type: ignore[no-untyped-def]
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            flatten(key, v, out)
    elif isinstance(obj, list):
        out[prefix] = json.dumps(obj, ensure_ascii=False)
    else:
        out[prefix] = obj


def convert_json_folder_to_csv(folder: Path, out_csv: Path) -> int:
    if not folder.exists() or not folder.is_dir():
        raise FileNotFoundError(f"Folder not found or not a directory: {folder}")
    rows = []
    files = sorted([p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == ".json"])
    for fp in files:
        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
            flat = {}
            flatten("", data, flat)
            flat["id"] = fp.stem
            rows.append(flat)
        except Exception:
            continue
    if not rows:
        pd.DataFrame(columns=["id"]).to_csv(out_csv, index=False)
        return 0
    df = pd.DataFrame(rows)
    cols = df.columns.tolist()
    if "id" in cols:
        cols = ["id"] + [c for c in cols if c != "id"]
        df = df[cols]
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    return len(rows)


def main() -> None:
    log_file = Path(__file__).resolve().parents[1] / "logs.txt"
    setup_file_logging(log_file)
    while True:
        print_banner()
        print()
        choice = prompt_menu()
        print()

        if choice == "1":
            region = input("Enter region/state code (e.g., tx): ").strip()
            city = input("Enter city (e.g., austin): ").strip()
            try:
                scraper: PermitListBaseScraper = select_scraper(region, city, type="list")
            except ValueError as e:
                print(f"{RED}{e}{RESET}")
                continue
            # Scraper-driven inputs
            schema = scraper.__class__.get_input_schema()  # type: ignore[attr-defined]
            inputs = prompt_for_model(schema)
            # Extract known fields and route through list runner to keep multi-instance support
            start_v = getattr(inputs, "start_date", "")
            end_v = getattr(inputs, "end_date", "")
            start_s = start_v.strftime('%Y-%m-%d') if hasattr(start_v, 'strftime') else str(start_v)
            end_s = end_v.strftime('%Y-%m-%d') if hasattr(end_v, 'strftime') else str(end_v)
            instances = getattr(inputs, "instances", 1)
            days_per_step = getattr(inputs, "days_per_step", -1)
            headless_raw_val = getattr(inputs, "headless_raw", None)
            if headless_raw_val is None:
                hb = getattr(inputs, "headless", None)
                if hb is not None:
                    headless_raw_val = 'n' if (hb is False) else 'y'
            headless_raw = str(headless_raw_val or "").lower()
            payload = inputs.model_dump()
            extras = {k: v for k, v in payload.items() if k not in {"start_date", "end_date", "instances", "days_per_step", "headless_raw", "headless"}}
            print(f"\n{BOLD}Running list scraper...{RESET}")
            try:
                run_list(
                    region=region,
                    city=city,
                    start_date=start_s,
                    end_date=end_s,
                    instances=instances,
                    days_per_step=days_per_step,
                    headless_raw=headless_raw,
                    extra_kwargs=extras if extras else None,
                )
            except Exception as e:
                print(f"{RED}Scrape failed: {e}{RESET}")
            input(f"\n{BOLD}Press Enter to return to menu...{RESET}")
            continue

        if choice == "2":
            region = input("Enter region/state code (e.g., tx): ").strip()
            city = input("Enter city (e.g., san_antonio): ").strip()
            try:
                scraper: PermitDetailsBaseScraper = select_scraper(region, city, type="details")  # type: ignore[assignment]
            except ValueError as e:
                print(f"{RED}{e}{RESET}")
                continue

            # Scraper-driven inputs
            schema = scraper.__class__.get_input_schema()  # type: ignore[attr-defined]
            inputs = prompt_for_model(schema)

            payload = inputs.model_dump()

            # permits source
            permits = []
            try:
                if "permits" in payload and isinstance(payload.get("permits"), list):
                    permits = [str(p) for p in payload.get("permits") or []]
                else:
                    from pathlib import Path as _P
                    csv_path_val = payload.get("permits_csv_path")
                    col_name = payload.get("permits_column") or "Permit Number"
                    if csv_path_val is None:
                        raise ValueError("permits_csv_path is required in inputs to read permit IDs")
                    csv_path = _P(str(csv_path_val)).expanduser().resolve()
                    permits = read_permit_numbers(csv_path, str(col_name))
            except Exception as e:
                print(f"{RED}{e}{RESET}")
                continue

            instances = int(payload.get("instances") or 1)
            headless_raw_val = payload.get("headless_raw")
            if headless_raw_val is None:
                hb = payload.get("headless")
                if hb is not None:
                    headless_raw_val = False if (hb is False) else True
            headless = bool(headless_raw_val)

            # extras for scraper.scrape_async
            exclude_keys = {"permits", "permits_csv_path", "permits_column", "instances", "headless_raw", "headless"}
            extras = {k: v for k, v in payload.items() if k not in exclude_keys}

            print(f"\n{BOLD}Running details scraper...{RESET}")
            try:
                run_details(
                    region=region,
                    city=city,
                    permits=permits,
                    instances=instances,
                    headless=headless,
                    extra_kwargs=extras if extras else None,
                )
            except Exception as e:
                print(f"{RED}Scrape failed: {e}{RESET}")
            input(f"\n{BOLD}Press Enter to return to menu...{RESET}")
            continue

        if choice == "3":
            folder_str = input("Enter path to folder with JSON files: ").strip()
            out_csv_str = input("Enter output CSV file path (e.g., output.csv): ").strip()
            folder = Path(folder_str).expanduser().resolve()
            out_csv = Path(out_csv_str).expanduser().resolve()
            try:
                count = convert_json_folder_to_csv(folder, out_csv)
                print(f"Converted {count} JSON files into: {BOLD}{out_csv}{RESET}")
            except Exception as e:
                print(f"{RED}Conversion failed: {e}{RESET}")
            input(f"\n{BOLD}Press Enter to return to menu...{RESET}")
            continue

        if choice == "4":
            print("Goodbye!")
            break

        print(f"{RED}Invalid option. Please select 1, 2, 3, or 4.{RESET}\n")


