"""CLI menu entrypoint and shared flows."""

from __future__ import annotations

from typing import List
from pathlib import Path
import json

import pandas as pd

from permits_scraper.ui.registry import select_scraper
from permits_scraper.ui.utils import GREEN, RED, YELLOW, CYAN, BOLD, RESET, setup_file_logging, read_permit_numbers
from permits_scraper.ui.details_runner import run_details
from permits_scraper.ui.list_runner import run_list


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
                _ = select_scraper(region, city, type="list")
            except ValueError as e:
                print(f"{RED}{e}{RESET}")
                continue
            start_date = input("Enter start date (DD/MM/YYYY): ").strip()
            end_date = input("Enter end date (DD/MM/YYYY): ").strip()
            dps_raw = input("Optional days_per_step (press Enter to skip): ").strip()
            headless_raw = input("Run headless? [Y/n] (default Y): ").strip().lower()
            inst_raw = input("How many instances? (default 1): ").strip()
            try:
                instances = int(inst_raw) if inst_raw else 1
            except ValueError:
                instances = 1
            days_per_step = None
            if dps_raw:
                try:
                    days_per_step = int(dps_raw)
                except ValueError:
                    days_per_step = -1
            print(f"\n{BOLD}Running list scraper...{RESET}")
            run_list(
                region=region,
                city=city,
                start_date=start_date,
                end_date=end_date,
                instances=instances,
                days_per_step=days_per_step if days_per_step is not None else -1,
                headless_raw=headless_raw,
            )
            input(f"\n{BOLD}Press Enter to return to menu...{RESET}")
            continue

        if choice == "2":
            region = input("Enter region/state code (e.g., tx): ").strip()
            city = input("Enter city (e.g., san_antonio): ").strip()
            try:
                _ = select_scraper(region, city, type="details")
            except ValueError as e:
                print(f"{RED}{e}{RESET}")
                continue
            csv_path_str = input("Enter full path to the CSV with permits: ").strip()
            column = input("Enter column name that contains permit IDs: ").strip()
            inst_raw = input("How many instances? (default 1): ").strip()
            headless_raw = input("Run headless? [Y/n] (default Y): ").strip().lower()
            from pathlib import Path as _P
            csv_path = _P(csv_path_str).expanduser().resolve()
            try:
                permits = read_permit_numbers(csv_path, column)
            except Exception as e:
                print(f"{RED}{e}{RESET}")
                continue
            try:
                instances = int(inst_raw) if inst_raw else 1
            except ValueError:
                instances = 1
            run_details(region=region, city=city, permits=permits, instances=instances, headless_raw=headless_raw)
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


