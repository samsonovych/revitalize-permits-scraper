"""CLI runner for USA Building Permits Scrapper.

This module provides an interactive console menu to run scraping tasks.
It includes connectivity checks, concurrent execution with per-instance
and overall progress bars, and file-based logging for diagnostics.

Menu
----
1. Scrape list of permits (temporary unavailable)
2. Scrape details for permits
3. Convert JSON files to CSV
4. Exit

Notes
-----
- Logs are written to ``logs.txt`` at the project root.
- Console output is user-focused; operational logs are not printed.
"""

from __future__ import annotations

import asyncio
import logging
import ssl
import urllib.request
from pathlib import Path
import json
from typing import Any, Dict, List, Tuple

import pandas as pd
from tqdm import tqdm

# ANSI colors (Linux/macOS terminals). No console logs via logging handlers.
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


LOG_FILE = Path(__file__).resolve().parent / "logs.txt"


def setup_logging() -> None:
    """Configure file-only logging.

    Returns
    -------
    None
        The root logger is configured to write to ``logs.txt``.
    """
    LOG_FILE.touch(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8")],
        force=True,
    )


def print_banner() -> None:
    """Print a banner at the top of the console.

    Returns
    -------
    None
        Only prints to stdout.
    """
    banner = f"""
{BOLD}{CYAN}===================================
   USA Building Permits Scrapper
==================================={RESET}
"""
    print(banner)


def check_connection(url: str, timeout: float = 5.0) -> bool:
    """Check reachability of a given HTTPS URL via HEAD request.

    Parameters
    ----------
    url : str
        Endpoint to check.
    timeout : float, default=5.0
        Timeout in seconds for the network check.

    Returns
    -------
    bool
        ``True`` if the endpoint appears reachable; ``False`` otherwise.
    """
    try:
        req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout, context=ssl.create_default_context()) as resp:
            return 200 <= getattr(resp, "status", 200) < 500
    except Exception:  # noqa: BLE001 - best-effort network check
        return False


def connection_status_text(url: str, is_up: bool) -> str:
    """Return a colored status line for Accela reachability.

    Parameters
    ----------
    is_up : bool
        Whether the endpoint is reachable.

    Returns
    -------
    str
        Colored status line suitable for console display.
    """
    if is_up:
        return f"{url} Connection status: {GREEN}available{RESET}"
    return (
        f"{url} Connection status: {RED}unavailable{RESET}"
        f" {YELLOW}(hint: try to use a proxy or VPN){RESET}"
    )


def prompt_menu() -> str:
    """Render and prompt the main menu, return selected option as a string.

    Returns
    -------
    str
        The selected menu option ("1", "2", or "3").
    """
    print("1. Scrape list of permits (temporary unavailable)")
    print("2. Scrape details for permits")
    print("3. Convert JSON files to CSV")
    print("4. Exit")
    choice = input(f"\n{BOLD}Select an option [1-4]: {RESET}").strip()
    return choice


def select_scraper(region: str, city: str):  # -> PermitDetailsScraper
    """Return an instantiated scraper for the given region and city.

    Parameters
    ----------
    region : str
        Two-letter state code, e.g., "tx".
    city : str
        City identifier, e.g., "san_antonio".

    Returns
    -------
    Any
        A concrete scraper instance implementing ``scrape_async``.

    Raises
    ------
    ValueError
        If no scraper matches the inputs.
    """
    r = region.lower().strip()
    c = city.lower().strip().replace(" ", "_")
    key = f"{r}-{c}"
    match key:
        case "tx-san_antonio":
            from permits_scraper.scrapers.regions.tx.san_antonio.permit_details import (
                PermitDetailsScraper,
            )

            return PermitDetailsScraper()
        case "tx-austin":
            from permits_scraper.scrapers.regions.tx.austin.permit_details import (
                PermitDetailsScraper,
            )

            return PermitDetailsScraper()
        case _:
            msg = f"No scraper available for region={region!r}, city={city!r}."
            logging.error(msg)
            raise ValueError(msg)


def _flatten(prefix: str, obj: Any, out: Dict[str, Any]) -> None:
    """Flatten a JSON-like object into a single-level dict with dotted keys.

    Parameters
    ----------
    prefix : str
        Current key prefix (dotted path); empty for root.
    obj : Any
        The JSON-like object (dict/list/primitive) to flatten.
    out : Dict[str, Any]
        Target dict to populate with flattened keys and scalar values.

    Notes
    -----
    - Dicts are recursively flattened.
    - Lists are serialized to JSON strings to preserve order and structure.
    - Scalars are stored as-is.
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            _flatten(key, v, out)
    elif isinstance(obj, list):
        out[prefix] = json.dumps(obj, ensure_ascii=False)
    else:
        out[prefix] = obj


def convert_json_folder_to_csv(folder: Path, out_csv: Path) -> int:
    """Convert JSON files in a folder into a single CSV with an id column.

    Parameters
    ----------
    folder : Path
        Directory containing JSON files (non-recursive).
    out_csv : Path
        Destination CSV file path.

    Returns
    -------
    int
        Number of JSON files successfully converted.
    """
    if not folder.exists() or not folder.is_dir():
        raise FileNotFoundError(f"Folder not found or not a directory: {folder}")

    rows: List[Dict[str, Any]] = []
    files = sorted([p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == ".json"])
    for fp in tqdm(files, desc="Converting JSON", leave=True):
        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
            flat: Dict[str, Any] = {}
            _flatten("", data, flat)
            flat["id"] = fp.stem
            rows.append(flat)
        except Exception:
            logging.exception("Failed to convert JSON file: %s", fp)
            continue

    if not rows:
        # Create an empty CSV with only id column
        pd.DataFrame(columns=["id"]).to_csv(out_csv, index=False)
        return 0

    df = pd.DataFrame(rows)
    # Move id column to the front if present
    cols = df.columns.tolist()
    if "id" in cols:
        cols = ["id"] + [c for c in cols if c != "id"]
        df = df[cols]
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    return len(rows)


def chunk_evenly(items: List[str], chunks: int) -> List[List[str]]:
    """Split a list into ``chunks`` contiguous slices with near-even sizes.

    Parameters
    ----------
    items : List[str]
        Input list to partition.
    chunks : int
        Number of slices to produce.

    Returns
    -------
    List[List[str]]
        A list of chunks; order is preserved and sizes differ by at most 1.

    Examples
    --------
    >>> chunk_evenly([1,2,3,4,5], 2)
    [[1, 2, 3], [4, 5]]
    """
    n = max(1, chunks)
    size, remainder = divmod(len(items), n)
    result: List[List[str]] = []
    start = 0
    for i in range(n):
        extra = 1 if i < remainder else 0
        end = start + size + extra
        result.append(items[start:end])
        start = end
    return result


def read_permit_numbers(csv_path: Path, column: str) -> List[str]:
    """Read and return unique permit numbers from a CSV column.

    Parameters
    ----------
    csv_path : Path
        Absolute or relative path to the CSV file containing permit data.
    column : str
        Column name that contains permit identifiers.

    Returns
    -------
    List[str]
        Deduplicated list of permit identifiers, preserving file order.
    """
    df = pd.read_csv(csv_path)
    if column not in df.columns:
        raise KeyError(f"Column {column!r} not found. Available: {list(df.columns)}")
    series = df[column].dropna().astype(str).map(str.strip)
    # Deduplicate preserving order
    seen: Dict[str, None] = {}
    unique = [x for x in series.tolist() if not (x in seen or seen.setdefault(x, None))]
    return unique


async def worker(
    instance_id: int,
    permits: List[str],
    region: str,
    city: str,
    per_bar: tqdm,
    overall_bar: tqdm,
) -> Tuple[int, int]:
    """Run a scraper instance over a chunk of permits.

    Parameters
    ----------
    instance_id : int
        Identifier for the worker (1-based).
    permits : List[str]
        List of permit numbers assigned to this worker.
    region : str
        Region/state code.
    city : str
        City identifier.
    per_bar : tqdm
        Progress bar tracking this worker's progress.
    overall_bar : tqdm
        Shared overall progress bar to be updated in tandem.

    Returns
    -------
    Tuple[int, int]
        (num_success, num_failed) for this worker.
    """
    success = 0
    failed = 0
    scraper = select_scraper(region, city)

    for permit in permits:
        try:
            result_map = await scraper.scrape_async([permit])
            if result_map.get(permit) is not None:
                success += 1
            else:
                failed += 1
        except Exception:
            logging.exception("worker-%s failed on permit %s", instance_id, permit)
            failed += 1
        finally:
            per_bar.set_postfix(success=success, failed=failed)
            per_bar.update(1)
            overall_bar.update(1)

    return success, failed


def run_scrape_details() -> None:
    """Interactively run the "Scrape details for permits" workflow.

    Steps:
    - Ask for region and city
    - Select scraper
    - Ask CSV path and show row count (excluding header)
    - Ask column name containing permit IDs
    - Ask number of concurrent scraper instances
    - Split work into disjoint chunks and run concurrently with progress bars

    Returns
    -------
    None
        Prints progress and completion summary.
    """
    region = input("Enter region/state code (e.g., tx): ").strip()
    city = input("Enter city (e.g., san_antonio): ").strip()

    # Validate scraper availability early
    try:
        _ = select_scraper(region, city)
    except ValueError as e:
        print(f"{RED}{e}{RESET}")
        return

    csv_path_str = input("Enter full path to the CSV with permits: ").strip()
    csv_path = Path(csv_path_str).expanduser().resolve()
    if not csv_path.exists():
        print(f"{RED}CSV file not found: {csv_path}{RESET}")
        return

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:  # noqa: BLE001
        print(f"{RED}Failed to read CSV: {e}{RESET}")
        logging.exception("Failed to read CSV at %s", csv_path)
        return

    print(f"Detected columns: {YELLOW}{list(df.columns)}{RESET}")
    print(f"Rows (excluding header): {BOLD}{len(df)}{RESET}")

    column = input("Enter column name that contains permit IDs: ").strip()
    try:
        permits = read_permit_numbers(csv_path, column)
    except Exception as e:  # noqa: BLE001
        print(f"{RED}{e}{RESET}")
        logging.exception("Invalid column specified")
        return

    if not permits:
        print(f"{RED}No permits found in column {column!r}.{RESET}")
        return

    try:
        instances = int(input("How many scraper instances to run simultaneously? ").strip())
    except ValueError:
        print(f"{RED}Please enter a valid integer for number of instances.{RESET}")
        return

    instances = max(1, min(instances, len(permits)))
    chunks = chunk_evenly(permits, instances)

    total = len(permits)
    overall_bar = tqdm(total=total, position=0, desc="Overall", leave=True)
    per_bars = [
        tqdm(total=len(ch), position=i + 1, desc=f"Instance {i + 1}", leave=True)
        for i, ch in enumerate(chunks)
    ]

    async def runner() -> None:
        tasks = [
            worker(i + 1, ch, region, city, per_bars[i], overall_bar)
            for i, ch in enumerate(chunks)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        ok = 0
        fail = 0
        for res in results:
            if isinstance(res, tuple):
                ok += res[0]
                fail += res[1]
            else:
                fail += 1
        print(f"\n{GREEN}Successfully scraped: {ok}{RESET} | {RED}Failed: {fail}{RESET}")

    try:
        asyncio.run(runner())
    finally:
        for b in per_bars:
            b.close()
        overall_bar.close()


def main() -> None:
    """Entry point to the CLI application.

    Returns
    -------
    None
        Starts the interactive loop until user exits.
    """
    setup_logging()
    while True:
        print_banner()
        check_urls = {"https://aca-prod.accela.com", "https://abc.austintexas.gov"}
        for url in check_urls:
            is_up = check_connection(url=url)
            print(connection_status_text(url, is_up))
        print()
        choice = prompt_menu()
        print()

        if choice == "1":
            print(f"{YELLOW}Option temporarily unavailable.{RESET}\n")
            continue
        if choice == "2":
            run_scrape_details()
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
            except Exception as e:  # noqa: BLE001
                print(f"{RED}Conversion failed: {e}{RESET}")
                logging.exception("Conversion failed")
            input(f"\n{BOLD}Press Enter to return to menu...{RESET}")
            continue
        if choice == "4":
            print("Goodbye!")
            break
        print(f"{RED}Invalid option. Please select 1, 2, 3, or 4.{RESET}\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
