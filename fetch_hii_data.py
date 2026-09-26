"""
Script to download historical hourly Rainfall and Water Level data from HII Open Data Catalog.
Supports 2019-01 to 2026-07 for Non-MOU stations (and optional MOU datasets for 2024-2026).

Saves data into {basin}/rainfall/ and {basin}/waterlevel/ (or dataset/{basin}/...).

Usage Examples:
  # Test run downloading 5 stations for 1 month in Yom basin
  python fetch_hii_data.py --basin yom --limit 5 --smoke-test

  # Full download for all basins (201901 - 202607, non-MOU rain + waterlevel)
  python fetch_hii_data.py --basin all

  # Target specific basins and skip others
  python fetch_hii_data.py --basin yom,nan,ping --skip-basin mun,chi

  # Download only rainfall or only water level
  python fetch_hii_data.py --type rainfall
  python fetch_hii_data.py --type waterlevel

  # Generate completeness CSV report after download
  python fetch_hii_data.py --report
"""

import os
import sys
import json
import csv
import re
import argparse
import urllib.request
import ssl
from pathlib import Path
from typing import Dict, List, Set, Any, Optional, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Ignore SSL verification for older institutional certs
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

HII_CATALOGS = {
    "rain_non_mou": {
        "base_url": "https://tiservice.hii.or.th/opendata/data_catalog/hourly_rain/",
        "data_type": "rainfall",
        "agency_category": "hii",
    },
    "rain_mou": {
        "base_url": "https://tiservice.hii.or.th/opendata/data_catalog_mou/hourly_rain_mou/",
        "data_type": "rainfall",
        "agency_category": "mou",
    },
    "wl_non_mou": {
        "base_url": "https://tiservice.hii.or.th/opendata/data_catalog/water_level/",
        "data_type": "waterlevel",
        "agency_category": "hii",
    },
    "wl_mou": {
        "base_url": "https://tiservice.hii.or.th/opendata/data_catalog_mou/water_level_mou/",
        "data_type": "waterlevel",
        "agency_category": "mou",
    },
}

# Attempt import from flood-analysis-model
try:
    from scripts.modules.basin_registry import get_all_slugs, get_basin
except ModuleNotFoundError:
    model_dir = Path(__file__).resolve().parent.parent / "flood-analysis-model"
    if model_dir.exists() and str(model_dir) not in sys.path:
        sys.path.insert(0, str(model_dir))
    try:
        from scripts.modules.basin_registry import get_all_slugs, get_basin
    except ModuleNotFoundError:
        def get_all_slugs() -> List[str]:
            return ["chi", "khong-north", "mun", "nan", "pa-sak", "ping", "wang", "yom"]
        def get_basin(slug: str) -> Dict[str, Any]:
            return {"slug": slug, "name_th": slug, "name_en": slug}


def generate_month_list(start_year_month: str = "201901", end_year_month: str = "202607") -> List[str]:
    """Generates a list of YYYYMM strings between start and end (inclusive)."""
    start_dt = datetime.strptime(start_year_month.replace("-", ""), "%Y%m")
    end_dt = datetime.strptime(end_year_month.replace("-", ""), "%Y%m")
    
    months = []
    curr = start_dt
    while curr <= end_dt:
        months.append(curr.strftime("%Y%m"))
        year = curr.year + (1 if curr.month == 12 else 0)
        month = 1 if curr.month == 12 else curr.month + 1
        curr = datetime(year, month, 1)
    return months


def discover_available_basins(base_dir: Path) -> List[str]:
    """Finds all basins that have a station/ subdirectory."""
    known_slugs = get_all_slugs()
    found = []
    for d in base_dir.iterdir():
        if d.is_dir() and (d / "station").exists():
            found.append(d.name)
    dataset_sub = base_dir / "dataset"
    if dataset_sub.exists() and dataset_sub.is_dir():
        for d in dataset_sub.iterdir():
            if d.is_dir() and (d / "station").exists() and d.name not in found:
                found.append(d.name)
    if not found:
        return [b for b in known_slugs if (base_dir / b).exists()]
    return sorted(found)


def resolve_dataset_root(base_dir: Path, cli_dir: Optional[str] = None) -> Path:
    """Detects if dataset root is base_dir or base_dir/dataset."""
    if cli_dir:
        return Path(cli_dir).resolve()
    if any((base_dir / b / "station").exists() for b in ["yom", "nan", "ping", "chi"]):
        return base_dir
    if (base_dir / "dataset").exists():
        return base_dir / "dataset"
    return base_dir


def load_basin_station_codes(dataset_dir: Path, target_basins: List[str]) -> Dict[str, Dict[str, Set[str]]]:
    """
    Loads full station oldcodes from dataset/{basin}/station/
    Returns {basin: {'rain_hii': {...}, 'rain_mou': {...}, 'wl_hii': {...}, 'wl_mou': {...}}}
    """
    basin_stations: Dict[str, Dict[str, Set[str]]] = {}
    for basin in target_basins:
        stn_dir = dataset_dir / basin / "station"
        basin_stations[basin] = {
            "rain_hii": set(),
            "rain_mou": set(),
            "wl_hii": set(),
            "wl_mou": set(),
        }
        
        if not stn_dir.exists():
            print(f"  [WARN] Station directory not found: {stn_dir}")
            continue

        # Rain HII + MOU
        rain_files = [
            stn_dir / f"{basin}_rain_stations_hii.json",
            stn_dir / f"{basin}_rainfall_stations_hii.json",
            stn_dir / f"{basin}_rain_stations.json",
            stn_dir / f"{basin}_rainfall_stations.json",
        ]
        rain_loaded = False
        for rf in rain_files:
            if rf.exists() and not rain_loaded:
                try:
                    with open(rf, "r", encoding="utf-8") as f:
                        items = json.load(f)
                        for it in items:
                            oldcode = (it.get("station") or {}).get("tele_station_oldcode", "").strip()
                            if oldcode:
                                if oldcode.upper().startswith("MOU"):
                                    basin_stations[basin]["rain_mou"].add(oldcode)
                                else:
                                    basin_stations[basin]["rain_hii"].add(oldcode)
                    rain_loaded = True
                except Exception as e:
                    print(f"  [WARN] Error reading {rf.name}: {e}")

        # Waterlevel HII + MOU
        wl_files = [
            stn_dir / f"{basin}_waterlevel_stations_hii.json",
            stn_dir / f"{basin}_waterlevel_stations.json",
        ]
        wl_loaded = False
        for wf in wl_files:
            if wf.exists() and not wl_loaded:
                try:
                    with open(wf, "r", encoding="utf-8") as f:
                        items = json.load(f)
                        for it in items:
                            oldcode = (it.get("station") or {}).get("tele_station_oldcode", "").strip()
                            if oldcode:
                                raw_code = oldcode.split("-")[-1] if "-" in oldcode else oldcode
                                if raw_code.upper().startswith("MOU"):
                                    basin_stations[basin]["wl_mou"].add(raw_code)
                                else:
                                    basin_stations[basin]["wl_hii"].add(raw_code)
                                    basin_stations[basin]["wl_hii"].add(oldcode)
                    wl_loaded = True
                except Exception as e:
                    print(f"  [WARN] Error reading {wf.name}: {e}")

        total_stn = (
            len(basin_stations[basin]["rain_hii"])
            + len(basin_stations[basin]["rain_mou"])
            + len(basin_stations[basin]["wl_hii"])
            + len(basin_stations[basin]["wl_mou"])
        )
        if total_stn == 0:
            print(f"  [WARN] No HII/MOU stations found in {stn_dir}.")

    return basin_stations


def fetch_file_content(url: str, timeout: int = 15) -> Optional[str]:
    """Fetches text content from URL with retry."""
    req = urllib.request.Request(url, headers=HEADERS)
    for _ in range(2):
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as resp:
                return resp.read().decode("utf-8", errors="ignore")
        except Exception:
            pass
    return None


def normalize_station_csv_rows(
    station_code: str,
    raw_csv_text: str,
    data_type: str
) -> List[List[str]]:
    """
    Parses and standardizes station CSV text to uniform 4-column schema:
    [station_code, datetime, value, quality_flag]
    where datetime is YYYY-MM-DD HH:MM:00 (or HH:MM:SS)

    Handles both:
    - 2025+ format: station_code,measure_datetime,rainfall_1h/water_level,quality_flag
    - 2019-2024 format: date,time,rain/water_lv (without station_code column)
    """
    lines = [l.strip() for l in raw_csv_text.splitlines() if l.strip()]
    if len(lines) <= 1:
        return []

    header = [c.strip().lower() for c in lines[0].split(",")]
    normalized_rows = []

    # Case A: 2025+ format with station_code column
    if "station_code" in header:
        stn_idx = header.index("station_code")
        dt_idx = header.index("measure_datetime") if "measure_datetime" in header else 1
        val_idx = 2
        flag_idx = 3 if len(header) > 3 else None

        for line in lines[1:]:
            parts = [p.strip() for p in line.split(",")]
            if len(parts) <= max(stn_idx, dt_idx, val_idx):
                continue
            stn = parts[stn_idx] or station_code
            dt = parts[dt_idx]
            val = parts[val_idx]
            flag = parts[flag_idx] if flag_idx is not None and len(parts) > flag_idx else ""
            normalized_rows.append([stn, dt, val, flag])

    # Case B: 2019-2024 format with date,time,...
    elif "date" in header and "time" in header:
        d_idx = header.index("date")
        t_idx = header.index("time")
        val_idx = 2
        flag_idx = 3 if len(header) > 3 else None

        for line in lines[1:]:
            parts = [p.strip() for p in line.split(",")]
            if len(parts) <= max(d_idx, t_idx, val_idx):
                continue
            date_str = parts[d_idx]
            time_str = parts[t_idx]
            # Standardize datetime
            if len(time_str) == 5:  # HH:MM
                dt_str = f"{date_str} {time_str}:00"
            else:
                dt_str = f"{date_str} {time_str}"
            val = parts[val_idx]
            flag = parts[flag_idx] if flag_idx is not None and len(parts) > flag_idx else ""
            normalized_rows.append([station_code, dt_str, val, flag])

    # Case C: Fallback
    else:
        for line in lines[1:]:
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 3:
                normalized_rows.append([station_code, parts[1], parts[2], parts[3] if len(parts) > 3 else ""])

    return normalized_rows


def download_hii_data(
    dataset_dir: Path,
    target_basins: List[str],
    months: List[str],
    catalogs_to_run: Optional[List[str]] = None,
    station_limit: Optional[int] = None,
    smoke_test: bool = False,
    skip_existing: bool = True,
    workers: int = 8,
):
    print("=" * 80)
    print("  HII OPEN DATA DOWNLOADER (2019-01 - 2026-07)")
    print("=" * 80)
    print(f"Target Basins  : {', '.join(target_basins)}")
    print(f"Months to fetch: {months[0]} to {months[-1]} (Total: {len(months)} months)")
    print(f"Catalogs       : {', '.join(catalogs_to_run or list(HII_CATALOGS.keys()))}")
    print(f"Skip Existing  : {skip_existing}")
    if station_limit:
        print(f"Station Limit  : Max {station_limit} stations per basin (Test/Sample Mode)")
    if smoke_test:
        print(">>> RUNNING IN SMOKE TEST MODE (First month only) <<<")

    station_mapping = load_basin_station_codes(dataset_dir, target_basins)
    selected_catalogs = catalogs_to_run or ["rain_non_mou", "wl_non_mou"]

    stats = {b: {"rainfall_records": 0, "waterlevel_records": 0, "months_downloaded": 0} for b in target_basins}
    active_months = months[:1] if smoke_test else months

    # Standard headers for output files
    rain_header = ["station_code", "measure_datetime", "rainfall_1h", "quality_flag"]
    wl_header = ["station_code", "measure_datetime", "water_level", "quality_flag"]

    for cat_key in selected_catalogs:
        if cat_key not in HII_CATALOGS:
            continue
        cat_info = HII_CATALOGS[cat_key]
        base_url = cat_info["base_url"]
        data_type = cat_info["data_type"]
        agency_cat = cat_info["agency_category"]

        print(f"\n--- Processing Catalog: {cat_key} ({base_url}) ---")

        for yyyymm in active_months:
            year = yyyymm[:4]
            month_dir_url = f"{base_url}{year}/{yyyymm}/"

            # Check if all target basins already have this month downloaded
            basins_needing_download = []
            for basin in target_basins:
                out_folder = dataset_dir / basin / data_type
                out_csv = out_folder / f"{basin}_hii_{cat_key}_{yyyymm}.csv"
                if skip_existing and out_csv.exists() and out_csv.stat().st_size > 200:
                    continue
                basins_needing_download.append(basin)

            if not basins_needing_download:
                continue

            print(f"  [{yyyymm}] Checking directory: {month_dir_url}")
            dir_html = fetch_file_content(month_dir_url)
            if not dir_html:
                print(f"    [WARN] Could not read directory {month_dir_url} (or not available)")
                continue

            available_files = re.findall(r'href=[\x22\x27]([^\x22\x27]+\.csv)[\x22\x27]', dir_html, re.I)
            available_stn_map = {Path(f).stem.upper(): f for f in available_files if f.lower() != "0station_metadata.csv"}
            print(f"    Available station CSVs on server: {len(available_stn_map):,}")

            for basin in basins_needing_download:
                key = f"{'rain' if data_type == 'rainfall' else 'wl'}_{agency_cat}"
                basin_codes = station_mapping[basin].get(key, set())

                # Match stations with available files (avoiding duplicate filenames)
                matched_pairs: List[Tuple[str, str]] = []  # (station_code, filename)
                seen_files = set()
                for c in basin_codes:
                    c_clean = c.split("-")[-1] if "-" in c else c
                    if c.upper() in available_stn_map and available_stn_map[c.upper()] not in seen_files:
                        fn = available_stn_map[c.upper()]
                        matched_pairs.append((c_clean.upper(), fn))
                        seen_files.add(fn)
                    elif c_clean.upper() in available_stn_map and available_stn_map[c_clean.upper()] not in seen_files:
                        fn = available_stn_map[c_clean.upper()]
                        matched_pairs.append((c_clean.upper(), fn))
                        seen_files.add(fn)

                if station_limit:
                    matched_pairs = matched_pairs[:station_limit]

                if not matched_pairs:
                    continue

                out_folder = dataset_dir / basin / data_type
                out_folder.mkdir(parents=True, exist_ok=True)
                out_csv = out_folder / f"{basin}_hii_{cat_key}_{yyyymm}.csv"

                print(f"    [{basin.upper()}] Downloading {len(matched_pairs)} stations -> {out_csv.name}")

                # Download station CSVs concurrently
                def download_single_station(item: Tuple[str, str]) -> List[List[str]]:
                    stn_code, fn = item
                    csv_url = f"{month_dir_url}{fn}"
                    content = fetch_file_content(csv_url)
                    if not content:
                        return []
                    return normalize_station_csv_rows(stn_code, content, data_type)

                all_rows = []
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futures = [pool.submit(download_single_station, p) for p in matched_pairs]
                    for fut in as_completed(futures):
                        try:
                            rows = fut.result()
                            if rows:
                                all_rows.extend(rows)
                        except Exception:
                            pass

                if all_rows:
                    target_header = rain_header if data_type == "rainfall" else wl_header
                    with open(out_csv, "w", encoding="utf-8-sig", newline="") as f:
                        writer = csv.writer(f)
                        writer.writerow(target_header)
                        writer.writerows(all_rows)

                    if data_type == "rainfall":
                        stats[basin]["rainfall_records"] += len(all_rows)
                    else:
                        stats[basin]["waterlevel_records"] += len(all_rows)
                    stats[basin]["months_downloaded"] += 1
                    print(f"      -> Saved {len(all_rows):,} rows ({len(matched_pairs)} stations) to {out_csv.name}")

    print("\n" + "=" * 80)
    print("  HII DATA DOWNLOAD SESSION SUMMARY")
    print("=" * 80)
    print(f"{'Basin':<14} | {'Rain Records':>14} | {'WL Records':>12} | {'Months Downloaded':>18}")
    print("-" * 80)
    for b in target_basins:
        print(
            f"{b:<14} | {stats[b]['rainfall_records']:>14,} | {stats[b]['waterlevel_records']:>12,} | {stats[b]['months_downloaded']:>18}"
        )
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="Download historical HII rainfall and water level data (2019-2026 Non-MOU)."
    )
    parser.add_argument("--dir", default=None, help="Root directory for dataset (default: auto-detect)")
    parser.add_argument(
        "--basin",
        default="all",
        help="Target basin(s): all, or comma-separated list (e.g. yom,nan,ping)",
    )
    parser.add_argument(
        "--skip-basin",
        default="",
        help="Basin(s) to skip/exclude: comma-separated list (e.g. mun,chi,pa-sak)",
    )
    parser.add_argument("--start", default="201901", help="Start month (YYYYMM), default: 201901")
    parser.add_argument("--end", default="202607", help="End month (YYYYMM), default: 202607")
    parser.add_argument(
        "--type",
        choices=["all", "rainfall", "waterlevel"],
        default="all",
        help="Data type to download: all (rainfall + waterlevel), rainfall, or waterlevel (default: all)",
    )
    parser.add_argument(
        "--include-mou",
        action="store_true",
        help="Include MOU catalogs (default: False, Non-MOU only)",
    )
    parser.add_argument(
        "--catalog",
        choices=list(HII_CATALOGS.keys()),
        help="Specific catalog to download (overrides --type and --include-mou)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of stations per basin to download (e.g. --limit 5 for testing)",
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help="Run quick smoke test on 1 month and sample stations",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing monthly CSV files (default: skip existing)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of concurrent worker threads for downloads (default: 8)",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Generate completeness CSV report after download",
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Skip download and generate completeness report directly",
    )

    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent
    dataset_dir = resolve_dataset_root(base_dir, args.dir)

    all_available = discover_available_basins(dataset_dir)
    if not all_available:
        all_available = discover_available_basins(base_dir)

    # Filter basins
    if args.basin.lower() == "all":
        target_basins = all_available
    else:
        req_basins = [b.strip().lower() for b in args.basin.split(",") if b.strip()]
        target_basins = [b for b in req_basins if b in all_available or b in get_all_slugs()]

    # Skip basins
    if args.skip_basin:
        skip_list = {b.strip().lower() for b in args.skip_basin.split(",") if b.strip()}
        target_basins = [b for b in target_basins if b not in skip_list]

    if not target_basins:
        print("[ERROR] No valid target basins found.")
        sys.exit(1)

    # Determine catalogs
    if args.catalog:
        catalogs = [args.catalog]
    else:
        catalogs = []
        if args.type in ["all", "rainfall"]:
            catalogs.append("rain_non_mou")
            if args.include_mou:
                catalogs.append("rain_mou")
        if args.type in ["all", "waterlevel"]:
            catalogs.append("wl_non_mou")
            if args.include_mou:
                catalogs.append("wl_mou")

    # If --report-only, jump to report
    if args.report_only:
        from report_hii_completeness import (
            load_all_basin_stations,
            build_completeness_report,
            export_reports,
            print_summary_table,
        )
        months = generate_month_list(args.start, args.end)
        stn_reg = load_all_basin_stations(dataset_dir, target_basins, include_mou=args.include_mou)
        if args.type == "rainfall":
            stn_reg["waterlevel"] = {}
        elif args.type == "waterlevel":
            stn_reg["rainfall"] = {}
        rows = build_completeness_report(stn_reg, months, source="auto", dataset_dir=dataset_dir, basins=target_basins)
        export_reports(rows, Path("hii_complete_stations_report.csv"), Path("hii_all_stations_completeness_report.csv"))
        print_summary_table(rows, target_basins)
        return

    months = generate_month_list(args.start, args.end)

    # Download
    download_hii_data(
        dataset_dir=dataset_dir,
        target_basins=target_basins,
        months=months,
        catalogs_to_run=catalogs,
        station_limit=args.limit,
        smoke_test=args.smoke_test,
        skip_existing=not args.overwrite,
        workers=args.workers,
    )

    # If --report requested
    if args.report:
        from report_hii_completeness import (
            load_all_basin_stations,
            build_completeness_report,
            export_reports,
            print_summary_table,
        )
        print("\n>>> Generating Station Completeness CSV Report <<<")
        stn_reg = load_all_basin_stations(dataset_dir, target_basins, include_mou=args.include_mou)
        if args.type == "rainfall":
            stn_reg["waterlevel"] = {}
        elif args.type == "waterlevel":
            stn_reg["rainfall"] = {}
        rows = build_completeness_report(stn_reg, months, source="auto", dataset_dir=dataset_dir, basins=target_basins)
        export_reports(rows, Path("hii_complete_stations_report.csv"), Path("hii_all_stations_completeness_report.csv"))
        print_summary_table(rows, target_basins)


if __name__ == "__main__":
    main()
