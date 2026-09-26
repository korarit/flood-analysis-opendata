"""
Report Generator for HII Station Data Completeness
==================================================
Identifies and reports which HII Non-MOU stations (Rainfall and Water Level)
have complete data for every month within the requested date range (default: 2019-01 to 2026-07, 91 months).

Outputs:
1. hii_complete_stations_report.csv (Stations that are 100% complete across all months, showing basin and metadata)
2. hii_all_stations_completeness_report.csv (All stations evaluated with completeness percentage and missing months)

Usage:
  # Check completeness via live HII Open Data Catalog (Fast ~10-15s, no large downloads needed)
  python report_hii_completeness.py --source remote

  # Check completeness using locally downloaded monthly CSV files
  python report_hii_completeness.py --source local

  # Filter specific basins or skip basins
  python report_hii_completeness.py --basin yom,nan --skip-basin mun
  python report_hii_completeness.py --type rainfall
  python report_hii_completeness.py --type waterlevel
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
from datetime import datetime
from typing import Dict, List, Set, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# SSL context for HII tiservice
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

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


def generate_month_list(start_ym: str = "201901", end_ym: str = "202607") -> List[str]:
    """Generates a list of YYYYMM strings between start and end (inclusive)."""
    start_dt = datetime.strptime(start_ym.replace("-", ""), "%Y%m")
    end_dt = datetime.strptime(end_ym.replace("-", ""), "%Y%m")
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
    # Check directly under base_dir
    for d in base_dir.iterdir():
        if d.is_dir() and (d / "station").exists():
            found.append(d.name)
    # Check under base_dir / dataset
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
        p = Path(cli_dir).resolve()
        return p
    # If base_dir has station folders directly (e.g. yom/station), use base_dir
    if any((base_dir / b / "station").exists() for b in ["yom", "nan", "ping", "chi"]):
        return base_dir
    if (base_dir / "dataset").exists():
        return base_dir / "dataset"
    return base_dir


def load_all_basin_stations(
    dataset_dir: Path,
    basins: List[str],
    include_mou: bool = False
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Loads full station metadata for specified basins.
    Returns:
    {
      "rainfall": {
         (basin, station_code): {
             "code": str,
             "name_th": str,
             "name_en": str,
             "basin": str,
             "sub_basin_id": str,
             "sub_basin_name_th": str,
             "sub_basin_name_en": str,
             "lat": float,
             "long": float,
             "province": str,
             "amphoe": str,
             "is_mou": bool
         }
      },
      "waterlevel": { ... }
    }
    """
    station_registry: Dict[str, Dict[Tuple[str, str], Dict[str, Any]]] = {
        "rainfall": {},
        "waterlevel": {}
    }

    for basin in basins:
        stn_dir = dataset_dir / basin / "station"
        if not stn_dir.exists():
            continue

        # 1. Rain files
        rain_files = [
            stn_dir / f"{basin}_rain_stations_hii.json",
            stn_dir / f"{basin}_rainfall_stations_hii.json",
            stn_dir / f"{basin}_rain_stations.json",
        ]
        for rf in rain_files:
            if rf.exists():
                try:
                    with open(rf, "r", encoding="utf-8") as f:
                        items = json.load(f)
                    for it in items:
                        stn_info = it.get("station") or {}
                        oldcode = (stn_info.get("tele_station_oldcode") or "").strip().upper()
                        if not oldcode:
                            continue
                        is_mou = oldcode.startswith("MOU")
                        if is_mou and not include_mou:
                            continue

                        name_obj = stn_info.get("tele_station_name") or {}
                        sub_basin_name = stn_info.get("sub_basin_name") or {}
                        geocode = it.get("geocode") or {}

                        record = {
                            "code": oldcode,
                            "name_th": name_obj.get("th") or "",
                            "name_en": name_obj.get("en") or "",
                            "basin": basin,
                            "sub_basin_id": stn_info.get("sub_basin_id") or "",
                            "sub_basin_name_th": sub_basin_name.get("th") or "",
                            "sub_basin_name_en": sub_basin_name.get("en") or "",
                            "lat": stn_info.get("tele_station_lat"),
                            "long": stn_info.get("tele_station_long"),
                            "province": (geocode.get("province_name") or {}).get("th") or "",
                            "amphoe": (geocode.get("amphoe_name") or {}).get("th") or "",
                            "is_mou": is_mou,
                        }
                        station_registry["rainfall"][(basin, oldcode)] = record
                    break
                except Exception as e:
                    print(f"  [WARN] Error reading {rf}: {e}")

        # 2. Waterlevel files
        wl_files = [
            stn_dir / f"{basin}_waterlevel_stations_hii.json",
            stn_dir / f"{basin}_waterlevel_stations.json",
        ]
        for wf in wl_files:
            if wf.exists():
                try:
                    with open(wf, "r", encoding="utf-8") as f:
                        items = json.load(f)
                    for it in items:
                        stn_info = it.get("station") or {}
                        oldcode = (stn_info.get("tele_station_oldcode") or "").strip().upper()
                        if not oldcode:
                            continue
                        raw_code = oldcode.split("-")[-1] if "-" in oldcode else oldcode
                        is_mou = raw_code.startswith("MOU")
                        if is_mou and not include_mou:
                            continue

                        name_obj = stn_info.get("tele_station_name") or {}
                        sub_basin_name = stn_info.get("sub_basin_name") or {}
                        geocode = it.get("geocode") or {}

                        record = {
                            "code": raw_code,
                            "full_code": oldcode,
                            "name_th": name_obj.get("th") or "",
                            "name_en": name_obj.get("en") or "",
                            "basin": basin,
                            "sub_basin_id": stn_info.get("sub_basin_id") or "",
                            "sub_basin_name_th": sub_basin_name.get("th") or "",
                            "sub_basin_name_en": sub_basin_name.get("en") or "",
                            "lat": stn_info.get("tele_station_lat"),
                            "long": stn_info.get("tele_station_long"),
                            "river": stn_info.get("river_name") or "",
                            "province": (geocode.get("province_name") or {}).get("th") or "",
                            "amphoe": (geocode.get("amphoe_name") or {}).get("th") or "",
                            "is_mou": is_mou,
                        }
                        station_registry["waterlevel"][(basin, raw_code)] = record
                    break
                except Exception as e:
                    print(f"  [WARN] Error reading {wf}: {e}")

    return station_registry


def fetch_remote_monthly_catalog(
    catalog_path: str,
    months: List[str],
    max_workers: int = 12
) -> Dict[str, Set[str]]:
    """
    Fetches file lists for all months concurrently from HII Open Data Catalog.
    Returns: { YYYYMM: { station_codes... } }
    """
    base_url = f"https://tiservice.hii.or.th/opendata/data_catalog/{catalog_path}/"
    results: Dict[str, Set[str]] = {}

    def fetch_single_month(ym: str) -> Tuple[str, Set[str]]:
        year = ym[:4]
        url = f"{base_url}{year}/{ym}/"
        req = urllib.request.Request(url, headers=HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=12, context=SSL_CTX) as resp:
                text = resp.read().decode("utf-8", errors="ignore")
                matches = re.findall(r'href=[\x22\x27]([^\x22\x27]+\.csv)[\x22\x27]', text, re.I)
                stns = {
                    Path(f).stem.upper()
                    for f in matches
                    if f.lower() != "0station_metadata.csv"
                }
                return ym, stns
        except Exception:
            return ym, set()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_single_month, ym): ym for ym in months}
        for future in as_completed(futures):
            ym, stn_set = future.result()
            results[ym] = stn_set

    return results


def check_local_station_availability(
    dataset_dir: Path,
    basins: List[str],
    data_type: str,
    months: List[str]
) -> Dict[Tuple[str, str], Set[str]]:
    """
    Scans local CSV files in dataset/{basin}/{data_type}/ and determines
    which months have data for each (basin, station_code).
    Returns: { (basin, station_code): { YYYYMM, ... } }
    """
    folder_name = "rainfall" if data_type == "rainfall" else "waterlevel"
    cat_tag = "rain_non_mou" if data_type == "rainfall" else "wl_non_mou"
    station_months: Dict[Tuple[str, str], Set[str]] = {}

    for basin in basins:
        data_dir = dataset_dir / basin / folder_name
        if not data_dir.exists():
            continue

        for ym in months:
            csv_path = data_dir / f"{basin}_hii_{cat_tag}_{ym}.csv"
            if not csv_path.exists() or csv_path.stat().st_size < 100:
                continue

            try:
                with open(csv_path, "r", encoding="utf-8-sig") as f:
                    reader = csv.reader(f)
                    header = next(reader, None)
                    for row in reader:
                        if not row:
                            continue
                        code = row[0].strip().upper()
                        key = (basin, code)
                        if key not in station_months:
                            station_months[key] = set()
                        station_months[key].add(ym)
            except Exception as e:
                print(f"  [WARN] Error reading local file {csv_path.name}: {e}")

    return station_months


def build_completeness_report(
    station_registry: Dict[str, Dict[Tuple[str, str], Dict[str, Any]]],
    months: List[str],
    source: str = "remote",
    dataset_dir: Optional[Path] = None,
    basins: Optional[List[str]] = None,
    max_workers: int = 12
) -> List[Dict[str, Any]]:
    """
    Builds detailed completeness records for every station.
    """
    total_months_count = len(months)
    report_rows = []

    for data_type in ["rainfall", "waterlevel"]:
        stations = station_registry[data_type]
        if not stations:
            continue

        cat_path = "hourly_rain" if data_type == "rainfall" else "water_level"

        if source == "remote":
            print(f"  Scanning HII Open Data Catalog for {data_type} ({len(months)} months)...")
            month_catalog = fetch_remote_monthly_catalog(cat_path, months, max_workers=max_workers)
            
            for (basin, stn_code), meta in stations.items():
                avail_months = []
                missing_months = []
                for ym in months:
                    if stn_code in month_catalog.get(ym, set()):
                        avail_months.append(ym)
                    else:
                        missing_months.append(ym)

                avail_cnt = len(avail_months)
                pct = (avail_cnt / total_months_count) * 100.0 if total_months_count > 0 else 0.0
                is_comp = (avail_cnt == total_months_count)

                report_rows.append({
                    "station_code": stn_code,
                    "station_name_th": meta["name_th"],
                    "station_name_en": meta["name_en"],
                    "basin": basin,
                    "sub_basin_id": meta["sub_basin_id"],
                    "sub_basin_name_th": meta["sub_basin_name_th"],
                    "sub_basin_name_en": meta["sub_basin_name_en"],
                    "data_type": data_type,
                    "lat": meta["lat"],
                    "long": meta["long"],
                    "province": meta["province"],
                    "amphoe": meta["amphoe"],
                    "total_expected_months": total_months_count,
                    "available_months_count": avail_cnt,
                    "completeness_pct": f"{pct:.1f}%",
                    "is_complete": is_comp,
                    "first_available_month": avail_months[0] if avail_months else "",
                    "last_available_month": avail_months[-1] if avail_months else "",
                    "missing_months_count": len(missing_months),
                    "missing_months_sample": ",".join(missing_months[:6]) + ("..." if len(missing_months) > 6 else ""),
                    "missing_months_all": ",".join(missing_months),
                })

        else:
            # Local source
            print(f"  Scanning local downloaded files for {data_type} in {len(basins or [])} basins...")
            local_map = check_local_station_availability(dataset_dir, basins or [], data_type, months)

            for (basin, stn_code), meta in stations.items():
                found_set = local_map.get((basin, stn_code), set())
                avail_months = [ym for ym in months if ym in found_set]
                missing_months = [ym for ym in months if ym not in found_set]

                avail_cnt = len(avail_months)
                pct = (avail_cnt / total_months_count) * 100.0 if total_months_count > 0 else 0.0
                is_comp = (avail_cnt == total_months_count)

                report_rows.append({
                    "station_code": stn_code,
                    "station_name_th": meta["name_th"],
                    "station_name_en": meta["name_en"],
                    "basin": basin,
                    "sub_basin_id": meta["sub_basin_id"],
                    "sub_basin_name_th": meta["sub_basin_name_th"],
                    "sub_basin_name_en": meta["sub_basin_name_en"],
                    "data_type": data_type,
                    "lat": meta["lat"],
                    "long": meta["long"],
                    "province": meta["province"],
                    "amphoe": meta["amphoe"],
                    "total_expected_months": total_months_count,
                    "available_months_count": avail_cnt,
                    "completeness_pct": f"{pct:.1f}%",
                    "is_complete": is_comp,
                    "first_available_month": avail_months[0] if avail_months else "",
                    "last_available_month": avail_months[-1] if avail_months else "",
                    "missing_months_count": len(missing_months),
                    "missing_months_sample": ",".join(missing_months[:6]) + ("..." if len(missing_months) > 6 else ""),
                    "missing_months_all": ",".join(missing_months),
                })

    return report_rows


def export_reports(
    report_rows: List[Dict[str, Any]],
    out_complete_csv: Path,
    out_all_csv: Path
):
    """Writes both the complete-only report and the full report CSV files."""
    if not report_rows:
        print("  [WARN] No records to export.")
        return

    # 1. Complete stations only
    complete_rows = [r for r in report_rows if r["is_complete"]]
    complete_headers = [
        "station_code",
        "station_name_th",
        "station_name_en",
        "basin",
        "sub_basin_id",
        "sub_basin_name_th",
        "sub_basin_name_en",
        "data_type",
        "lat",
        "long",
        "province",
        "amphoe",
        "total_expected_months",
        "available_months_count",
        "completeness_pct",
        "first_available_month",
        "last_available_month"
    ]

    out_complete_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(out_complete_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=complete_headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(complete_rows)

    print(f"  -> Saved COMPLETE stations report: {out_complete_csv} ({len(complete_rows):,} stations)")

    # 2. All stations report
    all_headers = [
        "station_code",
        "station_name_th",
        "station_name_en",
        "basin",
        "sub_basin_id",
        "sub_basin_name_th",
        "sub_basin_name_en",
        "data_type",
        "lat",
        "long",
        "province",
        "amphoe",
        "total_expected_months",
        "available_months_count",
        "completeness_pct",
        "is_complete",
        "first_available_month",
        "last_available_month",
        "missing_months_count",
        "missing_months_sample",
        "missing_months_all"
    ]

    out_all_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(out_all_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(report_rows)

    print(f"  -> Saved ALL stations completeness report: {out_all_csv} ({len(report_rows):,} stations)")


def print_summary_table(report_rows: List[Dict[str, Any]], target_basins: List[str]):
    """Prints a structured summary table grouped by Basin and Data Type."""
    print("\n" + "=" * 95)
    print("  HII NON-MOU STATION COMPLETENESS SUMMARY (2019-01 to 2026-07)")
    print("=" * 95)
    print(f"{'Basin':<14} | {'Rain Total':>10} | {'Rain Complete':>13} | {'WL Total':>8} | {'WL Complete':>11} | {'Total Complete':>14}")
    print("-" * 95)

    basin_stats = {
        b: {"rain_total": 0, "rain_comp": 0, "wl_total": 0, "wl_comp": 0}
        for b in target_basins
    }

    for r in report_rows:
        b = r["basin"]
        if b not in basin_stats:
            basin_stats[b] = {"rain_total": 0, "rain_comp": 0, "wl_total": 0, "wl_comp": 0}
        dt = r["data_type"]
        is_c = r["is_complete"]
        if dt == "rainfall":
            basin_stats[b]["rain_total"] += 1
            if is_c:
                basin_stats[b]["rain_comp"] += 1
        else:
            basin_stats[b]["wl_total"] += 1
            if is_c:
                basin_stats[b]["wl_comp"] += 1

    total_rain_stns = 0
    total_rain_comp = 0
    total_wl_stns = 0
    total_wl_comp = 0

    for b in target_basins:
        st = basin_stats[b]
        tot_c = st["rain_comp"] + st["wl_comp"]
        total_rain_stns += st["rain_total"]
        total_rain_comp += st["rain_comp"]
        total_wl_stns += st["wl_total"]
        total_wl_comp += st["wl_comp"]
        print(
            f"{b:<14} | {st['rain_total']:>10} | {st['rain_comp']:>13} | {st['wl_total']:>8} | {st['wl_comp']:>11} | {tot_c:>14}"
        )

    print("-" * 95)
    all_comp = total_rain_comp + total_wl_comp
    print(
        f"{'TOTAL':<14} | {total_rain_stns:>10} | {total_rain_comp:>13} | {total_wl_stns:>8} | {total_wl_comp:>11} | {all_comp:>14}"
    )
    print("=" * 95 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Check and report HII station data completeness across months (2019-2026)."
    )
    parser.add_argument("--dir", default=None, help="Root dataset directory (default: auto-detect)")
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
        help="Data type to evaluate: all, rainfall, or waterlevel (default: all)",
    )
    parser.add_argument(
        "--source",
        choices=["auto", "remote", "local"],
        default="auto",
        help="Data source to verify: remote (HII online catalog), local (downloaded CSVs), or auto (default)",
    )
    parser.add_argument(
        "--out-complete",
        default="hii_complete_stations_report.csv",
        help="Output CSV for complete stations (default: hii_complete_stations_report.csv)",
    )
    parser.add_argument(
        "--out-all",
        default="hii_all_stations_completeness_report.csv",
        help="Output CSV for all stations (default: hii_all_stations_completeness_report.csv)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=12,
        help="Number of concurrent worker threads for remote catalog checks (default: 12)",
    )
    parser.add_argument(
        "--include-mou",
        action="store_true",
        help="Include MOU stations (default: False, Non-MOU only)",
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
        print("[ERROR] No valid target basins found after filtering.")
        sys.exit(1)

    months = generate_month_list(args.start, args.end)

    print("=" * 80)
    print("  HII STATION COMPLETENESS ANALYZER & REPORT GENERATOR")
    print("=" * 80)
    print(f"Target Basins : {', '.join(target_basins)}")
    print(f"Date Range    : {months[0]} to {months[-1]} ({len(months)} months)")
    print(f"Data Types    : {args.type}")
    print(f"Non-MOU Only  : {not args.include_mou}")
    print(f"Dataset Root  : {dataset_dir}")

    # Auto detect source if auto
    source = args.source
    if source == "auto":
        # Check if local files for 2019 exist
        test_yom_local = dataset_dir / "yom" / "rainfall" / "yom_hii_rain_non_mou_201901.csv"
        if test_yom_local.exists():
            source = "local"
            print(f"Verification  : Local downloaded CSVs (detected existing files)")
        else:
            source = "remote"
            print(f"Verification  : HII Open Data Catalog Remote Index (Fast live scan)")
    else:
        print(f"Verification  : {source.capitalize()}")
    print("=" * 80)

    # 1. Load station metadata
    print("\n[Step 1/3] Loading station metadata from station directories...")
    station_registry = load_all_basin_stations(dataset_dir, target_basins, include_mou=args.include_mou)

    # Filter type if requested
    if args.type == "rainfall":
        station_registry["waterlevel"] = {}
    elif args.type == "waterlevel":
        station_registry["rainfall"] = {}

    rain_count = len(station_registry["rainfall"])
    wl_count = len(station_registry["waterlevel"])
    print(f"  Loaded {rain_count} Non-MOU rainfall stations, {wl_count} Non-MOU waterlevel stations.")

    # 2. Check completeness
    print(f"\n[Step 2/3] Checking monthly availability ({source} mode)...")
    report_rows = build_completeness_report(
        station_registry=station_registry,
        months=months,
        source=source,
        dataset_dir=dataset_dir,
        basins=target_basins,
        max_workers=args.workers,
    )

    # 3. Export CSV reports
    print("\n[Step 3/3] Exporting CSV reports...")
    out_complete = Path(args.out_complete)
    out_all = Path(args.out_all)
    export_reports(report_rows, out_complete, out_all)

    # 4. Print summary table
    print_summary_table(report_rows, target_basins)


if __name__ == "__main__":
    main()
