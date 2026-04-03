"""
IPI Pipe – Bulk Consent Decree ETL  (v3 — ICIS bulk download)

Downloads EPA ICIS-FE&C bulk data files (case_downloads.zip, ~73 MB) and
NPDES enforcement data (npdes_downloads.zip, ~300 MB), filters for CWA/SDWA
municipal consent decrees and state enforcement actions, enriches with Census
population data, and loads into Google BigQuery.

This approach yields FAR more records than the ECHO REST API (v2), because it
processes the complete ICIS database rather than scanning facility-by-facility.

Expected yield: 150–250+ CWA/SDWA consent decree and enforcement records.

Usage:
    python etl_bulk.py
    python etl_bulk.py --dry-run
    python etl_bulk.py --years 10
    python etl_bulk.py --skip-npdes         # Only process ICIS-FE&C (faster)
    python etl_bulk.py --dry-run --years 5
"""

import argparse
import csv
import io
import logging
import os
import sys
import tempfile
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from logging.handlers import RotatingFileHandler
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# EPA ECHO bulk download URLs — public, no auth needed, updated weekly
CASE_DOWNLOADS_URL = "https://echo.epa.gov/files/echodownloads/case_downloads.zip"
NPDES_DOWNLOADS_URL = "https://echo.epa.gov/files/echodownloads/npdes_downloads.zip"

# Census API
CENSUS_ACS_BASE = "https://api.census.gov/data"

MAX_YEARS_BACK = 15

# ---------------------------------------------------------------------------
# SIC / NAICS codes for municipal water / wastewater
# ---------------------------------------------------------------------------

MUNICIPAL_SIC_CODES = {"4952", "4941", "4911", "4959", "4953"}
MUNICIPAL_NAICS_CODES = {
    "221310",  # Water Supply and Irrigation Systems
    "221320",  # Sewage Treatment Facilities
    "221210",  # Natural Gas Distribution (cross-ref)
    "562111",  # Solid Waste Collection (some combined utilities)
    "562211",  # Hazardous Waste Treatment and Disposal
    "924110",  # Administration of Air and Water Resource
    "926110",  # Administration of General Economic Programs
}

# ENF_CONCLUSION_ACTION_CODE values from ICIS-FE&C
# CDC = Consent Decree, ACO = Admin Compliance Order, APO = Admin Penalty Order,
# PSA = Proposed Settlement Agreement, NOD = Notice of Determination,
# CAO = Consent Agreement/Final Order, UAO = Unilateral Admin Order,
# FFO = Federal Facility Compliance Agreement
CONSENT_DECREE_CODES = {"CDC", "CAO"}  # Strict consent decree types

BROADER_ENFORCEMENT_CODES = {
    "ACO",  # Administrative Compliance Order
    "APO",  # Administrative Penalty Order
    "PSA",  # Proposed Settlement Agreement
    "UAO",  # Unilateral Administrative Order
    "FFO",  # Federal Facility Compliance Agreement
    "NOD",  # Notice of Determination
}

# Text-based keywords for name/description matching
CONSENT_DECREE_KEYWORDS_TEXT = {
    "consent decree", "consent order", "consent agreement",
    "settlement", "judicial consent",
}

BROADER_ENFORCEMENT_KEYWORDS_TEXT = {
    "administrative order", "compliance order", "final order",
    "penalty order", "compliance agreement",
}

PIPE_INFRASTRUCTURE_KEYWORDS = [
    # Overflow types
    "sso", "cso", "sanitary sewer overflow", "combined sewer overflow",
    # Collection/conveyance systems
    "collection system", "sewer system", "sewer", "pipe", "pipeline",
    "conveyance", "interceptor", "trunk line",
    # Condition indicators
    "overflow", "inflow", "infiltration", "i/i", "i&i",
    # Management terms
    "capacity", "management", "operation and maintenance",
    # Facility types — full names
    "wastewater", "treatment plant", "water reclamation",
    "water recycling", "water pollution control", "sewage",
    # Facility abbreviations (common in EPA data)
    "wwtp", "wwtf", "wpcf", "wpaf", "awtf", "potw", "stp",
    "wwf",  # wastewater facility
    # Spaced abbreviations (EPA sometimes uses "W P C F" instead of "WPCF")
    "w p c f", "w p a f", "w w t f", "w w t p",
    # Municipal utility terms
    "mun util", "metro plant",
    # Storm sewer
    "ms4", "storm sewer", "stormwater",
    # Other municipal water infrastructure
    "lagoon", "sanitary district", "sewer authority",
    "sewer district", "water district", "public works",
    "ww utility", "ww treatment",
]

# Keywords that indicate a non-municipal (private/commercial) facility.
# If the facility name matches these, it is excluded from pipe infrastructure
# even if it contains a PIPE_INFRASTRUCTURE_KEYWORDS match, because IPI
# only services municipal underground water infrastructure.
NON_MUNICIPAL_KEYWORDS = [
    # Private companies / industrial
    " corp", " inc", " llc", " ltd", " co.", " company",
    "foods", "refinery", "chemical", "manufacturing", "mining", " mine ",
    "steel", "paper", " mill ", "packing", "processing", "petroleum",
    "energy", "power plant", "electric", " gas ", " oil ",
    "pharmaceutical", "textile", "brewery", "distill",
    "tannery", "rendering", "slaughter", "meat", "dairy",
    "sugar", "ethanol", "lumber", "aggregate", "quarry",
    "cement", "concrete", "asphalt", "plastic", "rubber",
    # Camps, resorts, hospitality
    "camp ", "campground", "resort", "lodge", "hotel", "motel",
    "conference center",
    # Small private systems (subdivisions, mobile homes, HOAs)
    "subdivision", "estates wwtp", "mobile home", " mhp ", "mhc,",
    "property owners", "homeowners", "trailer park", "rv park",
    # Commercial / other non-municipal
    "marina", " farm ", "ranch ", "shopping center", "plaza wwtp",
    "egg farm", "truck plaza",
]

# State FIPS codes for Census API lookups
STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12",
    "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
    "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
    "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
    "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
    "OH": "39", "OK": "40", "OR": "41", "PA": "42", "RI": "44",
    "SC": "45", "SD": "46", "TN": "47", "TX": "48", "UT": "49",
    "VT": "50", "VA": "51", "WA": "53", "WV": "54", "WI": "55",
    "WY": "56", "AS": "60", "GU": "66", "MP": "69", "PR": "72",
    "VI": "78",
}

# BigQuery schema — must match etl.py and seed_data.py
BQ_SCHEMA = [
    ("case_number", "STRING"),
    ("registry_id", "STRING"),
    ("fips_code", "STRING"),
    ("facility_name", "STRING"),
    ("city", "STRING"),
    ("state", "STRING"),
    ("zip_code", "STRING"),
    ("county", "STRING"),
    ("consent_decree_date", "DATE"),
    ("compliance_end_date", "DATE"),
    ("lead_agency", "STRING"),
    ("enforcement_level", "STRING"),
    ("action_type", "STRING"),
    ("violation_type", "STRING"),
    ("penalty_amount", "FLOAT64"),
    ("statute", "STRING"),
    ("pipe_infrastructure_flag", "BOOL"),
    ("population", "INT64"),
    ("days_to_deadline", "INT64"),
    ("urgency_tier", "STRING"),
    ("case_status", "STRING"),
    ("latitude", "FLOAT64"),
    ("longitude", "FLOAT64"),
    ("last_updated", "TIMESTAMP"),
]

# Concurrent Census lookups
MAX_CENSUS_WORKERS = 10

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("ipi_bulk_etl")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    log_dir = os.path.join(os.path.dirname(__file__), "logs")
    os.makedirs(log_dir, exist_ok=True)
    fh = RotatingFileHandler(
        os.path.join(log_dir, "etl_bulk.log"),
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


log = setup_logging()


# ---------------------------------------------------------------------------
# Download & extraction helpers
# ---------------------------------------------------------------------------

def download_zip(url: str, label: str) -> bytes:
    """Download a ZIP file from a URL. Returns raw bytes."""
    log.info("Downloading %s from %s ...", label, url)
    resp = requests.get(url, stream=True, timeout=600)
    resp.raise_for_status()
    total_mb = len(resp.content) / (1024 * 1024)
    log.info("  Downloaded %.1f MB", total_mb)
    return resp.content


def read_csv_from_zip(zip_bytes: bytes, csv_name: str) -> list[dict]:
    """Read a specific CSV file from a ZIP archive into a list of dicts.
    Handles NUL bytes, BOM, encoding issues common in EPA data files."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()
        # Find file (case-insensitive, may be in subdirectory)
        match = None
        for n in names:
            if n.lower().endswith(csv_name.lower()):
                match = n
                break
        if not match:
            log.warning("CSV '%s' not found in ZIP. Available: %s", csv_name, names[:10])
            return []

        log.info("  Reading %s ...", match)
        with zf.open(match) as f:
            raw = f.read()
            # Strip NUL bytes that EPA files sometimes contain
            raw = raw.replace(b"\x00", b"")
            text = raw.decode("utf-8-sig", errors="replace")
            reader = csv.DictReader(io.StringIO(text))
            rows = list(reader)
            log.info("  Read %d rows from %s", len(rows), csv_name)
            return rows


# ---------------------------------------------------------------------------
# ICIS-FE&C processing (case_downloads.zip)
# ---------------------------------------------------------------------------

def process_icis_fec(
    zip_bytes: bytes,
    cutoff_date: date,
    years_back: int,
) -> list[dict]:
    """Process ICIS-FE&C bulk data to extract CWA/SDWA consent decrees.

    Joins:
        CASE_ENFORCEMENTS  (main case table)
      + CASE_LAW_SECTIONS  (filter CWA/SDWA)
      + CASE_FACILITIES    (facility details)
      + CASE_ENFORCEMENT_CONCLUSIONS  (consent decree records)
      + CASE_MILESTONES    (key dates)

    Returns list of transformed records ready for BigQuery.
    """
    log.info("")
    log.info("=" * 60)
    log.info("PROCESSING ICIS-FE&C (Federal Enforcement Cases)")
    log.info("=" * 60)

    # Read the CSVs we need
    cases = read_csv_from_zip(zip_bytes, "CASE_ENFORCEMENTS.csv")
    law_sections = read_csv_from_zip(zip_bytes, "CASE_LAW_SECTIONS.csv")
    facilities = read_csv_from_zip(zip_bytes, "CASE_FACILITIES.csv")
    conclusions = read_csv_from_zip(zip_bytes, "CASE_ENFORCEMENT_CONCLUSIONS.csv")
    milestones = read_csv_from_zip(zip_bytes, "CASE_MILESTONES.csv")
    penalties = read_csv_from_zip(zip_bytes, "CASE_PENALTIES.csv")

    if not cases:
        log.error("No data in CASE_ENFORCEMENTS.csv — aborting FE&C processing")
        return []

    log.info("")
    log.info("Raw counts: %d cases, %d law sections, %d facilities, "
             "%d conclusions, %d milestones, %d penalties",
             len(cases), len(law_sections), len(facilities),
             len(conclusions), len(milestones), len(penalties))

    # Step 1: Find CWA / SDWA cases
    cwa_sdwa_activity_ids = set()
    statute_by_activity = {}
    for ls in law_sections:
        statute = (ls.get("STATUTE_CODE") or ls.get("STATUTE") or "").upper().strip()
        activity_id = (ls.get("ACTIVITY_ID") or "").strip()
        if statute in ("CWA", "SDWA") and activity_id:
            cwa_sdwa_activity_ids.add(activity_id)
            statute_by_activity[activity_id] = statute

    log.info("CWA/SDWA cases identified: %d", len(cwa_sdwa_activity_ids))

    # Step 2: Index cases by ACTIVITY_ID — filter to CWA/SDWA + ACTIVE only
    # ACTIVITY_STATUS_CODE: CLS=Closed, FOI/AOI/FOE/CSU/CNC/etc.=Active
    CLOSED_STATUSES = {"CLS"}
    case_map = {}
    status_counts = {}
    for c in cases:
        aid = (c.get("ACTIVITY_ID") or "").strip()
        if aid not in cwa_sdwa_activity_ids:
            continue
        status = (c.get("ACTIVITY_STATUS_CODE") or "").strip().upper()
        status_counts[status] = status_counts.get(status, 0) + 1
        if status in CLOSED_STATUSES:
            continue  # Skip closed/completed cases
        case_map[aid] = c

    log.info("CWA/SDWA case status breakdown:")
    for s, cnt in sorted(status_counts.items(), key=lambda x: -x[1]):
        marker = " [FILTERED OUT]" if s in CLOSED_STATUSES else ""
        log.info("  %s: %d%s", s, cnt, marker)
    log.info("CWA/SDWA ACTIVE cases: %d (filtered from %d total)",
             len(case_map), sum(status_counts.values()))

    # Step 3: Filter for municipal water/wastewater facilities
    # Build facility lookup by ACTIVITY_ID
    fac_by_activity = {}
    for f in facilities:
        aid = (f.get("ACTIVITY_ID") or "").strip()
        if aid not in case_map:
            continue
        # Check SIC/NAICS codes if available
        sic = (f.get("PRIMARY_SIC_CODE") or "").strip()
        naics = (f.get("PRIMARY_NAICS_CODE") or "").strip()

        # Accept: municipal SIC/NAICS, or facility name suggests municipal water/wastewater
        fac_name = (f.get("FACILITY_NAME") or "").lower()
        is_municipal = (
            sic in MUNICIPAL_SIC_CODES
            or naics in MUNICIPAL_NAICS_CODES
            or any(kw in fac_name for kw in [
                "water", "sewer", "wastewater", "wwtp", "potw",
                "sanitary", "utility", "utilities", "metropolitan",
                "treatment", "reclamation", "stormwater", "drainage",
                "aqueduct", "waterworks", "msd", "mwrd", "wrd",
                "water authority", "sewer authority",
                "water district", "sewer district",
                "water department", "public works",
            ])
        )
        if is_municipal:
            if aid not in fac_by_activity:
                fac_by_activity[aid] = []
            fac_by_activity[aid].append(f)

    log.info("Municipal water/wastewater CWA/SDWA cases: %d", len(fac_by_activity))

    # Step 4: Find consent decree conclusions
    conclusion_by_activity = {}
    code_stats = {}
    for conc in conclusions:
        aid = (conc.get("ACTIVITY_ID") or "").strip()
        if aid not in fac_by_activity:
            continue

        action_code = (conc.get("ENF_CONCLUSION_ACTION_CODE") or "").strip().upper()
        conc_name = (conc.get("ENF_CONCLUSION_NAME") or "").lower()

        # Match by code OR by name text
        is_consent_decree = (
            action_code in CONSENT_DECREE_CODES
            or any(kw in conc_name for kw in CONSENT_DECREE_KEYWORDS_TEXT)
        )
        is_broader = (
            action_code in BROADER_ENFORCEMENT_CODES
            or any(kw in conc_name for kw in BROADER_ENFORCEMENT_KEYWORDS_TEXT)
        )

        if is_consent_decree or is_broader:
            if aid not in conclusion_by_activity:
                conclusion_by_activity[aid] = []
            conclusion_by_activity[aid].append(conc)
            code_stats[action_code] = code_stats.get(action_code, 0) + 1

    log.info("Conclusion code breakdown:")
    for code, cnt in sorted(code_stats.items(), key=lambda x: -x[1]):
        log.info("  %s: %d", code, cnt)

    log.info("Cases with consent decree / enforcement conclusions: %d",
             len(conclusion_by_activity))

    # Step 5: Build milestone lookup (entry dates, compliance dates)
    milestone_map = {}  # activity_id -> list of milestones
    for ms in milestones:
        aid = (ms.get("ACTIVITY_ID") or "").strip()
        if aid in conclusion_by_activity:
            if aid not in milestone_map:
                milestone_map[aid] = []
            milestone_map[aid].append(ms)

    # Step 6: Build penalty lookup
    penalty_map = {}
    for p in penalties:
        aid = (p.get("ACTIVITY_ID") or "").strip()
        if aid in conclusion_by_activity:
            penalty_map[aid] = p

    # Step 7: Transform into output records
    records = []
    stats = {
        "total_conclusions": 0,
        "date_filtered": 0,
        "pipe_flagged": 0,
    }

    for activity_id, conc_list in conclusion_by_activity.items():
        case = case_map.get(activity_id, {})
        fac_list = fac_by_activity.get(activity_id, [{}])
        fac = fac_list[0]  # Primary facility
        case_milestones = milestone_map.get(activity_id, [])
        case_penalty = penalty_map.get(activity_id, {})

        for conc in conc_list:
            stats["total_conclusions"] += 1

            # Determine consent decree date
            entered_date = (
                conc.get("SETTLEMENT_ENTERED_DATE")
                or conc.get("SETTLEMENT_LODGED_DATE")
                or ""
            ).strip()

            # Also check milestones for "Consent Decree Entry" date
            if not entered_date:
                for ms in case_milestones:
                    desc = (ms.get("SUB_ACTIVITY_TYPE_DESC") or "").lower()
                    if "entry" in desc or "lodged" in desc or "consent" in desc:
                        entered_date = ms.get("ACTUAL_DATE", "")
                        if entered_date:
                            break

            cd_date = _parse_date(entered_date)

            # Date range filter
            if cd_date:
                try:
                    cd_dt = datetime.strptime(cd_date, "%Y-%m-%d").date()
                    if cd_dt < cutoff_date:
                        stats["date_filtered"] += 1
                        continue
                except ValueError:
                    pass

            # Compliance end date — look in milestones
            compliance_end = _find_compliance_end(case_milestones)

            # Penalty amount
            fed_penalty = _parse_float(
                conc.get("FED_PENALTY_ASSESSED_AMT")
                or case_penalty.get("FED_PENALTY")
                or case.get("TOTAL_PENALTY_ASSESSED_AMT")
                or "0"
            )
            state_penalty = _parse_float(
                conc.get("STATE_LOCAL_PENALTY_AMT")
                or case_penalty.get("ST_LCL_PENALTY")
                or "0"
            )
            total_penalty = fed_penalty + state_penalty

            # Lead agency / enforcement level
            lead = (case.get("LEAD") or "").strip().upper()
            if lead == "E" or lead == "EPA":
                lead_agency = "EPA"
                enforcement_level = "Federal"
            elif lead == "S" or lead == "STATE":
                lead_agency = "State"
                enforcement_level = "State"
            elif lead in ("J", "JOINT"):
                lead_agency = "Joint (EPA/State)"
                enforcement_level = "Federal"
            else:
                # Infer from case name or defaults
                case_name = (case.get("CASE_NAME") or "").lower()
                if "epa" in case_name or "united states" in case_name or "doj" in case_name:
                    lead_agency = "EPA"
                    enforcement_level = "Federal"
                else:
                    lead_agency = lead or "Unknown"
                    enforcement_level = "Unknown"

            # Action type — map code to human-readable description
            _ACTION_CODE_MAP = {
                "CDC": "Consent Decree",
                "CAO": "Consent Agreement/Final Order",
                "ACO": "Administrative Compliance Order",
                "APO": "Administrative Penalty Order",
                "PSA": "Proposed Settlement Agreement",
                "UAO": "Unilateral Administrative Order",
                "FFO": "Federal Facility Compliance Agreement",
                "NOD": "Notice of Determination",
            }
            raw_code = (conc.get("ENF_CONCLUSION_ACTION_CODE") or "").strip().upper()
            action_desc = _ACTION_CODE_MAP.get(raw_code, conc.get("ENF_CONCLUSION_NAME") or raw_code)

            # Violation type from statute
            statute = statute_by_activity.get(activity_id, "CWA")

            # Pipe infrastructure flag
            pipe_flag = _detect_pipe_infrastructure(case, conc, fac)
            if pipe_flag:
                stats["pipe_flagged"] += 1

            # Case number — prefer real docket number
            case_number = (
                case.get("DOJ_DOCKET_NMBR")
                or case.get("CASE_NUMBER")
                or conc.get("ENF_CONCLUSION_ID")
                or f"ICIS-{activity_id}"
            ).strip()

            # Facility location
            state_code = (
                fac.get("STATE_CODE")
                or case.get("STATE_CODE")
                or ""
            ).strip().upper()

            days = _compute_days(compliance_end)
            urgency = _compute_urgency(days)

            rec = {
                "case_number": case_number,
                "registry_id": (fac.get("REGISTRY_ID") or "").strip(),
                "fips_code": "",  # Will be enriched from Census or ECHO
                "facility_name": (fac.get("FACILITY_NAME") or case.get("CASE_NAME") or "").strip(),
                "city": (fac.get("CITY") or "").strip(),
                "state": state_code,
                "zip_code": (fac.get("ZIP") or "").strip(),
                "county": "",  # Census enrichment
                "consent_decree_date": cd_date,
                "compliance_end_date": compliance_end,
                "lead_agency": lead_agency,
                "enforcement_level": enforcement_level,
                "action_type": action_desc,
                "violation_type": statute,
                "penalty_amount": total_penalty,
                "statute": statute,
                "pipe_infrastructure_flag": pipe_flag,
                "population": None,
                "days_to_deadline": days,
                "urgency_tier": urgency,
                "case_status": _map_case_status(
                    (case.get("ACTIVITY_STATUS_CODE") or "").strip().upper()
                ),
                "latitude": _parse_float(fac.get("LATITUDE") or "", default=None),
                "longitude": _parse_float(fac.get("LONGITUDE") or "", default=None),
                "last_updated": datetime.utcnow().isoformat(),
            }
            records.append(rec)

    log.info("")
    log.info("ICIS-FE&C results:")
    log.info("  Total enforcement conclusions processed: %d", stats["total_conclusions"])
    log.info("  Filtered by date range: %d", stats["date_filtered"])
    log.info("  Pipe infrastructure flagged: %d", stats["pipe_flagged"])
    log.info("  Records produced: %d", len(records))

    return records


# ---------------------------------------------------------------------------
# NPDES processing (npdes_downloads.zip) — state-level CWA enforcement
# ---------------------------------------------------------------------------

def process_npdes(
    zip_bytes: bytes,
    cutoff_date: date,
) -> list[dict]:
    """Process NPDES bulk data for state and federal CWA enforcement actions.

    Joins:
        NPDES_FORMAL_ENFORCEMENT_ACTIONS  (enforcement actions by NPDES_ID)
      + ICIS_FACILITIES                   (facility name, city, state, lat/lon)
      + NPDES_SICS                        (SIC codes for municipal filtering)

    This dataset includes BOTH state and federal enforcement under CWA/NPDES.
    The AGENCY field distinguishes state from EPA-led actions.
    """
    log.info("")
    log.info("=" * 60)
    log.info("PROCESSING NPDES (CWA Enforcement Actions)")
    log.info("=" * 60)

    formal_actions = read_csv_from_zip(zip_bytes, "NPDES_FORMAL_ENFORCEMENT_ACTIONS.csv")
    facilities = read_csv_from_zip(zip_bytes, "ICIS_FACILITIES.csv")
    sics = read_csv_from_zip(zip_bytes, "NPDES_SICS.csv")

    if not formal_actions:
        log.error("No data in NPDES_FORMAL_ENFORCEMENT_ACTIONS.csv")
        return []

    log.info("Total NPDES formal enforcement actions: %d", len(formal_actions))
    log.info("Total ICIS facilities: %d", len(facilities))
    log.info("Total SIC records: %d", len(sics))

    # Build facility lookup by NPDES_ID
    fac_by_npdes = {}
    for f in facilities:
        npdes_id = (f.get("NPDES_ID") or "").strip()
        if npdes_id:
            fac_by_npdes[npdes_id] = f

    # Build SIC lookup by NPDES_ID (primary SIC only)
    sic_by_npdes = {}
    for s in sics:
        npdes_id = (s.get("NPDES_ID") or "").strip()
        is_primary = (s.get("PRIMARY_INDICATOR_FLAG") or "").upper() == "Y"
        sic_code = (s.get("SIC_CODE") or "").strip()
        if npdes_id and sic_code:
            if is_primary or npdes_id not in sic_by_npdes:
                sic_by_npdes[npdes_id] = sic_code

    log.info("Facility lookup: %d | SIC lookup: %d", len(fac_by_npdes), len(sic_by_npdes))

    # Identify municipal NPDES_IDs (by SIC or facility name)
    municipal_npdes = set()
    for npdes_id, sic in sic_by_npdes.items():
        if sic in MUNICIPAL_SIC_CODES:
            municipal_npdes.add(npdes_id)

    for npdes_id, fac in fac_by_npdes.items():
        if npdes_id in municipal_npdes:
            continue
        fac_name = (fac.get("FACILITY_NAME") or "").lower()
        if any(kw in fac_name for kw in [
            "water", "sewer", "wastewater", "wwtp", "potw",
            "sanitary", "utility", "utilities", "treatment",
            "reclamation", "aqueduct", "waterworks", "msd", "mwrd",
            "water authority", "sewer authority", "water district",
            "sewer district", "metropolitan", "public works",
            "stormwater", "drainage",
        ]):
            municipal_npdes.add(npdes_id)

    log.info("Municipal NPDES facilities identified: %d", len(municipal_npdes))

    # Filter enforcement actions
    records = []
    stats = {
        "municipal_matched": 0,
        "significant_matched": 0,
        "date_filtered": 0,
        "pipe_flagged": 0,
        "federal": 0,
        "state": 0,
    }

    for action in formal_actions:
        npdes_id = (action.get("NPDES_ID") or "").strip()
        if npdes_id not in municipal_npdes:
            continue

        stats["municipal_matched"] += 1

        # Check if it's a significant enforcement action type
        enf_type_desc = (action.get("ENF_TYPE_DESC") or "").lower()
        enf_type_code = (action.get("ENF_TYPE_CODE") or "").upper()

        is_significant = any(kw in enf_type_desc for kw in [
            "consent", "decree", "order", "settlement", "judicial",
            "compliance schedule", "formal enforcement", "contempt",
            "penalty", "assessment",
        ]) or enf_type_code in {
            "CDC", "CAO", "ACO", "APO", "NOD", "UAO", "FFO",  # Federal codes
            "CSO", "SOC", "AO", "CO", "CFO",  # State codes
            "CIC", "SCO", "NOV", "AOC",
        }
        if not is_significant:
            continue

        # Date filter
        entered_date = (action.get("SETTLEMENT_ENTERED_DATE") or "").strip()
        cd_date = _parse_date(entered_date)
        if cd_date:
            try:
                cd_dt = datetime.strptime(cd_date, "%Y-%m-%d").date()
                if cd_dt < cutoff_date:
                    stats["date_filtered"] += 1
                    continue
            except ValueError:
                pass

        stats["significant_matched"] += 1

        # Get facility details
        fac = fac_by_npdes.get(npdes_id, {})

        # Lead agency / enforcement level
        agency = (action.get("AGENCY") or "").strip().upper()
        if agency in ("E", "EPA"):
            lead_agency = "EPA"
            enforcement_level = "Federal"
            stats["federal"] += 1
        elif agency in ("S", "STATE"):
            lead_agency = "State"
            enforcement_level = "State"
            stats["state"] += 1
        else:
            lead_agency = agency or "Unknown"
            enforcement_level = "State" if agency else "Unknown"
            if agency:
                stats["state"] += 1

        # Penalties
        fed_penalty = _parse_float(action.get("FED_PENALTY_ASSESSED_AMT") or "0")
        state_penalty = _parse_float(action.get("STATE_LOCAL_PENALTY_AMT") or "0")
        total_penalty = fed_penalty + state_penalty

        # Pipe flag
        fac_name = (fac.get("FACILITY_NAME") or "").lower()
        pipe_flag = any(
            kw in fac_name or kw in enf_type_desc
            for kw in PIPE_INFRASTRUCTURE_KEYWORDS
        )
        if pipe_flag:
            stats["pipe_flagged"] += 1

        state_code = (fac.get("STATE_CODE") or "").strip().upper()

        case_number = (
            action.get("ENF_IDENTIFIER")
            or action.get("ACTIVITY_ID")
            or f"NPDES-{npdes_id}"
        ).strip()

        days = None  # NPDES enforcement actions don't have compliance end dates in this CSV
        rec = {
            "case_number": case_number,
            "registry_id": (fac.get("FACILITY_UIN") or "").strip(),
            "fips_code": "",
            "facility_name": (fac.get("FACILITY_NAME") or "").strip(),
            "city": (fac.get("CITY") or "").strip(),
            "state": state_code,
            "zip_code": (fac.get("ZIP") or "").strip(),
            "county": "",
            "consent_decree_date": cd_date,
            "compliance_end_date": None,
            "lead_agency": lead_agency,
            "enforcement_level": enforcement_level,
            "action_type": (action.get("ENF_TYPE_DESC") or enf_type_code).strip(),
            "violation_type": "CWA",
            "penalty_amount": total_penalty,
            "statute": "CWA",
            "pipe_infrastructure_flag": pipe_flag,
            "population": None,
            "days_to_deadline": days,
            "urgency_tier": _compute_urgency(days),
            "case_status": _npdes_case_status(cd_date),
            "latitude": _parse_float(fac.get("GEOCODE_LATITUDE") or "", default=None),
            "longitude": _parse_float(fac.get("GEOCODE_LONGITUDE") or "", default=None),
            "last_updated": datetime.utcnow().isoformat(),
        }
        records.append(rec)

    log.info("")
    log.info("NPDES results:")
    log.info("  Municipal enforcement actions: %d", stats["municipal_matched"])
    log.info("  Significant actions (post-filter): %d", stats["significant_matched"])
    log.info("  Filtered by date range: %d", stats["date_filtered"])
    log.info("  Federal: %d | State: %d", stats["federal"], stats["state"])
    log.info("  Pipe infrastructure flagged: %d", stats["pipe_flagged"])
    log.info("  Records produced: %d", len(records))

    return records


# ---------------------------------------------------------------------------
# Census population enrichment
# ---------------------------------------------------------------------------

_thread_local = threading.local()


def _get_thread_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update({"User-Agent": "IPIPipeBulkETL/3.0"})
        _thread_local.session = s
    return _thread_local.session


def enrich_census_population(records: list[dict]) -> int:
    """Enrich records with Census population data using concurrent lookups.
    Returns count of successfully enriched records."""
    census_key = os.getenv("CENSUS_API_KEY")
    if not census_key:
        log.warning("CENSUS_API_KEY not set — skipping population enrichment")
        return 0

    # Group by state for efficient lookups
    state_zips = {}
    for rec in records:
        st = rec.get("state", "")
        zc = rec.get("zip_code", "")
        if st and zc:
            if st not in state_zips:
                state_zips[st] = set()
            state_zips[st].add(zc[:5])

    log.info("Enriching Census population for %d states ...", len(state_zips))

    # Build ZIP -> population cache
    pop_cache = {}
    enriched_count = 0

    def fetch_state_pop(state_code: str) -> dict:
        """Fetch population for all ZCTAs in a state."""
        fips = STATE_FIPS.get(state_code)
        if not fips:
            return {}

        sess = _get_thread_session()
        url = f"{CENSUS_ACS_BASE}/2022/acs/acs5"
        params = {
            "get": "B01003_001E,NAME",
            "for": "zip code tabulation area:*",
            "in": f"state:{fips}",
            "key": census_key,
        }
        try:
            resp = sess.get(url, params=params, timeout=30)
            if resp.status_code != 200:
                return {}
            data = resp.json()
            if not data or len(data) < 2:
                return {}
            result = {}
            for row in data[1:]:
                pop_val = row[0]
                zcta = row[-1]
                try:
                    result[zcta] = int(pop_val)
                except (ValueError, TypeError):
                    pass
            return result
        except Exception:
            return {}

    # Concurrent Census lookups by state
    with ThreadPoolExecutor(max_workers=MAX_CENSUS_WORKERS) as executor:
        futures = {
            executor.submit(fetch_state_pop, st): st
            for st in state_zips
        }
        for future in as_completed(futures):
            st = futures[future]
            try:
                result = future.result()
                pop_cache.update(result)
                if result:
                    log.debug("  Census: %s → %d ZCTAs", st, len(result))
            except Exception as e:
                log.debug("  Census error for %s: %s", st, e)

    # Apply to records
    for rec in records:
        zc = (rec.get("zip_code") or "")[:5]
        if zc and zc in pop_cache:
            rec["population"] = pop_cache[zc]
            enriched_count += 1

    log.info("Census enrichment: %d / %d records enriched", enriched_count, len(records))
    return enriched_count


# ---------------------------------------------------------------------------
# Geocoding — resolve lat/lon from REGISTRY_ID via ECHO API or ZIP centroid
# ---------------------------------------------------------------------------

# ZIP code centroid URL (Census ZCTA gazetteer — ZIP archive, ~600 KB)
ZCTA_GAZETTEER_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_zcta_national.zip"


def geocode_records(records: list[dict]) -> int:
    """Add lat/lon to records missing coordinates.
    Strategy:
      1. Batch lookup REGISTRY_IDs via ECHO facility API
      2. Fall back to ZIP code centroids from Census ZCTA gazetteer
    Returns count of records geocoded.
    """
    # Find records needing geocoding
    needs_geo = [r for r in records if r.get("latitude") is None or r.get("longitude") is None]
    if not needs_geo:
        log.info("All records already have coordinates")
        return 0

    log.info("Geocoding %d records missing coordinates ...", len(needs_geo))
    geocoded = 0

    # Strategy 1: ZIP code centroids (fast, reliable — covers ~95% of records)
    still_needs = needs_geo
    if still_needs:
        log.info("  Falling back to ZIP centroids for %d remaining records ...", len(still_needs))
        zip_coords = _load_zip_centroids()
        log.info("  Loaded %d ZIP centroids", len(zip_coords))
        for r in still_needs:
            zc = (r.get("zip_code") or "")[:5]
            if zc and zc in zip_coords:
                lat, lon = zip_coords[zc]
                r["latitude"] = lat
                r["longitude"] = lon
                geocoded += 1

    log.info("Geocoding complete: %d / %d records resolved", geocoded, len(needs_geo))
    return geocoded


def _fetch_echo_coords(registry_ids: set) -> dict:
    """Batch lookup facility coordinates from ECHO API by REGISTRY_ID.
    Returns {registry_id: (lat, lon)} dict."""
    if not registry_ids:
        return {}

    coords = {}
    batch = list(registry_ids)

    # ECHO API allows up to ~100 registry IDs per request
    BATCH_SIZE = 80
    for i in range(0, len(batch), BATCH_SIZE):
        chunk = batch[i:i + BATCH_SIZE]
        ids_param = ",".join(chunk)

        try:
            url = "https://echodata.epa.gov/echo/echo_rest_services.get_facilities"
            params = {
                "output": "JSON",
                "p_registry_id": ids_param,
                "responseset": str(len(chunk)),
            }
            sess = _get_thread_session()
            resp = sess.get(url, params=params, timeout=60)
            if resp.status_code != 200:
                continue
            data = resp.json()

            # Navigate ECHO response structure
            results = data.get("Results", {})
            facilities = results.get("Facilities", [])
            for fac in facilities:
                rid = fac.get("RegistryId") or fac.get("FacRegistryID") or ""
                lat = fac.get("FacLat") or fac.get("Lat")
                lon = fac.get("FacLong") or fac.get("Long")
                if rid and lat and lon:
                    try:
                        lat_f = float(lat)
                        lon_f = float(lon)
                        if lat_f != 0 and lon_f != 0:
                            coords[rid] = (lat_f, lon_f)
                    except (ValueError, TypeError):
                        pass

            # Rate limit
            time.sleep(0.5)
        except Exception as e:
            log.debug("  ECHO batch lookup error: %s", e)
            continue

        if (i // BATCH_SIZE) % 10 == 0 and i > 0:
            log.info("    ECHO lookup progress: %d / %d IDs", i, len(batch))

    return coords


def _load_zip_centroids() -> dict:
    """Download Census ZCTA gazetteer (ZIP archive) and build {zip: (lat, lon)} lookup.
    2024 format: tab-delimited, cols: GEOID(0), ALAND(1), AWATER(2),
    ALAND_SQMI(3), AWATER_SQMI(4), INTPTLAT(5), INTPTLONG(6)"""
    try:
        sess = _get_thread_session()
        resp = sess.get(ZCTA_GAZETTEER_URL, timeout=120)
        if resp.status_code != 200:
            log.warning("  Failed to download ZCTA gazetteer: HTTP %d", resp.status_code)
            return {}

        coords = {}
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            # Find the .txt file inside
            txt_files = [n for n in zf.namelist() if n.endswith(".txt")]
            if not txt_files:
                log.warning("  No .txt file found in ZCTA gazetteer ZIP")
                return {}
            with zf.open(txt_files[0]) as f:
                text = f.read().decode("utf-8-sig", errors="replace")
                lines = text.strip().split("\n")
                for line in lines[1:]:  # Skip header
                    parts = line.split("\t")
                    if len(parts) >= 7:
                        zcta = parts[0].strip()
                        try:
                            lat = float(parts[5].strip())
                            lon = float(parts[6].strip())
                            if lat != 0 and lon != 0:
                                coords[zcta] = (lat, lon)
                        except (ValueError, IndexError):
                            pass
        return coords
    except Exception as e:
        log.warning("  Failed to load ZIP centroids: %s", e)
        return {}


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _parse_date(date_str: Optional[str]) -> Optional[str]:
    """Parse various date formats to YYYY-MM-DD."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y-%m-%dT%H:%M:%S", "%d-%b-%Y", "%d-%b-%y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _parse_float(val, default=0.0):
    """Safely parse a string to float."""
    if not val:
        return default
    if isinstance(val, (int, float)):
        return float(val)
    cleaned = str(val).replace("$", "").replace(",", "").strip()
    if not cleaned:
        return default
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return default


def _compute_days(end_date_str: Optional[str]) -> Optional[int]:
    """Days remaining to compliance deadline."""
    if not end_date_str:
        return None
    try:
        end_dt = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        return (end_dt - date.today()).days
    except ValueError:
        return None


def _compute_urgency(days: Optional[int]) -> str:
    """Urgency tier matching dashboard definitions."""
    if days is None:
        return "unknown"
    if days < 0:
        return "overdue"
    if days < 90:
        return "critical"
    if days <= 365:
        return "high"
    if days <= 730:
        return "medium"
    return "low"


def _npdes_case_status(consent_decree_date: Optional[str]) -> str:
    """Infer case status for NPDES records (no explicit status field).
    Uses recency: issued within last 5 years = Active, older = Likely Closed."""
    if not consent_decree_date:
        return "Unknown"
    try:
        cd_dt = datetime.strptime(consent_decree_date, "%Y-%m-%d").date()
        years_ago = (date.today() - cd_dt).days / 365.25
        if years_ago <= 5:
            return "Active"
        return "Likely Closed"
    except ValueError:
        return "Unknown"


def _map_case_status(status_code: str) -> str:
    """Map ICIS ACTIVITY_STATUS_CODE to human-readable status."""
    STATUS_MAP = {
        "FOI": "Active",           # Filed / Open / Initiated
        "AOI": "Active",           # Administrative Open/Initiated
        "FOE": "Active",           # Filed / Open / Entered
        "CSU": "Active",           # Case Supplemented
        "CNC": "Active",           # Concluded (but not closed)
        "DSP": "Active",           # Disposed
        "SPR": "Active",           # Superseded
        "CMA": "Active",           # Compliance Monitoring Active
        "CFL": "Active",           # Case Filed
        "CLS": "Closed",           # Closed
    }
    return STATUS_MAP.get(status_code, "Active" if status_code else "Unknown")


def _detect_pipe_infrastructure(case: dict, conclusion: dict, facility: dict) -> bool:
    """Check if this case involves municipal pipe/collection system infrastructure.

    Two-step filter:
      1. INCLUDE if any PIPE_INFRASTRUCTURE_KEYWORDS match the combined text
         (case name, enforcement conclusion, facility name, compliance action).
      2. EXCLUDE if the facility name matches NON_MUNICIPAL_KEYWORDS —
         these are private/industrial/commercial facilities that happen to
         have a WWTP but are not municipal underground pipe infrastructure.

    Data consistency rules (for future ETL runs & manual corrections):
      - Penalty amounts should reflect the *civil penalty* only, not total
        capital investment or supplemental environmental project costs.
      - Compliance deadlines should come from the court-ordered consent decree,
        not ICIS placeholder dates (where end_date == start_date).
      - When ICIS shows a 0-duration consent decree, treat it as a placeholder
        and look up the real deadline via deadline_lookup.py.
    """
    text = " ".join([
        case.get("CASE_NAME", ""),
        conclusion.get("ENF_CONCLUSION_NAME", ""),
        facility.get("FACILITY_NAME", ""),
        conclusion.get("COMP_ACTION_DESCRIPTION", ""),
    ]).lower()

    # Step 1: must match at least one pipe infrastructure keyword
    if not any(kw in text for kw in PIPE_INFRASTRUCTURE_KEYWORDS):
        return False

    # Step 2: exclude non-municipal facilities (unless clearly municipal)
    facility_name = facility.get("FACILITY_NAME", "").lower()
    municipal_overrides = [
        "city of", "town of", "village of", "county", "district",
        "authority", "municipal", "mud ", "mud no", "public works",
    ]
    if any(m in facility_name for m in municipal_overrides):
        return True  # clearly municipal, skip exclusion check

    if any(excl in facility_name for excl in NON_MUNICIPAL_KEYWORDS):
        return False

    return True


def _find_compliance_end(milestones: list[dict]) -> Optional[str]:
    """Find the latest compliance schedule end date from milestones."""
    latest = None
    for ms in milestones:
        desc = (ms.get("SUB_ACTIVITY_TYPE_DESC") or "").lower()
        if any(kw in desc for kw in [
            "compliance", "completion", "final", "termination", "end",
            "schedule", "deadline",
        ]):
            dt_str = ms.get("ACTUAL_DATE") or ms.get("PLANNED_DATE")
            dt = _parse_date(dt_str)
            if dt:
                try:
                    dt_obj = datetime.strptime(dt, "%Y-%m-%d").date()
                    if latest is None or dt_obj > latest:
                        latest = dt_obj
                except ValueError:
                    pass
    return latest.strftime("%Y-%m-%d") if latest else None


# ---------------------------------------------------------------------------
# De-duplication
# ---------------------------------------------------------------------------

def deduplicate_records(records: list[dict]) -> list[dict]:
    """Remove duplicate records, keeping the one with the most data.
    Also dedupes against same case from FE&C vs NPDES sources.
    """
    # Group by case_number
    by_case = {}
    for rec in records:
        cn = rec.get("case_number", "")
        if cn in by_case:
            existing = by_case[cn]
            # Keep the record with more data (penalty, dates, etc.)
            score_new = _record_completeness(rec)
            score_old = _record_completeness(existing)
            if score_new > score_old:
                by_case[cn] = rec
        else:
            by_case[cn] = rec

    # Also dedupe by facility+state+date (catch same case with different case numbers)
    final = {}
    for cn, rec in by_case.items():
        key = (
            (rec.get("facility_name") or "").lower().strip(),
            (rec.get("state") or "").strip(),
            (rec.get("consent_decree_date") or ""),
        )
        if key in final:
            existing = final[key]
            if _record_completeness(rec) > _record_completeness(existing):
                final[key] = rec
        else:
            final[key] = rec

    deduped = list(final.values())
    if len(records) != len(deduped):
        log.info("De-duplicated: %d → %d records", len(records), len(deduped))
    return deduped


def _record_completeness(rec: dict) -> int:
    """Score a record by completeness — higher is better."""
    score = 0
    if rec.get("case_number"):
        score += 1
    if rec.get("consent_decree_date"):
        score += 2
    if rec.get("compliance_end_date"):
        score += 2
    if rec.get("penalty_amount", 0) > 0:
        score += 2
    if rec.get("latitude") is not None:
        score += 1
    if rec.get("population") is not None:
        score += 1
    if rec.get("facility_name"):
        score += 1
    return score


# ---------------------------------------------------------------------------
# BigQuery load (reuses same MERGE/UPSERT pattern)
# ---------------------------------------------------------------------------

def load_to_bigquery(records: list[dict], dry_run: bool = False):
    """Create dataset/table if needed and MERGE records into BigQuery."""
    if dry_run:
        log.info("[DRY RUN] Skipping BigQuery load for %d records", len(records))
        return

    try:
        from google.cloud import bigquery
    except ImportError:
        log.error("google-cloud-bigquery not installed — cannot load to BQ")
        return

    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id:
        log.error("GCP_PROJECT_ID not set — cannot load to BigQuery")
        return

    client = bigquery.Client(project=project_id)
    dataset_id = f"{project_id}.ipi_intelligence"
    table_id = f"{dataset_id}.consent_decrees"

    # Create dataset if needed
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok=True)
    log.info("Dataset %s ready", dataset_id)

    # Create or update table schema
    schema = [bigquery.SchemaField(name, dtype) for name, dtype in BQ_SCHEMA]
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)

    existing_table = client.get_table(table_id)
    existing_names = {f.name for f in existing_table.schema}
    new_fields = [f for f in schema if f.name not in existing_names]
    if new_fields:
        updated_schema = list(existing_table.schema) + new_fields
        existing_table.schema = updated_schema
        client.update_table(existing_table, ["schema"])
        log.info("Updated table schema with %d new columns", len(new_fields))

    log.info("Table %s ready", table_id)

    # Load in batches via temp table MERGE
    BATCH_SIZE = 500
    total_merged = 0

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE

        temp_table_id = f"{dataset_id}._temp_bulk_{int(time.time())}_{batch_num}"
        temp_table = bigquery.Table(temp_table_id, schema=schema)
        client.create_table(temp_table, exists_ok=True)

        errors = client.insert_rows_json(temp_table_id, batch)
        if errors:
            log.error("BigQuery insert errors (batch %d): %s", batch_num, errors[:3])
            client.delete_table(temp_table_id, not_found_ok=True)
            continue

        merge_cols = [name for name, _ in BQ_SCHEMA if name != "case_number"]
        update_clause = ", ".join(f"T.{c} = S.{c}" for c in merge_cols)
        insert_cols = ", ".join(name for name, _ in BQ_SCHEMA)
        insert_vals = ", ".join(f"S.{name}" for name, _ in BQ_SCHEMA)

        merge_sql = f"""
        MERGE `{table_id}` T
        USING `{temp_table_id}` S
        ON T.case_number = S.case_number
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals})
        """

        query_job = client.query(merge_sql)
        query_job.result()
        rows_affected = query_job.num_dml_affected_rows or 0
        total_merged += rows_affected

        client.delete_table(temp_table_id, not_found_ok=True)
        log.info("  Batch %d/%d: %d rows merged", batch_num, total_batches, rows_affected)

    log.info("BigQuery load complete: %d total rows merged", total_merged)


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run_bulk_etl(dry_run: bool = False, years_back: int = MAX_YEARS_BACK, skip_npdes: bool = False):
    """Main ETL pipeline using ICIS bulk downloads."""
    cutoff_date = date(date.today().year - years_back, date.today().month, date.today().day)

    log.info("")
    log.info("=" * 60)
    log.info("IPI PIPE — BULK CONSENT DECREE ETL v3")
    log.info("=" * 60)
    log.info("Mode: %s", "DRY RUN" if dry_run else "LIVE")
    log.info("Date range: %s to present (%d years)", cutoff_date.strftime("%Y-%m-%d"), years_back)
    log.info("NPDES: %s", "SKIP" if skip_npdes else "INCLUDE")
    log.info("=" * 60)
    log.info("")

    all_records = []

    # --- Phase 1: ICIS-FE&C (Federal enforcement cases) ---
    try:
        fec_zip = download_zip(CASE_DOWNLOADS_URL, "ICIS-FE&C (case_downloads.zip)")
        fec_records = process_icis_fec(fec_zip, cutoff_date, years_back)
        all_records.extend(fec_records)
        del fec_zip  # Free memory
    except Exception as e:
        log.error("Failed to process ICIS-FE&C: %s", e)

    # --- Phase 2: NPDES (State + Federal CWA enforcement) ---
    if not skip_npdes:
        try:
            npdes_zip = download_zip(NPDES_DOWNLOADS_URL, "NPDES (npdes_downloads.zip)")
            npdes_records = process_npdes(npdes_zip, cutoff_date)
            all_records.extend(npdes_records)
            del npdes_zip
        except Exception as e:
            log.error("Failed to process NPDES: %s", e)

    if not all_records:
        log.warning("No records found from any source!")
        return

    # --- Phase 3: De-duplicate ---
    all_records = deduplicate_records(all_records)

    # --- Phase 4: Geocoding (resolve missing lat/lon) ---
    geocoded = geocode_records(all_records)
    log.info("Geocoded %d records", geocoded)

    # --- Phase 5: Census population enrichment ---
    enriched = enrich_census_population(all_records)
    log.info("Census enriched %d records", enriched)

    # --- Phase 6: Summary ---
    log.info("")
    log.info("=" * 60)
    log.info("FINAL RESULTS")
    log.info("=" * 60)
    log.info("  Total unique records: %d", len(all_records))

    # Count breakdowns
    federal = sum(1 for r in all_records if r.get("enforcement_level") == "Federal")
    state = sum(1 for r in all_records if r.get("enforcement_level") == "State")
    unknown = sum(1 for r in all_records if r.get("enforcement_level") not in ("Federal", "State"))
    log.info("  Federal: %d | State: %d | Unknown: %d", federal, state, unknown)

    states = set(r.get("state") for r in all_records if r.get("state"))
    log.info("  States & territories: %d (%s)", len(states), ", ".join(sorted(states)))

    total_penalty = sum(r.get("penalty_amount", 0) for r in all_records)
    log.info("  Total penalties: $%s", f"{total_penalty:,.2f}")

    pipe_count = sum(1 for r in all_records if r.get("pipe_infrastructure_flag"))
    log.info("  Pipe infrastructure flagged: %d", pipe_count)

    # Urgency breakdown
    urgency_counts = {}
    for r in all_records:
        t = r.get("urgency_tier", "unknown")
        urgency_counts[t] = urgency_counts.get(t, 0) + 1
    for tier in ["overdue", "critical", "high", "medium", "low", "unknown"]:
        log.info("  %s: %d", tier.upper(), urgency_counts.get(tier, 0))

    # Show sample
    log.info("")
    log.info("--- Sample Records (first 15) ---")
    for rec in sorted(all_records, key=lambda r: r.get("penalty_amount", 0), reverse=True)[:15]:
        pipe = " [PIPE]" if rec.get("pipe_infrastructure_flag") else ""
        penalty_str = f"${rec.get('penalty_amount', 0):,.0f}"
        log.info(
            "  %s | %s, %s | %s | %s | %s | %s%s",
            (rec.get("facility_name") or "Unknown")[:40],
            rec.get("city", ""),
            rec.get("state", ""),
            (rec.get("action_type") or "")[:25],
            rec.get("enforcement_level", ""),
            penalty_str,
            rec.get("urgency_tier", "unknown"),
            pipe,
        )

    log.info("=" * 60)

    # --- Phase 7: Load to BigQuery ---
    if all_records:
        load_to_bigquery(all_records, dry_run=dry_run)
    else:
        log.info("No records to load")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="IPI Consent Decree Bulk ETL v3 — ICIS bulk download approach",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Download and process data but do not write to BigQuery",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=MAX_YEARS_BACK,
        help=f"Number of years back to pull (max {MAX_YEARS_BACK}, default {MAX_YEARS_BACK})",
    )
    parser.add_argument(
        "--skip-npdes",
        action="store_true",
        help="Skip NPDES download (faster — only process ICIS-FE&C federal cases)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    years = min(args.years, MAX_YEARS_BACK)
    if years < 1:
        log.error("--years must be at least 1")
        sys.exit(1)

    try:
        run_bulk_etl(
            dry_run=args.dry_run,
            years_back=years,
            skip_npdes=args.skip_npdes,
        )
    except KeyboardInterrupt:
        log.info("ETL interrupted by user")
        sys.exit(1)
    except Exception as exc:
        log.exception("ETL failed: %s", exc)
        sys.exit(2)


if __name__ == "__main__":
    main()
