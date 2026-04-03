"""
IPI Pipe – Consent Decree ETL  (v2 — concurrent facility scan)

Pulls EPA ECHO enforcement data for municipal water/wastewater consent decrees,
enriches with Census population data, and loads into Google BigQuery.

v2 STRATEGY:
    Searches ECHO for municipal water/wastewater facilities with formal
    enforcement actions, then checks each facility's Detailed Facility Report
    (DFR) for consent decree actions using concurrent workers (15 threads).
    Runtime: ~2 min per state vs ~35 min in v1 (sequential).

Usage:
    python etl.py
    python etl.py --dry-run
    python etl.py --state TX
    python etl.py --dry-run --state TX
    python etl.py --years 10
    python etl.py --dry-run --state TX --years 5
"""

import argparse
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from logging.handlers import RotatingFileHandler
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ECHO_BASE = "https://echodata.epa.gov/echo"

# Facility search endpoints
ECHO_FACILITY_SEARCH = f"{ECHO_BASE}/echo_rest_services.get_facilities"
ECHO_FACILITY_PAGE = f"{ECHO_BASE}/echo_rest_services.get_qid"
ECHO_FORMAL_ACTIONS = f"{ECHO_BASE}/dfr_rest_services.get_formal_actions"
ECHO_CWA_SEARCH = f"{ECHO_BASE}/cwa_rest_services.get_facilities"
ECHO_CWA_PAGE = f"{ECHO_BASE}/cwa_rest_services.get_qid"

# Census API
CENSUS_ACS_BASE = "https://api.census.gov/data"

MAX_YEARS_BACK = 15

# Industry codes for municipal water/wastewater infrastructure
MUNICIPAL_SIC_CODES = ["4952", "4941"]
MUNICIPAL_NAICS_CODES = ["221310", "221320"]

# ---------------------------------------------------------------------------
# Enforcement action classification
# ---------------------------------------------------------------------------

CONSENT_DECREE_KEYWORDS = [
    "consent decree",
    "consent order",
    "judicial consent",
    "consent judgment",
]

BROAD_ENFORCEMENT_KEYWORDS = [
    "civil judicial",
    "judicial action",
    "judicial order",
    "judicial referral",
    "settlement",
    "decree",
    "consent",
    "doj referral",
    "section 309",
]

PIPE_INFRASTRUCTURE_KEYWORDS = [
    "sso",
    "cso",
    "sanitary sewer overflow",
    "combined sewer overflow",
    "collection system",
    "sewer system",
    "pipe",
    "pipeline",
    "conveyance",
    "interceptor",
    "trunk line",
    "overflow",
    "inflow",
    "infiltration",
    "i/i",
    "i&i",
    "capacity",
    "management",
    "operation and maintenance",
]

PAGE_SIZE = 100
REQUEST_TIMEOUT = 60
RETRY_LIMIT = 3
RETRY_BACKOFF = 2
# Max concurrent DFR calls — EPA blocks "robotic" queries above ~10 req/s.
# 5 workers with 0.3s sleep per call ≈ 6 req/s, completing TX in ~5 min.
MAX_WORKERS = 5

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

# All US state abbreviations for full-run iteration
ALL_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI",
    "WY",
    # US territories
    "PR", "VI", "GU", "AS", "MP",
]

# BigQuery schema
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


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging() -> logging.Logger:
    """Configure logging to console and rotating log file."""
    logger = logging.getLogger("ipi_etl")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # Rotating file handler
    log_dir = os.path.join(os.path.dirname(__file__), "logs")
    os.makedirs(log_dir, exist_ok=True)
    fh = RotatingFileHandler(
        os.path.join(log_dir, "etl.log"),
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


log = setup_logging()


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

# EPA ECHO blocks queries identified as "robotic" — use a browser-like
# User-Agent and per-thread sessions to stay under their rate limits.
_UA = (
    "Mozilla/5.0 (compatible; IPIPipeETL/2.0; "
    "+https://ipipipe.com) Python-requests"
)

# Main session for single-threaded operations (facility search, pagination)
session = requests.Session()
session.headers.update({"Accept": "application/json", "User-Agent": _UA})

# Thread-local storage for per-thread sessions used by concurrent DFR calls
_thread_local = threading.local()


def _get_thread_session() -> requests.Session:
    """Return a thread-local requests.Session. requests.Session is NOT
    thread-safe, so each worker thread gets its own."""
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update({"Accept": "application/json", "User-Agent": _UA})
        _thread_local.session = s
    return _thread_local.session


def api_get(
    url: str,
    params: dict,
    label: str = "",
    use_thread_session: bool = False,
) -> Optional[dict]:
    """Make a GET request with retry logic. Returns JSON or None on failure.

    Args:
        use_thread_session: If True, use a thread-local session (for
            concurrent DFR/Census calls). Otherwise use the global session.
    """
    sess = _get_thread_session() if use_thread_session else session
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            resp = sess.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            # Detect EPA robot-block responses
            err = data.get("Error", {})
            if isinstance(err, dict) and "robotic" in (
                err.get("ErrorMessage", "")
            ).lower():
                wait = 30 * attempt  # back off aggressively
                log.warning(
                    "%s robot-blocked — waiting %ds before retry %d/%d",
                    label, wait, attempt, RETRY_LIMIT,
                )
                time.sleep(wait)
                continue
            return data
        except requests.RequestException as exc:
            wait = RETRY_BACKOFF ** attempt
            log.warning(
                "%s attempt %d/%d failed: %s — retrying in %ds",
                label, attempt, RETRY_LIMIT, exc, wait,
            )
            time.sleep(wait)
    log.error("%s failed after %d attempts", label, RETRY_LIMIT)
    return None


# ---------------------------------------------------------------------------
# EXTRACT – ECHO Facility Search
# ---------------------------------------------------------------------------

def search_facilities_by_code(
    code: str,
    code_type: str,
    state: Optional[str] = None,
) -> tuple[Optional[str], int]:
    """Search ECHO for facilities by SIC or NAICS code with formal
    enforcement actions. Returns (QueryID, total_rows)."""
    params = {
        "output": "JSON",
        "p_fea": "Y",
        "rows": str(PAGE_SIZE),
    }
    if code_type == "sic":
        params["p_sic"] = code
    else:
        params["p_ncs"] = code
    if state:
        params["p_st"] = state.upper()

    data = api_get(ECHO_FACILITY_SEARCH, params, f"facility_search_{code}")
    if not data:
        return None, 0

    results = data.get("Results", {})
    if results.get("Error"):
        log.warning("ECHO error for %s %s: %s", code_type, code, results["Error"])
        return None, 0

    total = int(results.get("QueryRows", "0"))
    qid = results.get("QueryID")
    return qid, total


def search_cwa_facilities(
    state: Optional[str] = None,
) -> tuple[Optional[str], int]:
    """Search ECHO CWA endpoint for POTW facilities with enforcement
    actions. Returns (QueryID, total_rows)."""
    params = {
        "output": "JSON",
        "p_ptype": "POT",
        "p_fea": "Y",
        "rows": str(PAGE_SIZE),
    }
    if state:
        params["p_st"] = state.upper()

    data = api_get(ECHO_CWA_SEARCH, params, "cwa_facility_search")
    if not data:
        return None, 0

    results = data.get("Results", {})
    total = int(results.get("QueryRows", "0"))
    qid = results.get("QueryID")
    return qid, total


def fetch_facility_page(
    qid: str, page: int, endpoint: str = ECHO_FACILITY_PAGE,
) -> list[dict]:
    """Fetch one page of facility results using a QueryID."""
    params = {
        "output": "JSON",
        "qid": qid,
        "pageno": str(page),
        "pagesize": str(PAGE_SIZE),
    }
    data = api_get(endpoint, params, f"facility_page_{page}")
    if not data:
        return []
    return data.get("Results", {}).get("Facilities", [])


def fetch_all_facilities(
    qid: str, total_rows: int, endpoint: str = ECHO_FACILITY_PAGE,
) -> list[dict]:
    """Paginate through all facility results."""
    facilities = []
    total_pages = max(1, (total_rows + PAGE_SIZE - 1) // PAGE_SIZE)
    for page in range(1, total_pages + 1):
        log.info("    Fetching page %d/%d...", page, total_pages)
        batch = fetch_facility_page(qid, page, endpoint)
        if not batch:
            break
        facilities.extend(batch)
        if page < total_pages:
            time.sleep(0.5)
    return facilities


def normalize_facility(fac: dict) -> dict:
    """Normalize field names between all-media and CWA-specific endpoints."""
    return {
        "RegistryID": fac.get("RegistryID", ""),
        "FacName": fac.get("FacName") or fac.get("CWPName", ""),
        "FacCity": fac.get("FacCity") or fac.get("CWPCity", ""),
        "FacState": fac.get("FacState") or fac.get("CWPState", ""),
        "FacZip": fac.get("FacZip") or fac.get("CWPZip", ""),
        "FacCounty": fac.get("FacCounty") or fac.get("CWPCounty", ""),
        "FacLat": fac.get("FacLat", ""),
        "FacLong": fac.get("FacLong", ""),
        "FacFIPSCode": fac.get("FacFIPSCode", ""),
        "FacDerivedStctyFIPS": fac.get("FacDerivedStctyFIPS", ""),
        "FacSICCodes": fac.get("FacSICCodes") or fac.get("CWPSICCodes", ""),
        "FacNAICSCodes": fac.get("FacNAICSCodes") or fac.get("CWPNAICSCodes", ""),
        "SourceID": fac.get("SourceID", ""),
        "CWPPermitNmbr": fac.get("CWPPermitNmbr", ""),
        "MasterExternalPermitNmbr": fac.get("MasterExternalPermitNmbr", ""),
        "_raw": fac,
    }


def search_all_municipal_facilities(
    state: Optional[str] = None,
) -> list[dict]:
    """Search ECHO across SIC, NAICS, and CWA POTW permits.
    Returns deduplicated, normalized facility records."""
    all_facilities = []
    seen_ids = set()

    def _add_batch(batch: list[dict]):
        for fac in batch:
            norm = normalize_facility(fac)
            rid = norm["RegistryID"]
            if rid and rid not in seen_ids:
                seen_ids.add(rid)
                all_facilities.append(norm)
            elif not rid:
                all_facilities.append(norm)

    log.info(
        "  Searching ECHO for municipal facilities with enforcement actions%s...",
        f" in {state.upper()}" if state else "",
    )

    for sic in MUNICIPAL_SIC_CODES:
        qid, total = search_facilities_by_code(sic, "sic", state)
        if not qid or total == 0:
            log.info("    SIC %s: 0 facilities", sic)
            continue
        log.info("    SIC %s: %d facilities", sic, total)
        _add_batch(fetch_all_facilities(qid, total))

    for naics in MUNICIPAL_NAICS_CODES:
        qid, total = search_facilities_by_code(naics, "naics", state)
        if not qid or total == 0:
            log.info("    NAICS %s: 0 facilities", naics)
            continue
        log.info("    NAICS %s: %d new facilities", naics, total)
        _add_batch(fetch_all_facilities(qid, total))

    qid, total = search_cwa_facilities(state)
    if qid and total > 0:
        log.info("    CWA POTWs: %d facilities", total)
        _add_batch(fetch_all_facilities(qid, total, ECHO_CWA_PAGE))
    else:
        log.info("    CWA POTWs: 0 facilities")

    log.info("  Total unique facilities: %d", len(all_facilities))
    return all_facilities


# ---------------------------------------------------------------------------
# EXTRACT – DFR (Detailed Facility Report) for per-facility details
# ---------------------------------------------------------------------------

def fetch_formal_actions(source_id: str) -> tuple[list[dict], list[dict]]:
    """Fetch formal enforcement actions for a single facility via the DFR API.
    Returns (actions, program_dates)."""
    params = {"p_id": source_id, "output": "JSON"}
    data = api_get(
        ECHO_FORMAL_ACTIONS, params, f"formal_actions_{source_id}",
        use_thread_session=True,
    )
    if not data:
        return [], []
    fa = data.get("Results", {}).get("FormalActions", {})
    if not isinstance(fa, dict):
        return [], []
    actions = fa.get("Action", [])
    program_dates = fa.get("ProgramDates", [])
    if isinstance(actions, dict):
        actions = [actions]
    if isinstance(program_dates, dict):
        program_dates = [program_dates]
    return actions, program_dates


def classify_action(action: dict) -> str:
    """Classify an enforcement action. Returns 'consent_decree',
    'broad_enforcement', or 'other'."""
    action_type = (action.get("ActionType") or "").lower()
    statute = (action.get("Statute") or "").upper()

    if statute not in ("CWA", "SDWA"):
        return "other"

    if any(kw in action_type for kw in CONSENT_DECREE_KEYWORDS):
        return "consent_decree"
    if any(kw in action_type for kw in BROAD_ENFORCEMENT_KEYWORDS):
        return "broad_enforcement"

    return "other"


def action_within_date_range(action: dict, cutoff_date: date) -> bool:
    """Check if an action's date falls within the allowed range."""
    action_date_str = action.get("ActionDate")
    if not action_date_str:
        return True
    parsed = parse_date_to_date(action_date_str)
    if not parsed:
        return True
    return parsed >= cutoff_date


def detect_pipe_infrastructure(action: dict, fac: dict) -> bool:
    """Check if the action or facility context suggests pipe/collection-system
    infrastructure is involved."""
    searchable = " ".join([
        action.get("ActionType") or "",
        action.get("PenaltyDesc") or "",
        fac.get("FacName") or "",
    ]).lower()
    return any(kw in searchable for kw in PIPE_INFRASTRUCTURE_KEYWORDS)


# ---------------------------------------------------------------------------
# EXTRACT – Census Bureau
# ---------------------------------------------------------------------------

def get_census_population(
    state_abbr: str,
    county_fips: str,
) -> Optional[int]:
    """Fetch total population from Census ACS 5-year estimates for a county."""
    census_key = os.getenv("CENSUS_API_KEY", "")
    state_fips = STATE_FIPS.get(state_abbr.upper())
    if not state_fips or not county_fips:
        return None

    if len(county_fips) == 5:
        county_fips = county_fips[2:]
    county_fips = county_fips.zfill(3)

    for year in ("2023", "2022", "2021"):
        url = f"{CENSUS_ACS_BASE}/{year}/acs/acs5"
        params = {
            "get": "B01003_001E",
            "for": f"county:{county_fips}",
            "in": f"state:{state_fips}",
        }
        if census_key:
            params["key"] = census_key

        try:
            sess = _get_thread_session()
            resp = sess.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                rows = resp.json()
                if len(rows) >= 2:
                    val = rows[1][0]
                    return int(val) if val and val not in ("-", "null") else None
        except Exception as exc:
            log.debug("Census %s failed for %s/%s: %s", year, state_fips, county_fips, exc)
            continue

    return None


# ---------------------------------------------------------------------------
# TRANSFORM
# ---------------------------------------------------------------------------

def parse_date(date_str: Optional[str]) -> Optional[str]:
    """Parse various date string formats to YYYY-MM-DD ISO format."""
    if not date_str:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def parse_date_to_date(date_str: Optional[str]) -> Optional[date]:
    """Parse a date string to a Python date object."""
    if not date_str:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None


def parse_penalty(penalty_str: Optional[str]) -> float:
    """Parse penalty string like '$12,375' to float."""
    if not penalty_str:
        return 0.0
    cleaned = penalty_str.replace("$", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def compute_urgency(days: Optional[int]) -> str:
    """Classify urgency tier based on days to compliance deadline."""
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


def compute_days_to_deadline(end_date_str: Optional[str]) -> Optional[int]:
    """Calculate days remaining until compliance schedule end date."""
    if not end_date_str:
        return None
    try:
        end_dt = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        return (end_dt - date.today()).days
    except ValueError:
        return None


def extract_county_fips(fac: dict) -> str:
    """Extract county FIPS code from facility record."""
    return fac.get("FacDerivedStctyFIPS") or fac.get("FacFIPSCode") or ""


def find_compliance_end_date(
    program_dates: list[dict], statute: str,
) -> Optional[str]:
    """Find the compliance schedule end date from ProgramDates for a
    given statute (CWA or SDWA)."""
    for pd in program_dates:
        if pd.get("Program", "").upper() == statute.upper():
            return pd.get("EndDate")
    latest = None
    for pd in program_dates:
        end = pd.get("EndDate")
        if end:
            parsed = parse_date_to_date(end)
            if parsed and (latest is None or parsed > latest):
                latest = parsed
    return latest.strftime("%m/%d/%Y") if latest else None


def build_case_number(source_id: str, action: dict) -> str:
    """Construct a unique case identifier from source ID and action details."""
    action_date = (action.get("ActionDate") or "").replace("/", "")
    action_type = (action.get("ActionType") or "")[:20].replace(" ", "_")
    return f"{source_id}_{action_date}_{action_type}"


def transform_record(
    fac: dict,
    action: dict,
    program_dates: list[dict],
    population: Optional[int],
    pipe_flag: bool,
) -> dict:
    """Transform a facility + enforcement action into a flat output record."""
    consent_date = parse_date(action.get("ActionDate"))
    statute = action.get("Statute", "")

    end_date_raw = find_compliance_end_date(program_dates, statute)
    end_date = parse_date(end_date_raw)
    days = compute_days_to_deadline(end_date)

    fips = extract_county_fips(fac)

    return {
        "case_number": build_case_number(
            fac.get("RegistryID") or fac.get("SourceID", ""), action
        ),
        "registry_id": fac.get("RegistryID", ""),
        "fips_code": fips,
        "facility_name": fac.get("FacName", ""),
        "city": fac.get("FacCity", ""),
        "state": fac.get("FacState", ""),
        "zip_code": fac.get("FacZip", ""),
        "county": fac.get("FacCounty", ""),
        "consent_decree_date": consent_date,
        "compliance_end_date": end_date,
        "lead_agency": action.get("LeadAgency", ""),
        "enforcement_level": _classify_enforcement_level(action.get("LeadAgency", "")),
        "action_type": action.get("ActionType", ""),
        "violation_type": statute,
        "penalty_amount": parse_penalty(action.get("PenaltyAmount")),
        "statute": statute,
        "pipe_infrastructure_flag": pipe_flag,
        "population": population,
        "days_to_deadline": days,
        "urgency_tier": compute_urgency(days),
        "latitude": _safe_float(fac.get("FacLat")),
        "longitude": _safe_float(fac.get("FacLong")),
        "last_updated": datetime.utcnow().isoformat(),
    }


def _classify_enforcement_level(lead_agency: str) -> str:
    """Classify whether an enforcement action is Federal, State, or Joint
    based on the lead agency field from ECHO."""
    la = (lead_agency or "").lower()
    if "epa" in la or "doj" in la or "federal" in la:
        return "Federal"
    if "state" in la:
        return "State"
    # If it contains both indicators, it's joint
    if la:
        return "State"  # ECHO defaults non-EPA to state
    return "Unknown"


def _safe_float(val) -> Optional[float]:
    """Safely convert a value to float."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# LOAD – BigQuery
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

    # Build temp table and MERGE
    temp_table_id = f"{dataset_id}._temp_consent_decrees_{int(time.time())}"
    temp_table = bigquery.Table(temp_table_id, schema=schema)
    client.create_table(temp_table, exists_ok=True)

    errors = client.insert_rows_json(temp_table_id, records)
    if errors:
        log.error("BigQuery insert errors: %s", errors)
        client.delete_table(temp_table_id, not_found_ok=True)
        return

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

    log.info("Executing MERGE into %s...", table_id)
    query_job = client.query(merge_sql)
    query_job.result()
    log.info(
        "MERGE complete — %d rows affected",
        query_job.num_dml_affected_rows or 0,
    )

    client.delete_table(temp_table_id, not_found_ok=True)


# ---------------------------------------------------------------------------
# ORCHESTRATION — Concurrent Facility Scan
# ---------------------------------------------------------------------------

def _enrich_facility(
    fac: dict,
    cutoff_date: date,
) -> tuple[list[dict], dict]:
    """Process a single facility: fetch its formal actions, classify them,
    enrich with Census data.  Returns (records, mini_stats).
    Called concurrently from the fallback path."""
    mini_stats = {
        "actions_checked": 0,
        "consent_decrees_found": 0,
        "broad_enforcement_found": 0,
        "pipe_infrastructure_flagged": 0,
        "filtered_by_date": 0,
        "census_enriched": 0,
        "census_errors": 0,
    }
    records = []

    source_ids = []
    for key in ("RegistryID", "CWPPermitNmbr", "SourceID", "MasterExternalPermitNmbr"):
        val = fac.get(key)
        if val and val not in source_ids:
            source_ids.append(val)

    if not source_ids:
        return records, mini_stats

    actions = []
    program_dates = []
    for sid in source_ids:
        actions, program_dates = fetch_formal_actions(sid)
        if actions:
            break
        time.sleep(0.2)

    mini_stats["actions_checked"] = len(actions)
    seen_cases = set()

    for action in actions:
        classification = classify_action(action)
        if classification == "other":
            continue

        if not action_within_date_range(action, cutoff_date):
            mini_stats["filtered_by_date"] += 1
            continue

        case_key = build_case_number(source_ids[0], action)
        if case_key in seen_cases:
            continue
        seen_cases.add(case_key)

        if classification == "consent_decree":
            mini_stats["consent_decrees_found"] += 1
        else:
            mini_stats["broad_enforcement_found"] += 1

        pipe_flag = detect_pipe_infrastructure(action, fac)
        if pipe_flag:
            mini_stats["pipe_infrastructure_flagged"] += 1

        population = None
        county_fips = extract_county_fips(fac)
        fac_state = fac.get("FacState", "")
        if county_fips and fac_state:
            try:
                population = get_census_population(fac_state, county_fips)
                if population:
                    mini_stats["census_enriched"] += 1
            except Exception:
                mini_stats["census_errors"] += 1

        record = transform_record(fac, action, program_dates, population, pipe_flag)
        records.append(record)

    return records, mini_stats


def run_facility_scan(
    state: Optional[str],
    cutoff_date: date,
    stats: dict,
) -> list[dict]:
    """Scan municipal facilities and check each for consent decree actions.
    Uses concurrent DFR calls (15 workers) for speed (~2 min vs 35 min)."""
    log.info("Scanning municipal facilities with %d concurrent workers...",
             MAX_WORKERS)

    facilities = search_all_municipal_facilities(state)
    if not facilities:
        log.warning("  No facilities found")
        return []

    stats["facilities_scanned"] = len(facilities)
    log.info("  Scanning %d facilities with %d concurrent workers...",
             len(facilities), MAX_WORKERS)

    records = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_enrich_facility, fac, cutoff_date): fac
            for fac in facilities
        }

        done_count = 0
        for future in as_completed(futures):
            done_count += 1
            if done_count % 100 == 0 or done_count == len(facilities):
                log.info(
                    "  Processed %d/%d facilities  [CDs so far: %d]",
                    done_count, len(facilities),
                    stats["consent_decrees_found"],
                )

            try:
                fac_records, mini_stats = future.result()
                records.extend(fac_records)
                for key in mini_stats:
                    if key in stats:
                        stats[key] += mini_stats[key]
            except Exception as exc:
                fac = futures[future]
                log.warning(
                    "  Error processing %s: %s",
                    fac.get("FacName", "Unknown"), exc,
                )
                stats["errors"] += 1

    return records


# ---------------------------------------------------------------------------
# ORCHESTRATION — Main Pipeline
# ---------------------------------------------------------------------------

def run_etl(
    state: Optional[str] = None,
    dry_run: bool = False,
    years_back: int = MAX_YEARS_BACK,
):
    """Main ETL pipeline: extract → transform → load.

    Searches ECHO for municipal water/wastewater facilities with formal
    enforcement actions, then checks each facility's DFR for consent decree
    actions using concurrent workers.  Census-enriches matches and loads
    to BigQuery with UPSERT logic.
    """
    cutoff_date = date.today() - relativedelta(years=years_back)

    log.info("=" * 60)
    log.info("IPI Consent Decree ETL v2 — %s",
             datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))
    log.info(
        "Mode: %s | State: %s | Date range: %s to present (%d years)",
        "DRY RUN" if dry_run else "LIVE",
        state or "ALL",
        cutoff_date.strftime("%Y-%m-%d"),
        years_back,
    )
    log.info("=" * 60)

    stats = {
        "facilities_scanned": 0,
        "actions_checked": 0,
        "consent_decrees_found": 0,
        "broad_enforcement_found": 0,
        "pipe_infrastructure_flagged": 0,
        "filtered_by_date": 0,
        "census_enriched": 0,
        "census_errors": 0,
        "records_output": 0,
        "errors": 0,
    }

    all_records = run_facility_scan(state, cutoff_date, stats)

    stats["records_output"] = len(all_records)
    log.info("Total unique enforcement records: %d", len(all_records))

    # Display sample records
    if all_records:
        log.info("")
        log.info("--- Sample Records (first 10) ---")
        for rec in all_records[:10]:
            pipe_marker = " [PIPE]" if rec.get("pipe_infrastructure_flag") else ""
            log.info(
                "  %s | %s, %s | %s | %s | $%.2f | Pop: %s | %s | %s%s",
                (rec.get("facility_name") or "Unknown")[:35],
                rec.get("city", ""),
                rec.get("state", ""),
                (rec.get("action_type") or "")[:30],
                rec.get("lead_agency", ""),
                rec.get("penalty_amount", 0),
                rec.get("population") or "N/A",
                rec.get("urgency_tier", "unknown"),
                rec.get("consent_decree_date") or "no date",
                pipe_marker,
            )

    # Load to BigQuery
    if all_records:
        load_to_bigquery(all_records, dry_run=dry_run)
    else:
        log.info("No enforcement records to load")

    print_summary(stats)


def print_summary(stats: dict):
    """Print a run summary."""
    log.info("")
    log.info("=" * 60)
    log.info("RUN SUMMARY")
    log.info("=" * 60)
    log.info("  Facilities scanned:          %d", stats.get("facilities_scanned", 0))
    log.info("  Enforcement actions checked:  %d", stats.get("actions_checked", 0))
    log.info("  Consent decrees (exact):      %d", stats.get("consent_decrees_found", 0))
    log.info("  Broad enforcement matches:    %d", stats.get("broad_enforcement_found", 0))
    log.info("  Pipe infrastructure flagged:  %d", stats.get("pipe_infrastructure_flagged", 0))
    log.info("  Filtered (date range):        %d", stats.get("filtered_by_date", 0))
    log.info("  Census-enriched records:      %d", stats.get("census_enriched", 0))
    log.info("  Census lookup errors:         %d", stats.get("census_errors", 0))
    log.info("  Total records output:         %d", stats.get("records_output", 0))
    log.info("  Errors encountered:           %d", stats.get("errors", 0))
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="IPI Consent Decree ETL v2 — concurrent facility scan",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Pull and transform data but do not write to BigQuery",
    )
    parser.add_argument(
        "--state",
        type=str,
        default=None,
        help="Two-letter state or territory code to limit the pull (e.g. TX, CA, PR)",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=MAX_YEARS_BACK,
        help=f"Number of years back to pull (max {MAX_YEARS_BACK}, default {MAX_YEARS_BACK})",
    )
    return parser.parse_args()


def main():
    """Entry point."""
    args = parse_args()

    years = min(args.years, MAX_YEARS_BACK)
    if years < 1:
        log.error("--years must be at least 1")
        sys.exit(1)

    try:
        run_etl(state=args.state, dry_run=args.dry_run, years_back=years)
    except KeyboardInterrupt:
        log.info("ETL interrupted by user")
        sys.exit(1)
    except Exception as exc:
        log.exception("ETL failed with unexpected error: %s", exc)
        sys.exit(2)


if __name__ == "__main__":
    main()
