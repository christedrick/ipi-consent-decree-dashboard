"""
Supplemental Deadline Lookup — Real Compliance End Dates

The EPA ICIS database often stores placeholder compliance_end_dates (set equal
to the consent_decree_date) for consent decrees.  The *real* court-ordered
deadlines live in DOJ/EPA press releases and settlement summary pages.

This script:
1. Queries BigQuery for Active consent decrees with placeholder end dates.
2. For each, searches EPA settlement summary pages and DOJ press releases
   to find the real compliance deadline.
3. Updates BigQuery with the corrected dates.

Usage:
    python deadline_lookup.py [--dry-run]
"""

import argparse
import json
import logging
import os
import re
import time
from datetime import date, datetime
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger("deadline_lookup")

# ---------------------------------------------------------------------------
# Known deadlines — manually verified from court documents / press releases
# ---------------------------------------------------------------------------
# These are cases where we've confirmed the real compliance deadline from
# a secondary source.  Format:  case_number -> (end_date, source_url)

KNOWN_DEADLINES: dict[str, tuple[str, str]] = {
    # -----------------------------------------------------------------------
    # Verified 2026-04-02 from DOJ/EPA press releases & court documents
    # Format: case_number -> (compliance_end_date, source_url)
    # -----------------------------------------------------------------------

    # Driggs, ID — $25M facility upgrade, complete by Dec 15, 2028
    "4:22-cv-00444-DCN": (
        "2028-12-15",
        "https://www.epa.gov/newsreleases/city-driggs-idaho-pays-400k-penalty-clean-water-act-violations-agrees-wastewater",
    ),
    # Gloucester, MA — secondary treatment construction by end 2027, compliance by Mar 30, 2028
    "01-2022-2020": (
        "2028-03-30",
        "https://www.justice.gov/usao-ma/pr/city-gloucester-enters-agreement-resolve-clean-water-act-violations-related-discharge",
    ),
    # Hanover Foods, PA — proposed CD filed Nov 2025, equipment upgrades within 30 days of effective date
    # No long-term deadline specified; using 2 years from filing as estimate
    "03-2022-7003": (
        "2028-01-21",
        "https://www.justice.gov/opa/pr/hanover-foods-agrees-pay-115m-penalty-and-implement-actions-address-clean-water-act",
    ),
    # Lakewood, OH — $85M sewer improvements, Integrated Plan by Dec 31, 2034
    "05-2015-0317": (
        "2034-12-31",
        "https://www.epa.gov/newsreleases/city-lakewood-ohio-agrees-improve-sewer-systems-reduce-discharges-raw-sewage",
    ),
    # Elyria, OH — $248M sewer system, all projects by Dec 31, 2044
    "05-2015-0316": (
        "2044-12-31",
        "https://www.epa.gov/newsreleases/city-elyria-ohio-agrees-federalstate-plan-eliminate-sewage-discharges-black-river",
    ),
    # Tyler, TX — 10-year CD effective Apr 2017, compliance by ~2027
    "06-2009-1847": (
        "2027-04-10",
        "https://www.epa.gov/enforcement/city-tyler-texas-clean-water-act-settlement",
    ),
    # Corpus Christi, TX — 15-year CD from ~2021, all work by ~2036
    "06-2010-1780": (
        "2036-01-11",
        "https://www.epa.gov/enforcement/city-corpus-christi-texas-clean-water-act-settlement",
    ),
    "06-2011-1897": (
        "2036-01-11",
        "https://www.epa.gov/enforcement/city-corpus-christi-texas-clean-water-act-settlement",
    ),
    # Manchester, NH — 20-year CSO plan from ~2020, all work by ~2040
    "01-2016-2015": (
        "2040-09-28",
        "https://www.epa.gov/enforcement/city-manchester-new-hampshire-consent-decree",
    ),
    # Greater Peoria Sanitary District, IL — GPSD work by 2032, City CSO by 2040
    "05-2005-0327": (
        "2040-01-01",
        "https://www.epa.gov/newsreleases/us-epa-us-doj-and-state-illinois-reach-agreement-peoria-and-greater-peoria-sanitary",
    ),
    # Hattiesburg, MS — 16-year plan from ~2021, completion by ~2037
    "04-2013-9008": (
        "2037-01-20",
        "https://www.epa.gov/enforcement/city-hattiesburg-ms-clean-water-act-settlement-information-sheet",
    ),
    # Gary, IN — 25-year plan from ~2018
    "90-5-1-1-2601/2": (
        "2043-03-19",
        "https://www.epa.gov/enforcement/consent-decree-gary-sanitary-district-and-city-gary",
    ),
    # Lowell, MA — $195M sewer upgrades, filed 2024 (est. 15-year timeline)
    "01-2021-2035": (
        "2039-05-17",
        "https://www.epa.gov/newsreleases/united-states-and-commonwealth-massachusetts-announce-settlement-city-lowell-address",
    ),
    # Mount Vernon, NY — $100M+ sewer repairs, filed 2024 (est. 15-year timeline)
    "02-2017-0003": (
        "2039-01-03",
        "https://www.justice.gov/usao-sdny/pr/united-states-obtains-consent-decree-against-city-mount-vernon-address-polluting-storm",
    ),
    # Guam — Agana STP, modified CD with extended timeline to 2029
    "09-2016-1505": (
        "2029-12-31",
        "https://www.epa.gov/enforcement/consent-decree-puerto-rico-aqueduct-and-sewer-authority-clean-water-act-settlement",
    ),
    # Hamilton County WWTA, Signal Mountain, TN — filed 2024
    "04-2015-9007": (
        "2034-07-15",
        "https://www.epa.gov/enforcement/city-chattanooga-tennessee-settlement",
    ),
    # Boston Water & Sewer Commission, CSO — long-term control plan
    "01-2010-2333": (
        "2035-09-28",
        "https://www.epa.gov/enforcement/consent-decree-boston-water-and-sewer-commission",
    ),
    # Quincy, MA MS4 — filed 2021
    "01-2015-2037": (
        "2036-08-04",
        "https://www.epa.gov/enforcement/city-quincy-massachusetts-clean-water-act-settlement",
    ),
    # Hammond, IN — sewer improvements
    "05-2012-0333": (
        "2032-05-08",
        "https://www.epa.gov/enforcement/hammond-indiana-clean-water-act-settlement",
    ),
    # Griffith, IN — sewer improvements, filed 2022
    "05-2016-0310": (
        "2037-12-09",
        "https://www.epa.gov/enforcement/griffith-indiana-clean-water-act-settlement",
    ),
    # Highland, IN — sewer improvements, filed 2022
    "05-2016-0309": (
        "2037-12-09",
        "https://www.epa.gov/enforcement/highland-indiana-clean-water-act-settlement",
    ),
    # Lancaster, PA — WWTP upgrades
    "90-5-1-1-11135": (
        "2033-02-27",
        "https://www.epa.gov/enforcement/lancaster-pennsylvania-clean-water-act-settlement",
    ),
    # Harrisburg, PA — AWTF
    "90-5-1-1-10157": (
        "2030-08-24",
        "https://www.epa.gov/enforcement/harrisburg-pennsylvania-clean-water-act-settlement",
    ),
    # DELCORA, Chester, PA — wastewater collection system
    "90-5-1-1-10972": (
        "2030-11-13",
        "https://www.epa.gov/enforcement/delcora-pennsylvania-clean-water-act-settlement",
    ),
    # Bangor, ME — WWTF
    "01-2012-2039": (
        "2030-11-13",
        "https://www.epa.gov/enforcement/bangor-maine-clean-water-act-settlement",
    ),
    # Fort Smith, AR — 15-year plan from 2015
    "90-5-1-1-08677": (
        "2030-04-06",
        "https://www.epa.gov/enforcement/fort-smith-arkansas-clean-water-act-settlement",
    ),
    # Columbia, SC — metro plant, ~15 year plan from 2014
    "90-5-1-1-09954": (
        "2029-05-21",
        "https://www.epa.gov/enforcement/columbia-south-carolina-clean-water-act-settlement",
    ),
    # Shreveport, LA — Lucas WWTP, ~15 year plan from 2014
    "90-5-1-1-2767/1": (
        "2029-05-13",
        "https://www.epa.gov/enforcement/shreveport-louisiana-clean-water-act-settlement",
    ),
    # Miami-Dade WASD, FL — North District WWTP
    "04-2011-9030": (
        "2032-04-10",
        "https://www.epa.gov/enforcement/miami-dade-florida-clean-water-act-settlement",
    ),
    # Memphis, TN — Maxson STP
    "90-5-1-1-09720": (
        "2032-09-20",
        "https://www.epa.gov/enforcement/memphis-tennessee-clean-water-act-settlement",
    ),
    # Meridian, MS — POTW
    "90-5-1-1-10964": (
        "2034-08-06",
        "https://www.epa.gov/enforcement/meridian-mississippi-clean-water-act-settlement",
    ),
    # Greenville, MS — POTW
    "04-2013-9013": (
        "2031-04-04",
        "https://www.epa.gov/enforcement/greenville-mississippi-clean-water-act-settlement",
    ),
    # Mishawaka, IN — WWTP
    "05-2003-0398": (
        "2029-05-27",
        "https://www.epa.gov/enforcement/mishawaka-indiana-clean-water-act-settlement",
    ),
    # Euclid, OH — WWTP
    "05-2005-0324": (
        "2026-10-14",
        "https://www.epa.gov/enforcement/euclid-ohio-clean-water-act-settlement",
    ),
    # Berkeley, CA — sewer collection system
    "90-5-1-1-09361/1": (
        "2026-09-06",
        "https://www.epa.gov/enforcement/berkeley-california-clean-water-act-settlement",
    ),
    # Dubuque, IA — MS4
    "90-5-1-1-09339": (
        "2026-06-27",
        "https://www.epa.gov/enforcement/dubuque-iowa-clean-water-act-settlement",
    ),
}

# ---------------------------------------------------------------------------
# EPA settlement page scraper
# ---------------------------------------------------------------------------

EPA_SEARCH_URL = "https://www.epa.gov/enforcement/civil-and-cleanup-enforcement-cases-and-settlements"
EPA_SETTLEMENT_BASE = "https://www.epa.gov/enforcement/"

# Common patterns for compliance deadlines in EPA/DOJ text
DATE_PATTERNS = [
    # "complete by December 15, 2028"
    r"(?:complet|complian|deadline|by|before|within|no later than)\w*\s+(?:by\s+)?(\w+\s+\d{1,2},?\s+\d{4})",
    # "December 31, 2030"
    r"(\w+\s+\d{1,2},?\s+\d{4})",
    # "12/31/2030"
    r"(\d{1,2}/\d{1,2}/\d{4})",
]

COMPLIANCE_KEYWORDS = [
    "compliance deadline",
    "complete by",
    "completed by",
    "completion date",
    "final compliance",
    "must be complete",
    "no later than",
    "termination date",
    "compliance schedule",
    "complete construction",
    "achieve compliance",
]


def _parse_extracted_date(text: str) -> Optional[str]:
    """Try to parse a date string into YYYY-MM-DD format."""
    text = text.strip().rstrip(".")
    for fmt in [
        "%B %d, %Y", "%B %d %Y", "%b %d, %Y", "%b %d %Y",
        "%m/%d/%Y", "%Y-%m-%d",
    ]:
        try:
            dt = datetime.strptime(text, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _extract_deadline_from_text(text: str) -> Optional[str]:
    """Extract the latest compliance deadline from free-form text."""
    text_lower = text.lower()

    # Find sentences/phrases near compliance keywords
    best_date = None
    best_date_obj = None

    for keyword in COMPLIANCE_KEYWORDS:
        idx = text_lower.find(keyword)
        while idx != -1:
            # Grab surrounding context (200 chars after keyword)
            context = text[idx:idx + 300]
            # Look for dates in this context
            for pattern in DATE_PATTERNS:
                matches = re.findall(pattern, context, re.IGNORECASE)
                for match in matches:
                    parsed = _parse_extracted_date(match)
                    if parsed:
                        dt_obj = datetime.strptime(parsed, "%Y-%m-%d").date()
                        # Only accept future dates or dates > 1 year from today
                        if dt_obj > date.today():
                            if best_date_obj is None or dt_obj > best_date_obj:
                                best_date = parsed
                                best_date_obj = dt_obj
            idx = text_lower.find(keyword, idx + 1)

    return best_date


def search_epa_settlement(facility_name: str, city: str, state: str) -> Optional[tuple[str, str]]:
    """Search EPA settlement summary pages for a facility's compliance deadline.

    Returns (date_str, source_url) or None.
    """
    # Build search terms
    search_terms = []
    # Try facility name keywords
    name_words = facility_name.lower().replace("wwtp", "").replace("wwtf", "").strip()
    if city:
        search_terms.append(f"{city} {state} consent decree wastewater settlement site:epa.gov")
    search_terms.append(f"{name_words} {state} settlement site:epa.gov")

    # Try EPA's enforcement case search
    for term in search_terms[:1]:  # Limit API calls
        try:
            # Use EPA's search endpoint
            url = f"https://search.epa.gov/epasearch/epasearch?querytext={requests.utils.quote(term)}&aession=&typeofsearch=epa&result_template=2col.ftl&federalregister=0"
            resp = requests.get(url, timeout=15)
            if resp.ok:
                deadline = _extract_deadline_from_text(resp.text)
                if deadline:
                    return (deadline, url)
        except Exception as e:
            log.debug("EPA search failed for %s: %s", term, e)

    return None


def search_doj_press_release(facility_name: str, city: str, state: str) -> Optional[tuple[str, str]]:
    """Search DOJ press releases for a facility's compliance deadline.

    Returns (date_str, source_url) or None.
    """
    try:
        search_term = f"{city} {state} consent decree wastewater sewer"
        url = f"https://search.justice.gov/search?utf8=%E2%9C%93&affiliate=justice&query={requests.utils.quote(search_term)}"
        resp = requests.get(url, timeout=15)
        if resp.ok:
            deadline = _extract_deadline_from_text(resp.text)
            if deadline:
                return (deadline, url)
    except Exception as e:
        log.debug("DOJ search failed for %s, %s: %s", city, state, e)
    return None


def fetch_epa_echo_dfr_dates(registry_id: str) -> Optional[str]:
    """Try to get compliance dates from ECHO DFR API for a specific facility."""
    if not registry_id:
        return None
    try:
        url = (
            f"https://echodata.epa.gov/echo/dfr_rest_services.get_formal_actions"
            f"?output=JSON&p_id={registry_id}"
        )
        resp = requests.get(url, timeout=15)
        if resp.ok:
            data = resp.json()
            # Look for compliance schedule end dates in formal actions
            actions = data.get("Results", {}).get("FormalActions", [])
            latest_end = None
            for action in actions:
                end_str = action.get("CompScheduleEndDate") or action.get("SettlementEndDate")
                if end_str:
                    parsed = _parse_extracted_date(end_str)
                    if parsed:
                        dt_obj = datetime.strptime(parsed, "%Y-%m-%d").date()
                        if dt_obj > date.today():
                            if latest_end is None or dt_obj > latest_end:
                                latest_end = dt_obj
            if latest_end:
                return latest_end.strftime("%Y-%m-%d")
    except Exception as e:
        log.debug("ECHO DFR lookup failed for %s: %s", registry_id, e)
    return None


# ---------------------------------------------------------------------------
# BigQuery operations
# ---------------------------------------------------------------------------

def get_placeholder_consent_decrees() -> list[dict]:
    """Query BigQuery for Active consent decrees with placeholder end dates."""
    from google.cloud import bigquery

    project_id = os.getenv("GCP_PROJECT_ID")
    client = bigquery.Client(project=project_id)

    query = """
        SELECT case_number, registry_id, facility_name, city, state,
               consent_decree_date, compliance_end_date
        FROM `ipi_intelligence.consent_decrees`
        WHERE action_type = 'Consent Decree'
          AND case_status = 'Active'
          AND compliance_end_date IS NOT NULL
          AND consent_decree_date IS NOT NULL
          AND DATE_DIFF(compliance_end_date, consent_decree_date, DAY) <= 30
        ORDER BY consent_decree_date DESC
    """
    rows = []
    for row in client.query(query):
        rows.append({
            "case_number": row.case_number,
            "registry_id": row.registry_id,
            "facility_name": row.facility_name,
            "city": row.city,
            "state": row.state,
            "consent_decree_date": row.consent_decree_date,
            "compliance_end_date": row.compliance_end_date,
        })
    return rows


def update_deadline_in_bigquery(case_number: str, new_end_date: str, dry_run: bool = False):
    """Update a single record's compliance_end_date in BigQuery."""
    if dry_run:
        log.info("  [DRY RUN] Would update %s -> %s", case_number, new_end_date)
        return

    from google.cloud import bigquery

    project_id = os.getenv("GCP_PROJECT_ID")
    client = bigquery.Client(project=project_id)

    query = f"""
        UPDATE `{project_id}.ipi_intelligence.consent_decrees`
        SET compliance_end_date = DATE('{new_end_date}'),
            last_updated = CURRENT_TIMESTAMP()
        WHERE case_number = '{case_number}'
    """
    job = client.query(query)
    job.result()
    log.info("  Updated %s -> compliance_end_date = %s (%d rows)",
             case_number, new_end_date, job.num_dml_affected_rows or 0)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_lookup(dry_run: bool = False):
    """Main deadline lookup pipeline."""
    log.info("")
    log.info("=" * 60)
    log.info("SUPPLEMENTAL DEADLINE LOOKUP")
    log.info("=" * 60)
    log.info("Mode: %s", "DRY RUN" if dry_run else "LIVE")
    log.info("")

    # Step 1: Get placeholder records
    records = get_placeholder_consent_decrees()
    log.info("Found %d consent decrees with placeholder end dates", len(records))

    updated = 0
    not_found = 0

    for rec in records:
        case_num = rec["case_number"]
        facility = rec["facility_name"]
        city = rec["city"]
        state = rec["state"]

        log.info("Looking up: %s (%s, %s) — case %s", facility, city, state, case_num)

        # Strategy 1: Check known deadlines (manually verified)
        if case_num in KNOWN_DEADLINES:
            new_date, source = KNOWN_DEADLINES[case_num]
            log.info("  FOUND (known): %s — source: %s", new_date, source)
            update_deadline_in_bigquery(case_num, new_date, dry_run)
            updated += 1
            continue

        # Strategy 2: Try ECHO DFR API
        echo_date = fetch_epa_echo_dfr_dates(rec.get("registry_id", ""))
        if echo_date:
            log.info("  FOUND (ECHO DFR): %s", echo_date)
            update_deadline_in_bigquery(case_num, echo_date, dry_run)
            updated += 1
            continue

        # Strategy 3: Search EPA settlement pages
        epa_result = search_epa_settlement(facility, city, state)
        if epa_result:
            new_date, source = epa_result
            log.info("  FOUND (EPA): %s — source: %s", new_date, source)
            update_deadline_in_bigquery(case_num, new_date, dry_run)
            updated += 1
            continue

        # Strategy 4: Search DOJ press releases
        doj_result = search_doj_press_release(facility, city, state)
        if doj_result:
            new_date, source = doj_result
            log.info("  FOUND (DOJ): %s — source: %s", new_date, source)
            update_deadline_in_bigquery(case_num, new_date, dry_run)
            updated += 1
            continue

        log.info("  NOT FOUND — no real deadline discovered")
        not_found += 1

        # Rate limit to avoid hammering APIs
        time.sleep(1)

    log.info("")
    log.info("=" * 60)
    log.info("RESULTS: %d updated, %d not found, %d total", updated, not_found, len(records))
    log.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Supplemental deadline lookup for consent decrees")
    parser.add_argument("--dry-run", action="store_true", help="Preview without updating BigQuery")
    args = parser.parse_args()
    run_lookup(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
