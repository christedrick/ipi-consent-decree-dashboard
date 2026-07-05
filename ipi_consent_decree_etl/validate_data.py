"""
Data Validation — IPI Consent Decree Dashboard

Runs after each ETL refresh to flag suspect records before they reach
the dashboard. Catches the systemic ICIS data quality issues:

  1. Placeholder deadlines (end_date == start_date)
  2. Penalty amounts that may combine civil penalty + SEP/capital costs
  3. Non-municipal facilities incorrectly flagged as pipe infrastructure
  4. Missing or stale dates
  5. Federal consent decrees that can be cross-referenced against EPA ECHO API

Outputs a validation report and optionally writes flags to BigQuery.

Usage:
    python validate_data.py [--fix]     # --fix auto-corrects where confident
    python validate_data.py             # report only (default)
"""

import json
import os
import sys
from datetime import date, datetime

import requests
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
load_dotenv(os.path.expanduser("~/.config/ipi-etl/.env"))  # secrets live outside the synced repo dir

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ipi-consent-decree-dashboard")
TABLE = f"`{PROJECT_ID}.ipi_intelligence.consent_decrees`"

# ---------------------------------------------------------------------------
# Thresholds for flagging suspect data
# ---------------------------------------------------------------------------

# Penalties above this for a single consent decree are unusual — likely
# combining civil penalty + supplemental environmental project (SEP) costs.
# The vast majority of CWA civil penalties are under $5M; amounts above
# that warrant manual review against DOJ/EPA press releases.
PENALTY_REVIEW_THRESHOLD = 5_000_000

# Consent decree durations outside this range are suspect
MIN_PLAUSIBLE_DURATION_YEARS = 0.5   # less than 6 months is unusual
MAX_PLAUSIBLE_DURATION_YEARS = 40    # more than 40 years is unusual

# Non-municipal keywords (mirrors etl_bulk.py NON_MUNICIPAL_KEYWORDS)
NON_MUNICIPAL_KEYWORDS = [
    " corp", " inc", " llc", " ltd", " co.", " company",
    "foods", "refinery", "chemical", "manufacturing", "mining", " mine ",
    "steel", "paper", " mill ", "packing", "processing", "petroleum",
    "energy", "power plant", "electric", " gas ", " oil ",
    "pharmaceutical", "textile", "brewery", "distill",
    "tannery", "rendering", "slaughter", "meat", "dairy",
    "sugar", "ethanol", "lumber", "aggregate", "quarry",
    "cement", "concrete", "asphalt", "plastic", "rubber",
    "camp ", "campground", "resort", "lodge", "hotel", "motel",
    "conference center",
    "subdivision", "estates wwtp", "mobile home", " mhp ", "mhc,",
    "property owners", "homeowners", "trailer park", "rv park",
    "marina", " farm ", "ranch ", "shopping center", "plaza wwtp",
    "egg farm", "truck plaza",
]

# EPA ECHO API for cross-referencing federal records
ECHO_API = "https://echodata.epa.gov/echo/dfr_rest_services"


# ---------------------------------------------------------------------------
# Validation checks
# ---------------------------------------------------------------------------

def check_placeholder_deadlines(client):
    """Flag records where compliance_end_date == consent_decree_date."""
    q = f"""
    SELECT case_number, facility_name, city, state, consent_decree_date,
           compliance_end_date, action_type
    FROM {TABLE}
    WHERE compliance_end_date = consent_decree_date
      AND compliance_end_date IS NOT NULL
      AND case_status = 'Active'
      AND action_type = 'Consent Decree'
    ORDER BY consent_decree_date DESC
    """
    rows = list(client.query(q).result())
    return [{
        "check": "placeholder_deadline",
        "severity": "high",
        "case_number": r.case_number,
        "facility": r.facility_name,
        "location": f"{r.city}, {r.state}",
        "detail": f"end_date == start_date ({r.consent_decree_date})",
        "action": "Look up real deadline via deadline_lookup.py or DOJ/EPA press releases",
    } for r in rows]


def check_suspect_penalties(client):
    """Flag records with unusually high penalties (may include SEP/capital)."""
    q = f"""
    SELECT case_number, facility_name, city, state, penalty_amount, action_type
    FROM {TABLE}
    WHERE penalty_amount > {PENALTY_REVIEW_THRESHOLD}
      AND case_status = 'Active'
    ORDER BY penalty_amount DESC
    """
    rows = list(client.query(q).result())
    return [{
        "check": "suspect_penalty",
        "severity": "medium",
        "case_number": r.case_number,
        "facility": r.facility_name,
        "location": f"{r.city}, {r.state}",
        "detail": f"Penalty ${r.penalty_amount:,.0f} exceeds ${PENALTY_REVIEW_THRESHOLD:,.0f} — may include SEP/capital costs",
        "action": "Verify civil penalty amount against DOJ/EPA press release",
    } for r in rows]


def check_non_municipal_pipe_flags(client):
    """Flag records where pipe_infrastructure_flag=TRUE but facility name suggests non-municipal."""
    q = f"""
    SELECT case_number, facility_name, city, state, action_type
    FROM {TABLE}
    WHERE pipe_infrastructure_flag = TRUE
      AND case_status = 'Active'
    ORDER BY facility_name
    """
    rows = list(client.query(q).result())
    # Municipal indicators — if name contains these, it's likely municipal
    # even if it also matches a non-municipal keyword (e.g., "Town of Oil City")
    municipal_overrides = [
        "city of", "town of", "village of", "county", "district",
        "authority", "municipal", "mud ", "mud no", "public works",
    ]

    flagged = []
    for r in rows:
        name_lower = (r.facility_name or "").lower()
        # Skip if clearly municipal
        if any(m in name_lower for m in municipal_overrides):
            continue
        for kw in NON_MUNICIPAL_KEYWORDS:
            if kw in name_lower:
                flagged.append({
                    "check": "non_municipal_pipe_flag",
                    "severity": "medium",
                    "case_number": r.case_number,
                    "facility": r.facility_name,
                    "location": f"{r.city}, {r.state}",
                    "detail": f"Pipe-flagged but facility name contains '{kw.strip()}'",
                    "action": "Review whether this is a municipal facility; set pipe_infrastructure_flag=FALSE if not",
                })
                break
    return flagged


def check_missing_dates(client):
    """Flag active records missing key dates."""
    q = f"""
    SELECT case_number, facility_name, city, state, action_type,
           consent_decree_date, compliance_end_date
    FROM {TABLE}
    WHERE case_status = 'Active'
      AND action_type = 'Consent Decree'
      AND (consent_decree_date IS NULL OR compliance_end_date IS NULL)
    """
    rows = list(client.query(q).result())
    return [{
        "check": "missing_dates",
        "severity": "low",
        "case_number": r.case_number,
        "facility": r.facility_name,
        "location": f"{r.city}, {r.state}",
        "detail": f"Missing {'start date' if not r.consent_decree_date else 'end date'}",
        "action": "Look up dates via ECHO or DOJ/EPA sources",
    } for r in rows]


def check_implausible_durations(client):
    """Flag consent decrees with unusually short or long durations."""
    q = f"""
    SELECT * FROM (
      SELECT case_number, facility_name, city, state,
             consent_decree_date, compliance_end_date,
             DATE_DIFF(compliance_end_date, consent_decree_date, DAY) / 365.25 as duration_years
      FROM {TABLE}
      WHERE compliance_end_date IS NOT NULL
        AND consent_decree_date IS NOT NULL
        AND compliance_end_date != consent_decree_date
        AND case_status = 'Active'
        AND action_type = 'Consent Decree'
    )
    WHERE duration_years < {MIN_PLAUSIBLE_DURATION_YEARS}
       OR duration_years > {MAX_PLAUSIBLE_DURATION_YEARS}
    ORDER BY duration_years
    """
    rows = list(client.query(q).result())
    return [{
        "check": "implausible_duration",
        "severity": "medium",
        "case_number": r.case_number,
        "facility": r.facility_name,
        "location": f"{r.city}, {r.state}",
        "detail": f"Duration {r.duration_years:.1f} years ({r.consent_decree_date} → {r.compliance_end_date})",
        "action": "Verify deadline against court documents",
    } for r in rows]


def cross_reference_echo(client, max_checks=20):
    """Cross-reference a sample of federal consent decrees against EPA ECHO API.

    Checks penalty amounts where ECHO has data. Limited to max_checks to
    avoid rate limiting.
    """
    q = f"""
    SELECT case_number, registry_id, facility_name, city, state, penalty_amount
    FROM {TABLE}
    WHERE enforcement_level = 'Federal'
      AND action_type = 'Consent Decree'
      AND case_status = 'Active'
      AND registry_id IS NOT NULL
      AND registry_id != ''
      AND penalty_amount > 0
    ORDER BY consent_decree_date DESC
    LIMIT {max_checks}
    """
    rows = list(client.query(q).result())
    flagged = []

    for r in rows:
        try:
            url = f"{ECHO_API}.get_enforcement_summary?p_id={r.registry_id}&output=JSON"
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                continue
            data = resp.json()
            # Navigate ECHO JSON structure
            results = data.get("Results", {})
            enf = results.get("EnforcementSummary", [])
            if not enf:
                continue
            echo_penalty = None
            for e in enf:
                tp = e.get("TotalCasePenalties")
                if tp:
                    try:
                        echo_penalty = float(tp.replace(",", "").replace("$", ""))
                    except (ValueError, AttributeError):
                        pass
            if echo_penalty and abs(echo_penalty - r.penalty_amount) > 1000:
                flagged.append({
                    "check": "echo_penalty_mismatch",
                    "severity": "high",
                    "case_number": r.case_number,
                    "facility": r.facility_name,
                    "location": f"{r.city}, {r.state}",
                    "detail": f"Our DB: ${r.penalty_amount:,.0f} vs ECHO: ${echo_penalty:,.0f}",
                    "action": "Investigate which is the civil penalty vs total settlement",
                })
        except Exception:
            continue

    return flagged


# ---------------------------------------------------------------------------
# Auto-fix (when --fix is passed)
# ---------------------------------------------------------------------------

def auto_fix_non_municipal_flags(client, issues):
    """Set pipe_infrastructure_flag=FALSE for confirmed non-municipal facilities."""
    non_muni = [i for i in issues if i["check"] == "non_municipal_pipe_flag"]
    if not non_muni:
        return 0
    case_numbers = [i["case_number"] for i in non_muni]
    # Batch update
    placeholders = ", ".join([f"'{cn}'" for cn in case_numbers])
    q = f"""
    UPDATE {TABLE}
    SET pipe_infrastructure_flag = FALSE
    WHERE case_number IN ({placeholders})
    """
    job = client.query(q)
    job.result()
    return job.num_dml_affected_rows


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def print_report(all_issues):
    """Print a formatted validation report."""
    by_check = {}
    for issue in all_issues:
        by_check.setdefault(issue["check"], []).append(issue)

    high_count = sum(1 for i in all_issues if i["severity"] == "high")
    med_count = sum(1 for i in all_issues if i["severity"] == "medium")
    low_count = sum(1 for i in all_issues if i["severity"] == "low")

    print("=" * 80)
    print("IPI DATA VALIDATION REPORT")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Issues found: {len(all_issues)} ({high_count} high, {med_count} medium, {low_count} low)")
    print("=" * 80)

    severity_order = {"high": 0, "medium": 1, "low": 2}
    for check_name, issues in sorted(by_check.items(),
                                      key=lambda x: severity_order.get(x[1][0]["severity"], 9)):
        sev = issues[0]["severity"].upper()
        print(f"\n[{sev}] {check_name} — {len(issues)} issue(s)")
        print("-" * 60)
        for i in issues[:10]:  # Show first 10
            print(f"  {i['case_number']:<20} {i['facility'][:40]:<40} {i['location']}")
            print(f"    → {i['detail']}")
        if len(issues) > 10:
            print(f"  ... and {len(issues) - 10} more")

    print("\n" + "=" * 80)
    return high_count, med_count, low_count


def save_report(all_issues, path):
    """Save the validation report as JSON for the dashboard to read."""
    report = {
        "timestamp": datetime.now().isoformat(),
        "total_issues": len(all_issues),
        "high": sum(1 for i in all_issues if i["severity"] == "high"),
        "medium": sum(1 for i in all_issues if i["severity"] == "medium"),
        "low": sum(1 for i in all_issues if i["severity"] == "low"),
        "issues": all_issues,
    }
    with open(path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nReport saved to {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    auto_fix = "--fix" in sys.argv

    os.environ.setdefault(
        "GOOGLE_APPLICATION_CREDENTIALS",
        os.path.join(os.path.dirname(__file__), "service-account.json"),
    )
    client = bigquery.Client(project=PROJECT_ID)

    print("Running data validation checks...")
    all_issues = []

    # Run all checks
    checks = [
        ("Placeholder deadlines", check_placeholder_deadlines),
        ("Suspect penalties", check_suspect_penalties),
        ("Non-municipal pipe flags", check_non_municipal_pipe_flags),
        ("Missing dates", check_missing_dates),
        ("Implausible durations", check_implausible_durations),
        ("ECHO cross-reference", lambda c: cross_reference_echo(c, max_checks=20)),
    ]

    for name, check_fn in checks:
        print(f"  Checking: {name}...")
        try:
            issues = check_fn(client)
            all_issues.extend(issues)
            print(f"    → {len(issues)} issue(s)")
        except Exception as e:
            print(f"    → ERROR: {e}")

    # Print report
    high, med, low = print_report(all_issues)

    # Save JSON report for dashboard
    report_path = os.path.join(os.path.dirname(__file__), "validation_report.json")
    save_report(all_issues, report_path)

    # Auto-fix if requested
    if auto_fix:
        print("\n--- AUTO-FIX MODE ---")
        fixed = auto_fix_non_municipal_flags(client, all_issues)
        if fixed:
            print(f"  Unflagged {fixed} non-municipal pipe records")
        else:
            print("  No auto-fixable issues found")

    # Exit code based on severity
    if high > 0:
        print(f"\n⚠ {high} HIGH severity issues require manual review")
        sys.exit(1)
    else:
        print("\n✓ No high-severity issues")
        sys.exit(0)


if __name__ == "__main__":
    main()
