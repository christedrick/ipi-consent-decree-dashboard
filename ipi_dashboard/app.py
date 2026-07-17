"""
IPI Pipe — Consent Decree Sales Intelligence Dashboard

Streamlit dashboard powered by BigQuery data from the consent decree ETL.
Helps the IPI Pipe sales team identify municipalities under CWA/SDWA consent
decrees that are likely to need underground pipe infrastructure services.
"""

import json
import os
import subprocess
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

# ---------------------------------------------------------------------------
# Cloud vs local credential detection
# ---------------------------------------------------------------------------
try:
    _RUNNING_ON_CLOUD = bool(
        os.getenv("STREAMLIT_SHARING_MODE")
        or (hasattr(st, "secrets") and "gcp_service_account" in st.secrets)
    )
except Exception:
    _RUNNING_ON_CLOUD = False


def _get_bigquery_client(project_id):
    """Create a BigQuery client using cloud secrets or local credentials."""
    if _RUNNING_ON_CLOUD:
        try:
            credentials = service_account.Credentials.from_service_account_info(
                dict(st.secrets["gcp_service_account"])
            )
            return bigquery.Client(project=project_id, credentials=credentials)
        except Exception:
            pass
    return bigquery.Client(project=project_id)

# ---------------------------------------------------------------------------
# Access control — write actions are editor-only when sharing is enabled
# ---------------------------------------------------------------------------

def _viewer_email() -> str:
    """Signed-in viewer's email (Streamlit Cloud provides it on private apps)."""
    try:
        return (getattr(st.user, "email", "") or "").lower()
    except Exception:
        return ""


def can_edit() -> bool:
    """True if this viewer may use write actions (queue research, review
    contacts). Controlled by an `editors` list in Streamlit secrets:

        editors = ["tedrickc@gmail.com", "someone@ipi-pipe.com"]

    If the secret is absent, everyone with access can edit (single-team
    mode — today's behavior). Local development is always unrestricted.
    """
    if not _RUNNING_ON_CLOUD:
        return True
    try:
        editors = [str(e).strip().lower() for e in st.secrets.get("editors", [])]
    except Exception:
        editors = []
    if not editors:
        return True
    return _viewer_email() in editors


# ---------------------------------------------------------------------------
# ETL refresh helpers
# ---------------------------------------------------------------------------

_ETL_DIR = Path(__file__).resolve().parent.parent / "ipi_consent_decree_etl"
_REFRESH_STAMP = _ETL_DIR / ".last_refresh"
_REFRESH_SCRIPT = _ETL_DIR / "refresh.sh"


def _get_last_refresh():
    """Read the last ETL refresh timestamp from the stamp file."""
    if _REFRESH_STAMP.exists():
        return _REFRESH_STAMP.read_text().strip()
    return None


def _run_etl_refresh() -> tuple[bool, str]:
    """Execute the ETL refresh pipeline and return (success, log_snippet)."""
    env = os.environ.copy()
    env["GOOGLE_APPLICATION_CREDENTIALS"] = str(_ETL_DIR / "service-account.json")
    try:
        result = subprocess.run(
            ["bash", str(_REFRESH_SCRIPT)],
            capture_output=True, text=True, timeout=1800, env=env,
        )
        # Write timestamp on success
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
        _REFRESH_STAMP.write_text(now_str)
        if result.returncode == 0:
            return True, result.stdout[-500:] if len(result.stdout) > 500 else result.stdout
        return False, result.stderr[-500:] if len(result.stderr) > 500 else result.stderr
    except subprocess.TimeoutExpired:
        return False, "ETL refresh timed out after 30 minutes."
    except Exception as e:
        return False, str(e)


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="IPI — Enforcement & Infrastructure Intelligence",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------

st.markdown("""
<style>
    /* KPI cards */
    .kpi-card {
        background: linear-gradient(135deg, #1A1F2B 0%, #252B3B 100%);
        border: 1px solid #333;
        border-radius: 12px;
        padding: 20px 24px;
        text-align: center;
    }
    .kpi-value {
        font-size: 2.2rem;
        font-weight: 700;
        margin: 0;
        line-height: 1.2;
    }
    .kpi-label {
        font-size: 0.85rem;
        color: #999;
        margin-top: 4px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .kpi-overdue { color: #FF4B4B; }
    .kpi-nearing { color: #FF6B35; }
    .kpi-prime { color: #00C853; }
    .kpi-high { color: #66BB6A; }
    .kpi-neutral { color: #FAFAFA; }
    .kpi-blue { color: #42A5F5; }

    /* Urgency badges */
    .badge {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 12px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
    }
    .badge-overdue { background: #FF4B4B22; color: #FF4B4B; border: 1px solid #FF4B4B44; }
    .badge-nearing-deadline { background: #FF6B3522; color: #FF6B35; border: 1px solid #FF6B3544; }
    .badge-late { background: #FFA72622; color: #FFA726; border: 1px solid #FFA72644; }
    .badge-moderate { background: #42A5F522; color: #42A5F5; border: 1px solid #42A5F544; }
    .badge-high { background: #66BB6A22; color: #66BB6A; border: 1px solid #66BB6A44; }
    .badge-prime { background: #00C85322; color: #00C853; border: 1px solid #00C85344; }
    .badge-unknown { background: #99999922; color: #999999; border: 1px solid #99999944; }
    .badge-monitoring { background: #7E57C222; color: #7E57C2; border: 1px solid #7E57C244; }

    /* Header */
    .main-header {
        font-size: 1.8rem;
        font-weight: 700;
        margin-bottom: 0;
    }
    .sub-header {
        font-size: 0.95rem;
        color: #888;
        margin-top: -8px;
        margin-bottom: 24px;
    }

    /* Hide Streamlit footer */
    footer { visibility: hidden; }

    /* Table styling */
    .dataframe { font-size: 0.85rem; }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def load_data(include_history: bool = False) -> pd.DataFrame:
    """Load enforcement data from BigQuery, ranked signal-first.

    V2 signal weighting: state enforcement actions are the LEADING
    indicator (rank 1) — state agencies act earlier in the enforcement
    lifecycle than EPA/DOJ. Federal consent decrees stay visible as the
    secondary signal (rank 2), other federal actions rank 3.

    By default closed/likely-closed records stay in BigQuery (roughly
    12k of 27k rows) — they're historical context, not leads. The
    sidebar toggle sets include_history=True to pull everything.
    """
    project_id = os.getenv("GCP_PROJECT_ID", "ipi-consent-decree-dashboard")

    client = _get_bigquery_client(project_id)
    history_clause = (
        "" if include_history
        else "WHERE COALESCE(case_status, 'Unknown') NOT IN ('Closed', 'Likely Closed')"
    )
    query = f"""
        SELECT case_number, facility_name, city, state, zip_code, county,
               consent_decree_date, compliance_end_date, lead_agency,
               enforcement_level, action_type, violation_type,
               penalty_amount, statute, pipe_infrastructure_flag,
               population, case_status, signal_type, signal_rank,
               latitude, longitude
        FROM `ipi_intelligence.consent_decrees`
        {history_clause}
        ORDER BY COALESCE(signal_rank, 5) ASC, penalty_amount DESC
    """
    df = client.query(query).to_dataframe()

    # Fallback signal classification for rows/environments where the
    # backfill hasn't run (derives from enforcement_level + action_type).
    if "signal_rank" not in df.columns:
        df["signal_rank"] = pd.NA
        df["signal_type"] = pd.NA
    missing = df["signal_rank"].isna()
    if missing.any():
        def _classify(row):
            level = str(row.get("enforcement_level") or "").lower()
            action = str(row.get("action_type") or "").lower()
            if level == "state":
                return "State Enforcement Action", 1
            if level == "federal":
                if any(t in action for t in (
                    "consent decree", "consent agreement/final order",
                    "civil judicial action",
                )):
                    return "Federal Consent Decree", 2
                return "Federal Enforcement Action", 3
            return "Unclassified", 5
        classified = df.loc[missing].apply(_classify, axis=1)
        df.loc[missing, "signal_type"] = classified.apply(lambda t: t[0])
        df.loc[missing, "signal_rank"] = classified.apply(lambda t: t[1])
    df["signal_rank"] = df["signal_rank"].astype("Int64")

    # Size tier (Layer 2) — always derived fresh from population so it can
    # never go stale vs. the materialized BigQuery column.
    def _size_tier(pop):
        if pd.isna(pop):
            return "Unknown"
        if pop < 100_000:
            return "Small"
        if pop < 500_000:
            return "Medium"
        return "Large"
    if "population" in df.columns:
        df["size_tier"] = df["population"].apply(_size_tier)
    else:
        df["size_tier"] = "Unknown"

    # Recalculate lifecycle stage from live dates (vectorized — same logic
    # as _sales_priority, ~100x faster than row-wise apply at this scale)
    if "compliance_end_date" in df.columns:
        df = _compute_lifecycle_stages(df)

    # DMR/QNCR rows are discovery-tier monitoring signals, not enforcement
    # actions — their recent signal dates must NOT score them as "prime"
    # leads. They get their own tier, excluded from Prime/High KPIs.
    df.loc[df["signal_rank"] == 4, "urgency_tier"] = "monitoring"

    # Default V2 ordering: signal strength first, then sales priority
    # recency tiers within a signal, then penalty size.
    _tier_order = {
        "prime": 0, "high": 1, "nearing deadline": 2, "overdue": 3,
        "late": 4, "moderate": 5, "unknown": 6, "monitoring": 7,
    }
    if "urgency_tier" in df.columns:
        df["_tier_sort"] = df["urgency_tier"].map(_tier_order).fillna(6)
        df = df.sort_values(
            by=["signal_rank", "_tier_sort", "penalty_amount"],
            ascending=[True, True, False],
        ).drop(columns="_tier_sort").reset_index(drop=True)

    return df


def _compute_lifecycle_stages(df: pd.DataFrame) -> pd.DataFrame:
    """Vectorized lifecycle-stage computation (see _sales_priority for the
    reference row-wise logic and tier rationale; the two must stay in sync —
    a parity test in the repo compares them)."""
    import numpy as np

    today = pd.Timestamp(date.today())
    end = pd.to_datetime(df["compliance_end_date"], errors="coerce")
    start = pd.to_datetime(df["consent_decree_date"], errors="coerce")

    # "Real" deadline: end exists AND (no start, or end is >1yr after start)
    has_deadline = end.notna() & (start.isna() | ((end - start).dt.days > 365))
    days_left = (end - today).dt.days.where(has_deadline)
    years_ago = (today - start).dt.days / 365.25

    df["days_to_deadline"] = (end - today).dt.days.astype("Int64")

    conditions = [
        days_left < 0,                                   # overdue
        days_left < 365,                                 # nearing deadline
        years_ago <= 1,                                  # prime
        (years_ago <= 2) | (days_left > 3650),           # high
        days_left <= 1825,                               # late
        (years_ago <= 5) | (days_left > 1825),           # moderate
        years_ago.notna(),                               # late (old, no deadline)
    ]
    choices = ["overdue", "nearing deadline", "prime", "high",
               "late", "moderate", "late"]
    df["urgency_tier"] = np.select(conditions, choices, default="unknown")
    return df


def _has_real_deadline(row) -> bool:
    """Check if a record has a meaningful compliance end date.

    Many ICIS records have end_date == start_date (placeholder data).
    A 'real' deadline means the end date is at least 1 year after start.
    """
    end = row.get("compliance_end_date")
    start = row.get("consent_decree_date")
    if pd.isna(end):
        return False
    if pd.notna(start):
        try:
            diff = (end - start).days if hasattr(end, 'days') else (end - start)
            if hasattr(diff, 'days'):
                diff = diff.days
            return int(diff) > 365
        except (TypeError, ValueError):
            return False
    # Has end date but no start date — trust it
    return True


def _sales_priority(row, today) -> str:
    """Compute sales priority tier for IPI lead scoring.

    IPI provides pre-construction inspection services, so municipalities
    EARLY in the consent decree lifecycle are the highest-priority leads.

    Tier logic (evaluated top-to-bottom, first match wins):
    1. OVERDUE:           has real deadline that has passed
    2. NEARING DEADLINE:  has real deadline < 1 yr away
    3. PRIME:             issued within the last year (recency wins over deadline proximity)
    4. HIGH:              issued 1–2 yrs ago OR 10+ yrs to deadline
    5. LATE:              has real deadline 1–5 yrs away
    6. MODERATE:          issued 2–5 yrs ago OR 5–10 yrs to deadline
    7. UNKNOWN:           no date information

    Note: PRIME and HIGH are checked BEFORE LATE because IPI prioritizes
    recently-issued consent decrees — a CD issued 6 months ago with a
    3-year deadline is a better lead than one issued 15 years ago with
    5 years left.
    """
    has_deadline = _has_real_deadline(row)
    days_left = None
    if has_deadline:
        d = row.get("days_to_deadline")
        if pd.notna(d):
            days_left = int(d)

    # --- Deadline-driven urgent tiers ---
    if days_left is not None:
        if days_left < 0:
            return "overdue"
        if days_left < 365:
            return "nearing deadline"

    # --- Recency-driven tiers (checked BEFORE "late" deadline tier) ---
    start = row.get("consent_decree_date")
    years_ago = None
    if pd.notna(start):
        try:
            years_ago = (today - start).days / 365.25
        except (TypeError, AttributeError):
            pass

    if years_ago is not None and years_ago <= 1:
        return "prime"

    # HIGH: issued 1-2 yrs ago OR 10+ yrs to deadline
    if years_ago is not None and years_ago <= 2:
        return "high"
    if days_left is not None and days_left > 3650:
        return "high"

    # LATE: has real deadline 1-5 years away (checked after PRIME/HIGH)
    if days_left is not None and days_left <= 1825:
        return "late"

    # MODERATE: issued 2-5 yrs ago OR 5-10 yrs to deadline
    if years_ago is not None and years_ago <= 5:
        return "moderate"
    if days_left is not None and days_left > 1825:
        return "moderate"

    # If we have a start date but it's older than 5 years and no useful deadline
    if years_ago is not None:
        return "late"

    return "unknown"


def urgency_badge(tier: str) -> str:
    """Return HTML badge for an urgency tier."""
    return f'<span class="badge badge-{tier}">{tier}</span>'


def format_currency(val) -> str:
    """Format a number as currency."""
    if pd.isna(val) or val == 0:
        return "—"
    return f"${val:,.0f}"


def format_population(val) -> str:
    """Format population with commas."""
    if pd.isna(val) or val == 0:
        return "—"
    return f"{int(val):,}"


@st.cache_data(ttl=3600)
def load_qualified_targets() -> pd.DataFrame:
    """Load the Layer 3a/4 municipality-grain target list, if exported.

    Rebuilt daily at 7:30am by the export job; queue/review actions in the
    app explicitly clear this cache, so a long TTL is safe."""
    project_id = os.getenv("GCP_PROJECT_ID", "ipi-consent-decree-dashboard")
    client = _get_bigquery_client(project_id)
    try:
        df = client.query("""
            SELECT q.municipality_key, q.city, q.state, q.size_tier,
                   q.best_signal_type, q.n_signals, q.n_state_actions,
                   q.n_federal_decrees, q.n_dmr, q.n_recent_incidents,
                   q.latest_signal_date, q.total_penalties, q.population,
                   q.priority_score, q.has_stakeholders,
                   rq.status AS queue_status,
                   COALESCE(s.n_pending, 0)  AS n_pending,
                   COALESCE(s.n_approved, 0) AS n_approved,
                   COALESCE(s.n_synced, 0)   AS n_synced
            FROM `ipi_intelligence.qualified_targets` q
            LEFT JOIN `ipi_intelligence.research_queue` rq
              USING (municipality_key)
            LEFT JOIN (
              SELECT municipality_key,
                     COUNTIF(hubspot_sync_status = 'pending')  AS n_pending,
                     COUNTIF(hubspot_sync_status = 'approved') AS n_approved,
                     COUNTIF(hubspot_sync_status = 'synced')   AS n_synced
              FROM `ipi_intelligence.stakeholders_staging`
              GROUP BY municipality_key
            ) s USING (municipality_key)
            ORDER BY priority_score DESC, total_penalties DESC
        """).to_dataframe()

        # One human-readable pipeline stage per municipality, most
        # actionable state wins.
        def _pipeline_status(r):
            if r.n_pending > 0:
                return "Review contacts"
            if r.n_approved > 0:
                return "Ready to sync"
            if r.n_synced > 0:
                return "In HubSpot"
            if r.queue_status == "researching":
                return "Researching"
            if r.queue_status == "queued":
                return "Queued"
            if r.queue_status == "done":
                return "Researched"
            return "—"
        df["pipeline_status"] = df.apply(_pipeline_status, axis=1)
        return df
    except Exception:
        return pd.DataFrame()  # table not exported yet


RESEARCH_QUEUE_DDL = """
CREATE TABLE IF NOT EXISTS `ipi_intelligence.research_queue` (
  municipality_key STRING NOT NULL,
  city STRING,
  state STRING,
  priority_score INT64,
  status STRING,          -- queued | researching | done
  queued_at TIMESTAMP
)
"""


def queue_for_research(selected: pd.DataFrame) -> int:
    """Write selected municipalities into the contact-research queue.
    Re-queueing an existing municipality resets it to 'queued'.
    Fully parameterized — no SQL built from strings."""
    project_id = os.getenv("GCP_PROJECT_ID", "ipi-consent-decree-dashboard")
    client = _get_bigquery_client(project_id)
    client.query(RESEARCH_QUEUE_DDL).result()

    from google.cloud import bigquery as bq
    row_params = [
        bq.StructQueryParameter(
            None,
            bq.ScalarQueryParameter("municipality_key", "STRING", str(r.municipality_key)),
            bq.ScalarQueryParameter("city", "STRING", str(r.city or "")),
            bq.ScalarQueryParameter("state", "STRING", str(r.state or "")),
            bq.ScalarQueryParameter("priority_score", "INT64", int(r.priority_score)),
        )
        for r in selected.itertuples()
    ]
    merge_sql = """
    MERGE `ipi_intelligence.research_queue` T
    USING (SELECT * FROM UNNEST(@rows)) S
    ON T.municipality_key = S.municipality_key
    WHEN MATCHED THEN UPDATE SET
      status = 'queued', queued_at = CURRENT_TIMESTAMP(),
      priority_score = S.priority_score
    WHEN NOT MATCHED THEN INSERT
      (municipality_key, city, state, priority_score, status, queued_at)
      VALUES (S.municipality_key, S.city, S.state, S.priority_score,
              'queued', CURRENT_TIMESTAMP())
    """
    job = client.query(
        merge_sql,
        job_config=bq.QueryJobConfig(query_parameters=[
            bq.ArrayQueryParameter("rows", "STRUCT", row_params),
        ]),
    )
    job.result()
    return job.num_dml_affected_rows or 0


@st.cache_data(ttl=60)
def load_research_queue() -> pd.DataFrame:
    project_id = os.getenv("GCP_PROJECT_ID", "ipi-consent-decree-dashboard")
    client = _get_bigquery_client(project_id)
    try:
        return client.query("""
            SELECT municipality_key, city, state, priority_score, status, queued_at
            FROM `ipi_intelligence.research_queue`
            ORDER BY status, priority_score DESC
        """).to_dataframe()
    except Exception:
        return pd.DataFrame()


def remove_from_queue(keys: list) -> int:
    """Delete municipalities from the research queue by key."""
    if not keys:
        return 0
    project_id = os.getenv("GCP_PROJECT_ID", "ipi-consent-decree-dashboard")
    client = _get_bigquery_client(project_id)
    from google.cloud import bigquery as bq
    job = client.query(
        "DELETE FROM `ipi_intelligence.research_queue` "
        "WHERE municipality_key IN UNNEST(@keys)",
        job_config=bq.QueryJobConfig(query_parameters=[
            bq.ArrayQueryParameter("keys", "STRING", [str(k) for k in keys]),
        ]),
    )
    job.result()
    return job.num_dml_affected_rows or 0


COWORK_QUEUE_PROMPT = """\
Research municipal contacts for IPI. Read the queue:
  SELECT * FROM `ipi-consent-decree-dashboard.ipi_intelligence.research_queue`
  WHERE status = 'queued' ORDER BY priority_score DESC
For each municipality find: water/utility director, mayor, city council or
board members (flag public-works/infrastructure/finance committees), city
manager or public works director, county commissioners if the utility is
county-run, and the district's state legislator(s). Every contact row needs
email and/or LinkedIn profile URL (email -> HubSpot Sequence, LinkedIn ->
HeyReach); a name with neither doesn't count. Sources in order: Ballotpedia,
the municipal website, Clay waterfall (low-confidence until spot-checked),
LinkedIn Sales Navigator to verify. Write rows to
`ipi_intelligence.stakeholders_staging` (stakeholder_id = new UUID,
municipality_key from the queue row, verified = FALSE, hubspot_sync_status =
'pending', ipi_audience_segment = 'State Representative' for political roles,
IPI's operational segment for water/utility staff). When a municipality is
fully researched, set its research_queue.status = 'done'. Full brief:
ipi_consent_decree_etl/LAYER3B_HANDOFF.md in the IPI Dashboard repo.\
"""


# ---------------------------------------------------------------------------
# Contact review — approve/reject researched stakeholders (the quality gate
# between Cowork research output and HubSpot)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=60)
def load_pending_stakeholders() -> pd.DataFrame:
    project_id = os.getenv("GCP_PROJECT_ID", "ipi-consent-decree-dashboard")
    client = _get_bigquery_client(project_id)
    try:
        return client.query("""
            SELECT stakeholder_id, city, state, full_name, role_title,
                   role_category, committee, email, phone, linkedin_url,
                   source, source_url, confidence
            FROM `ipi_intelligence.stakeholders_staging`
            WHERE hubspot_sync_status = 'pending'
            ORDER BY state, city, role_category
        """).to_dataframe()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=120)
def load_stakeholder_counts() -> dict:
    """Pipeline totals for the review section header."""
    project_id = os.getenv("GCP_PROJECT_ID", "ipi-consent-decree-dashboard")
    client = _get_bigquery_client(project_id)
    try:
        row = list(client.query("""
            SELECT
              COUNTIF(hubspot_sync_status = 'pending')  AS pending,
              COUNTIF(hubspot_sync_status = 'approved') AS approved,
              COUNTIF(hubspot_sync_status = 'synced')   AS synced,
              COUNTIF(hubspot_sync_status = 'rejected') AS rejected
            FROM `ipi_intelligence.stakeholders_staging`
        """).result())[0]
        return {"pending": row.pending, "approved": row.approved,
                "synced": row.synced, "rejected": row.rejected}
    except Exception:
        return {"pending": 0, "approved": 0, "synced": 0, "rejected": 0}


def apply_review_decisions(approve_ids: list, reject_ids: list) -> int:
    """Write review decisions back to staging (parameterized)."""
    project_id = os.getenv("GCP_PROJECT_ID", "ipi-consent-decree-dashboard")
    client = _get_bigquery_client(project_id)
    from google.cloud import bigquery as bq
    total = 0
    for ids, status, verified in (
        (approve_ids, "approved", True),
        (reject_ids, "rejected", False),
    ):
        if not ids:
            continue
        job = client.query(
            """
            UPDATE `ipi_intelligence.stakeholders_staging`
            SET hubspot_sync_status = @status,
                verified = @verified,
                updated_at = CURRENT_TIMESTAMP()
            WHERE stakeholder_id IN UNNEST(@ids)
              AND hubspot_sync_status = 'pending'
            """,
            job_config=bq.QueryJobConfig(query_parameters=[
                bq.ScalarQueryParameter("status", "STRING", status),
                bq.ScalarQueryParameter("verified", "BOOL", verified),
                bq.ArrayQueryParameter("ids", "STRING", [str(i) for i in ids]),
            ]),
        )
        job.result()
        total += job.num_dml_affected_rows or 0
    return total


def render_contact_review():
    """Approve/reject researched contacts before they reach HubSpot."""
    counts = load_stakeholder_counts()
    st.markdown("#### Contact Review")
    st.caption(
        f"**{counts['pending']} awaiting review** · "
        f"{counts['approved']} approved (ready to sync) · "
        f"{counts['synced']} in HubSpot · {counts['rejected']} rejected. "
        "Approved contacts sync to HubSpot tagged with municipality, signal, "
        "and priority score; rejected ones never leave this table."
    )

    pending = load_pending_stakeholders()
    if pending.empty:
        st.info(
            "Nothing to review. New contacts appear here after a Cowork "
            "research run finishes (they arrive as 'pending')."
        )
        return

    if not can_edit():
        st.caption("View-only access — an editor approves or rejects these.")
        st.dataframe(pending, use_container_width=True, height=380, hide_index=True)
        return

    review_df = pending.copy()
    review_df.insert(0, "approve", False)
    review_df.insert(1, "reject", False)

    edited = st.data_editor(
        review_df,
        column_order=[
            "approve", "reject", "city", "state", "full_name", "role_title",
            "role_category", "committee", "email", "phone", "linkedin_url",
            "source", "source_url", "confidence",
        ],
        column_config={
            "approve": st.column_config.CheckboxColumn("Approve"),
            "reject": st.column_config.CheckboxColumn("Reject"),
            "city": st.column_config.TextColumn("Municipality", disabled=True),
            "state": st.column_config.TextColumn("State", width="small", disabled=True),
            "full_name": st.column_config.TextColumn("Name", disabled=True),
            "role_title": st.column_config.TextColumn("Title", disabled=True),
            "role_category": st.column_config.TextColumn("Role", width="small", disabled=True),
            "committee": st.column_config.TextColumn("Committee", disabled=True),
            "email": st.column_config.TextColumn("Email", disabled=True),
            "phone": st.column_config.TextColumn("Phone", disabled=True),
            "linkedin_url": st.column_config.LinkColumn("LinkedIn", display_text="profile"),
            "source": st.column_config.TextColumn("Source", width="small", disabled=True),
            "source_url": st.column_config.LinkColumn("Source Link", display_text="open"),
            "confidence": st.column_config.TextColumn("Confidence", width="small", disabled=True),
        },
        use_container_width=True,
        height=380,
        hide_index=True,
        key="review_editor",
    )

    approve_ids = edited.loc[edited["approve"] & ~edited["reject"], "stakeholder_id"].tolist()
    reject_ids = edited.loc[edited["reject"] & ~edited["approve"], "stakeholder_id"].tolist()
    conflicted = int((edited["approve"] & edited["reject"]).sum())
    if conflicted:
        st.warning(f"{conflicted} row(s) have BOTH approve and reject ticked — they'll be skipped.")

    if st.button(
        f"Apply decisions ({len(approve_ids)} approve, {len(reject_ids)} reject)",
        disabled=not (approve_ids or reject_ids),
        type="primary",
    ):
        try:
            n = apply_review_decisions(approve_ids, reject_ids)
            load_pending_stakeholders.clear()
            load_stakeholder_counts.clear()
            load_qualified_targets.clear()
            st.success(f"Updated {n} contact(s).")
            st.rerun()
        except Exception:
            st.error("Couldn't save review decisions — check BigQuery access and try again.")


@st.cache_data(ttl=900)
def load_incidents() -> pd.DataFrame:
    """Load recent news incidents from the incident monitor (last 30 days).
    Feeds update daily; 15-min TTL keeps same-day manual runs visible."""
    project_id = os.getenv("GCP_PROJECT_ID", "ipi-consent-decree-dashboard")
    client = _get_bigquery_client(project_id)
    try:
        df = client.query("""
            SELECT published_at, city, state, incident_type, headline,
                   url, news_source
            FROM `ipi_intelligence.incident_reports`
            WHERE published_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
              AND NOT dismissed
            ORDER BY published_at DESC
            LIMIT 200
        """).to_dataframe()
        if not df.empty:
            # BigQuery returns UTC; display in Eastern so "tonight's" news
            # doesn't show tomorrow's date.
            df["published_at"] = (
                df["published_at"].dt.tz_convert("US/Eastern").dt.tz_localize(None)
            )
        return df
    except Exception:
        return pd.DataFrame()  # monitor hasn't run yet


@st.cache_data(ttl=900)
def load_freshness() -> dict:
    """Real freshness from BigQuery — trustworthy on cloud and local alike
    (the old .last_refresh stamp file only existed on the machine that ran
    the pipeline, and went stale)."""
    project_id = os.getenv("GCP_PROJECT_ID", "ipi-consent-decree-dashboard")
    client = _get_bigquery_client(project_id)
    try:
        row = list(client.query("""
            SELECT
              (SELECT MAX(last_updated)  FROM `ipi_intelligence.consent_decrees`)  AS enforcement,
              (SELECT MAX(first_seen_at) FROM `ipi_intelligence.incident_reports`) AS incidents,
              (SELECT MAX(exported_at)   FROM `ipi_intelligence.qualified_targets`) AS targets
        """).result())[0]
        fmt = lambda t: (pd.Timestamp(t).tz_convert("US/Eastern")
                         .strftime("%b %d, %I:%M %p ET").lstrip("0")) if t else "never"
        return {"enforcement": fmt(row.enforcement),
                "incidents": fmt(row.incidents),
                "targets": fmt(row.targets)}
    except Exception:
        return {"enforcement": "unknown", "incidents": "unknown", "targets": "unknown"}


def render_incidents(incidents: pd.DataFrame):
    """Live incident feed — the 'reach out ASAP' trigger list."""
    st.markdown("#### Live Incidents — Last 30 Days")
    st.caption(
        "News-monitored sewer overflows, boil-water advisories, and water-"
        "infrastructure incidents across Medium/Large qualified targets. "
        "An incident here + a contact in HubSpot = same-week outreach window. "
        "Municipalities with incidents get +10 priority score."
    )
    if incidents.empty:
        st.info(
            "No incidents on record yet — the monitor runs with each data "
            "refresh (or cron `incident_monitor.py` daily for same-day alerts)."
        )
        return
    st.dataframe(
        incidents,
        column_config={
            "published_at": st.column_config.DatetimeColumn("Published (ET)", format="MMM DD, HH:mm"),
            "city": st.column_config.TextColumn("Municipality"),
            "state": st.column_config.TextColumn("State", width="small"),
            "incident_type": st.column_config.TextColumn("Type", width="small"),
            "headline": st.column_config.TextColumn("Headline", width="large"),
            "url": st.column_config.LinkColumn("Link", display_text="open"),
            "news_source": st.column_config.TextColumn("Source"),
        },
        use_container_width=True,
        height=380,
        hide_index=True,
    )


def render_lead_kpis(targets: pd.DataFrame, counts: dict):
    """Municipality-grain KPIs — the numbers that matter for lead gen."""
    total = len(targets)
    large = int((targets["size_tier"] == "Large").sum()) if total else 0
    with_incidents = int((targets["n_recent_incidents"] > 0).sum()) if total else 0
    in_flight = int(
        targets["pipeline_status"].isin(
            ["Queued", "Researching", "Review contacts", "Ready to sync"]
        ).sum()
    ) if "pipeline_status" in targets.columns and total else 0
    in_hubspot = int((targets["n_synced"] > 0).sum()) if "n_synced" in targets.columns and total else 0

    cols = st.columns(6)
    cards = [
        (f"{total}", "Qualified Targets", "kpi-neutral"),
        (f"{large}", "Large (500k+)", "kpi-blue"),
        (f"{with_incidents}", "Live Incidents", "kpi-nearing"),
        (f"{in_flight}", "Research In Flight", "kpi-high"),
        (f"{counts['pending']}", "Contacts To Review", "kpi-overdue" if counts['pending'] else "kpi-neutral"),
        (f"{in_hubspot}", "In HubSpot", "kpi-prime"),
    ]
    for col, (val, lbl, cls) in zip(cols, cards):
        with col:
            st.markdown(f"""
            <div class="kpi-card">
                <p class="kpi-value {cls}">{val}</p>
                <p class="kpi-label">{lbl}</p>
            </div>""", unsafe_allow_html=True)


def render_top_targets(targets: pd.DataFrame):
    """Render the V2 priority-scored municipality ranking (Layer 4)."""
    st.markdown("#### Top Priority Targets — Municipality Ranking")
    st.caption(
        "Composite score: signal strength (state action 40 > federal decree 30 "
        "> other federal 20 > DMR 10) + signal volume + size tier + recency "
        "+ pipe-infrastructure flag + stakeholder reachability. "
        "One row per qualified municipality (Medium/Large with an active signal)."
    )
    if targets.empty:
        st.info(
            "Qualified target list not exported yet — run "
            "`python export_targets.py` in the ETL directory (or a full refresh)."
        )
        return

    st.caption(
        "Tick rows, then click **Queue for contact research** — queued "
        "municipalities flow to the Cowork research run (prompt below), and "
        "researched contacts land in HubSpot after review."
    )

    # Lead-list filters (scoped to this table; the sidebar governs the
    # record-grain views on the other tabs)
    fcol1, fcol2, fcol3, fcol4 = st.columns([2, 1.5, 1.5, 1.2])
    with fcol1:
        search = st.text_input(
            "Search municipality", "", placeholder="e.g. Houston",
            key="target_search",
        )
    with fcol2:
        state_sel = st.multiselect(
            "State", sorted(targets["state"].dropna().unique().tolist()),
            default=[], placeholder="All states", key="target_states",
        )
    with fcol3:
        size_sel = st.multiselect(
            "Size", ["Large", "Medium"], default=[],
            placeholder="All sizes", key="target_sizes",
        )
    with fcol4:
        incidents_only = st.checkbox("Live incidents only", key="target_incidents")

    if search:
        targets = targets[targets["city"].fillna("").str.contains(search, case=False)]
    if state_sel:
        targets = targets[targets["state"].isin(state_sel)]
    if size_sel:
        targets = targets[targets["size_tier"].isin(size_sel)]
    if incidents_only:
        targets = targets[targets["n_recent_incidents"] > 0]
    if targets.empty:
        st.info("No targets match these filters.")
        return

    event = st.dataframe(
        targets,
        column_order=[
            "city", "state", "pipeline_status", "size_tier",
            "best_signal_type", "n_signals",
            "n_state_actions", "n_federal_decrees", "n_dmr",
            "n_recent_incidents", "latest_signal_date", "total_penalties",
            "population", "priority_score", "has_stakeholders",
        ],
        column_config={
            "city": st.column_config.TextColumn("Municipality"),
            "state": st.column_config.TextColumn("State", width="small"),
            "pipeline_status": st.column_config.TextColumn("Pipeline", width="medium"),
            "size_tier": st.column_config.TextColumn("Size", width="small"),
            "best_signal_type": st.column_config.TextColumn("Best Signal"),
            "n_signals": st.column_config.NumberColumn("Signals", format="%d"),
            "n_state_actions": st.column_config.NumberColumn("State", format="%d"),
            "n_federal_decrees": st.column_config.NumberColumn("Fed CD", format="%d"),
            "n_dmr": st.column_config.NumberColumn("DMR", format="%d"),
            "n_recent_incidents": st.column_config.NumberColumn("Incidents 90d", format="%d"),
            "latest_signal_date": st.column_config.DateColumn("Latest Signal", format="YYYY-MM-DD"),
            "total_penalties": st.column_config.NumberColumn("Penalties", format="$%,.0f"),
            "population": st.column_config.NumberColumn("Population", format="%,d"),
            "priority_score": st.column_config.ProgressColumn(
                "Priority Score", min_value=0, max_value=120, format="%d"
            ),
            "has_stakeholders": st.column_config.CheckboxColumn("Contacts"),
        },
        use_container_width=True,
        height=520,
        hide_index=True,
        on_select="rerun",
        selection_mode="multi-row",
        key="targets_table",
    )

    selected_rows = event.selection.rows if event and event.selection else []
    col_a, col_b = st.columns([1, 2])
    with col_a:
        if st.button(
            f"Queue {len(selected_rows)} selected for contact research",
            disabled=not selected_rows or not can_edit(),
            type="primary",
            help=None if can_edit() else "View-only access — ask an editor to queue targets",
        ):
            picked = targets.iloc[selected_rows]
            try:
                queue_for_research(picked)
                load_research_queue.clear()
                load_qualified_targets.clear()  # pipeline_status shows queue state
                st.success(
                    f"Queued: {', '.join(picked['city'].fillna('?').tolist())}"
                )
            except Exception:
                st.error("Couldn't write to the research queue — check BigQuery access and try again.")
    with col_b:
        csv = targets.drop(columns=["municipality_key"]).to_csv(index=False)
        st.download_button(
            label="Download full target list as CSV",
            data=csv,
            file_name=f"ipi_qualified_targets_{date.today().isoformat()}.csv",
            mime="text/csv",
        )

    queue = load_research_queue()
    with st.expander(
        f"Contact research queue ({len(queue)} municipalities) + Cowork prompt",
        expanded=False,
    ):
        if not queue.empty:
            st.caption("Tick queue rows and click Remove to take them off the list.")
            queue_event = st.dataframe(
                queue,
                column_order=["city", "state", "priority_score", "status", "queued_at"],
                column_config={
                    "city": st.column_config.TextColumn("Municipality"),
                    "state": st.column_config.TextColumn("State", width="small"),
                    "priority_score": st.column_config.NumberColumn("Score", format="%d"),
                    "status": st.column_config.TextColumn("Status", width="small"),
                    "queued_at": st.column_config.DatetimeColumn("Queued", format="MMM DD, HH:mm"),
                },
                use_container_width=True,
                height=220,
                hide_index=True,
                on_select="rerun",
                selection_mode="multi-row",
                key="queue_table",
            )
            q_selected = (queue_event.selection.rows
                          if queue_event and queue_event.selection else [])
            rm_col, clear_col = st.columns(2)
            with rm_col:
                if st.button(
                    f"Remove {len(q_selected)} selected from queue",
                    disabled=not q_selected or not can_edit(),
                ):
                    picked = queue.iloc[q_selected]
                    try:
                        n = remove_from_queue(picked["municipality_key"].tolist())
                        load_research_queue.clear()
                        load_qualified_targets.clear()
                        st.success(f"Removed {n} from queue")
                        st.rerun()
                    except Exception:
                        st.error("Couldn't remove from the queue — check BigQuery access and try again.")
            with clear_col:
                if st.button("Clear entire queue", disabled=not can_edit()):
                    try:
                        n = remove_from_queue(queue["municipality_key"].tolist())
                        load_research_queue.clear()
                        load_qualified_targets.clear()
                        st.success(f"Removed {n} from queue")
                        st.rerun()
                    except Exception:
                        st.error("Couldn't clear the queue — check BigQuery access and try again.")
        st.markdown("**Paste this into Cowork to run the research:**")
        st.code(COWORK_QUEUE_PROMPT, language=None)


# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

def render_record_filters(df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    """Record-grain filters, rendered at the top of the Map & Analytics tab
    (the All Enforcement Data tab inherits the same selection).

    Returns (filtered_df, size_tiers_for_ranked_list). The size-tier
    selection is returned separately because it applies ONLY to the data
    tab's ranked table — the map always shows all sizes.
    """
    st.markdown("#### Record Filters")

    row1 = st.columns([1.2, 1.2, 1.2, 1.2])
    with row1[0]:
        if "case_status" in df.columns:
            status_options = sorted(df["case_status"].dropna().unique().tolist())
        else:
            status_options = ["Active", "Closed", "Likely Closed", "Unknown"]
        selected_status = st.multiselect(
            "Case Status",
            options=status_options,
            default=["Active"] if "Active" in status_options else [],
            help="Active = confirmed open enforcement action | Likely Closed = state action older than 5 years",
        )
    with row1[1]:
        _signal_order = [
            "State Enforcement Action", "Federal Consent Decree",
            "Federal Enforcement Action", "DMR Violation", "Unclassified",
        ]
        present_signals = (
            df["signal_type"].dropna().unique().tolist()
            if "signal_type" in df.columns else []
        )
        signal_options = [s for s in _signal_order if s in present_signals] or _signal_order
        selected_signals = st.multiselect(
            "Signal Type",
            options=signal_options,
            default=[],
            placeholder="All signals",
            help="State actions = leading indicator | Federal consent decrees = "
                 "secondary signal | DMR Violation = monitoring-only discovery tier",
        )
    with row1[2]:
        states = sorted(df["state"].dropna().unique().tolist())
        selected_states = st.multiselect(
            "States & Territories",
            options=states,
            default=[],
            placeholder="All states & territories",
        )
    with row1[3]:
        size_options = ["Large", "Medium", "Small", "Unknown"]
        selected_sizes = st.multiselect(
            "Size Tier (data tab)",
            options=size_options,
            default=["Large", "Medium"],
            help="Small < 100k | Medium 100k-500k | Large 500k+ (service-area "
                 "population). Applies to the data tab's table; the map always "
                 "shows all sizes. 'Unknown' = no population data on record.",
        )

    pipe_only = st.checkbox(
        "Pipe infrastructure only",
        value=False,
        help="Show only records flagged for sewer/collection system/pipe infrastructure issues "
             "(SSO, CSO, pipeline, wastewater, POTW, etc.)",
    )

    with st.expander("Advanced filters"):
        if "action_type" in df.columns:
            action_types = sorted(df["action_type"].dropna().unique().tolist())
        else:
            action_types = []

        LARGE_PROJECT_TYPES = [
            "Consent Decree",
            "Civil Judicial Action",
            "State Administrative Order of Consent",
            "State CWA Penalty AO",
            "FDEP Consent Order",
            "Georgia EPD Consent Order",
            "Georgia EPD Administrative Order",
            "Regional Water Board CAO",
            "NJDEP Administrative Consent Order",
            "PA DEP Consent Order",
            "IDEM Consent Order",
            "IDEM Agreed Order",
            "Ohio EPA Consent Order",
            "Ohio EPA Director's Final Findings & Orders",
            "WV DEP Consent Order",
            "TDEC Consent Order",
            "ADEM Consent Order",
            "EGLE Administrative Consent Order",
            "TCEQ Agreed Order",
            "MDEQ Administrative Order",
            "PA DEP Administrative Order",
            "State Water Board CDO",
        ]
        LEADING_INDICATOR_TYPES = [
            "Administrative Compliance Order",
        ]

        preset_col1, preset_col2, preset_col3 = st.columns(3)
        with preset_col1:
            if st.button("Large Projects", help="Consent decrees + state consent/penalty orders + civil judicial actions", use_container_width=True):
                st.session_state["_action_type_sel"] = [t for t in LARGE_PROJECT_TYPES if t in action_types]
        with preset_col2:
            if st.button("Leading Indicators", help="Federal ACOs — often precede consent decrees", use_container_width=True):
                st.session_state["_action_type_sel"] = [t for t in LEADING_INDICATOR_TYPES if t in action_types]
        with preset_col3:
            if st.button("Clear", help="Reset action type filter", use_container_width=True):
                st.session_state["_action_type_sel"] = []

        selected_action_types = st.multiselect(
            "Action Type",
            options=action_types,
            key="_action_type_sel",
            placeholder="All action types",
        )

        recently_issued = st.checkbox(
            "Recently issued only (≤ 1 year)",
            value=False,
            help="Show only enforcement actions issued since "
                 + (date.today() - timedelta(days=365)).strftime("%B %Y"),
        )

        if "consent_decree_date" in df.columns and len(df) > 0:
            valid_years = df["consent_decree_date"].dropna().apply(lambda d: d.year)
            min_year = int(valid_years.min()) if len(valid_years) > 0 else 1990
            max_year = int(valid_years.max()) if len(valid_years) > 0 else date.today().year
        else:
            min_year = 1990
            max_year = date.today().year
        year_range = st.slider(
            "Action year range",
            min_value=min_year,
            max_value=max_year,
            value=(min_year, max_year),
        )

        urgency_options = ["prime", "high", "moderate", "late",
                           "nearing deadline", "overdue", "unknown", "monitoring"]
        selected_urgency = st.multiselect(
            "Lifecycle Stage",
            options=urgency_options,
            default=[],
            placeholder="All priorities",
        )

        max_penalty = int(df["penalty_amount"].max()) if len(df) > 0 else 1000000
        penalty_range = st.slider(
            "Minimum penalty ($)",
            min_value=0,
            max_value=max_penalty,
            value=0,
            step=100000,
            format="$%d",
        )

    # Apply filters
    filtered = df.copy()

    # Case status filter
    if selected_status and "case_status" in filtered.columns:
        filtered = filtered[filtered["case_status"].isin(selected_status)]

    # Recently issued filter (≤ 1 year)
    if recently_issued and "consent_decree_date" in filtered.columns:
        cutoff = date.today() - timedelta(days=365)
        filtered = filtered[
            filtered["consent_decree_date"].apply(
                lambda d: d >= cutoff if pd.notna(d) else False
            )
        ]

    if selected_action_types and "action_type" in filtered.columns:
        filtered = filtered[filtered["action_type"].isin(selected_action_types)]

    if selected_signals and "signal_type" in filtered.columns:
        filtered = filtered[filtered["signal_type"].isin(selected_signals)]

    if "consent_decree_date" in filtered.columns and not recently_issued:
        filtered = filtered[
            filtered["consent_decree_date"].apply(
                lambda d: year_range[0] <= d.year <= year_range[1]
                if pd.notna(d) else True
            )
        ]

    if selected_states:
        filtered = filtered[filtered["state"].isin(selected_states)]

    if selected_urgency:
        filtered = filtered[filtered["urgency_tier"].isin(selected_urgency)]

    if pipe_only:
        if "pipe_infrastructure_flag" in filtered.columns:
            filtered = filtered[filtered["pipe_infrastructure_flag"].fillna(False) == True]
        else:
            filtered = filtered.iloc[0:0]  # empty — column missing

    filtered = filtered[filtered["penalty_amount"] >= penalty_range]

    # Inline summary — state first (V2 primary signal)
    federal_count = len(filtered[filtered.get("enforcement_level", pd.Series()) == "Federal"]) if "enforcement_level" in filtered.columns else 0
    state_count = len(filtered[filtered.get("enforcement_level", pd.Series()) == "State"]) if "enforcement_level" in filtered.columns else 0
    st.caption(
        f"Showing **{len(filtered):,}** of {len(df):,} records · "
        f"State: **{state_count:,}** | Federal: **{federal_count:,}**"
    )

    return filtered, selected_sizes


def render_sidebar_utilities():
    """Sidebar keeps only global utilities: freshness, reload, data quality."""
    # --- Data Refresh (local only — ETL can't run on Streamlit Cloud) ---
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Data Refresh")
    fresh = load_freshness()
    st.sidebar.caption(
        f"Enforcement data: **{fresh['enforcement']}**  \n"
        f"Incidents: **{fresh['incidents']}**  \n"
        f"Target list: **{fresh['targets']}**"
    )

    if st.sidebar.button(
        "Reload data now",
        help="Clears the app's data cache and re-reads everything from "
             "BigQuery. Use after a manual pipeline run; otherwise data "
             "auto-refreshes within an hour.",
    ):
        st.cache_data.clear()
        st.rerun()

    if not _RUNNING_ON_CLOUD:
        if st.sidebar.button("Refresh EPA Data", help="Downloads latest EPA bulk files, reloads BigQuery, and applies deadline corrections. Takes ~5-15 minutes."):
            with st.sidebar.status("Refreshing EPA data...", expanded=True) as status:
                st.write("Downloading latest EPA bulk files...")
                st.write("This may take 5-15 minutes. Please keep this tab open.")
                success, log = _run_etl_refresh()
                if success:
                    status.update(label="Refresh complete!", state="complete")
                    st.write(f"Finished at {_get_last_refresh()}")
                    st.cache_data.clear()
                else:
                    status.update(label="Refresh failed", state="error")
                    st.code(log, language="text")

    # --- Data Quality ---
    _render_data_quality_badge()


def _render_data_quality_badge():
    """Show a data quality summary from the latest validation report."""
    report_path = _ETL_DIR / "validation_report.json"
    if not report_path.exists():
        return
    try:
        import json
        with open(report_path) as f:
            report = json.load(f)
        high = report.get("high", 0)
        med = report.get("medium", 0)
        total = report.get("total_issues", 0)
        ts = report.get("timestamp", "")[:16]
        if high > 0:
            st.sidebar.warning(f"Data quality: {high} issues need review ({ts})")
        elif med > 0:
            st.sidebar.info(f"Data quality: {med} items to review ({ts})")
        else:
            st.sidebar.success(f"Data quality: all checks passed ({ts})")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# KPI Section
# ---------------------------------------------------------------------------

def render_kpis(df: pd.DataFrame):
    """Render the top KPI cards.

    "Active Enforcement Actions" = all records in the filtered set.
    These include both federal consent decrees and state administrative
    orders that have NOT yet been formally terminated by a court/agency.
    A decree remains "active" even when repair work is underway — it is
    only removed after all terms are satisfied and the court terminates it.
    """
    total = len(df)
    total_penalty = df["penalty_amount"].sum()

    # Count by sales priority tier
    urgency_lower = df["urgency_tier"].str.lower()
    prime = int((urgency_lower == "prime").sum())
    high = int((urgency_lower == "high").sum())
    overdue = int((urgency_lower == "overdue").sum())

    # Federal vs State breakdown
    federal = int((df.get("enforcement_level", pd.Series()) == "Federal").sum()) if "enforcement_level" in df.columns else total
    state_level = int((df.get("enforcement_level", pd.Series()) == "State").sum()) if "enforcement_level" in df.columns else 0

    states_count = df["state"].nunique()

    cols = st.columns(6)

    with cols[0]:
        st.markdown(f"""
        <div class="kpi-card">
            <p class="kpi-value kpi-neutral">{total}</p>
            <p class="kpi-label">Records In View</p>
        </div>""", unsafe_allow_html=True)

    with cols[1]:
        st.markdown(f"""
        <div class="kpi-card">
            <p class="kpi-value kpi-neutral"><span class="kpi-blue">{state_level}</span> / {federal}</p>
            <p class="kpi-label">State (Primary) / Federal</p>
        </div>""", unsafe_allow_html=True)

    with cols[2]:
        st.markdown(f"""
        <div class="kpi-card">
            <p class="kpi-value kpi-prime">{prime}</p>
            <p class="kpi-label">Prime-Stage Records</p>
        </div>""", unsafe_allow_html=True)

    with cols[3]:
        st.markdown(f"""
        <div class="kpi-card">
            <p class="kpi-value kpi-high">{prime + high}</p>
            <p class="kpi-label">Prime + High Records</p>
        </div>""", unsafe_allow_html=True)

    with cols[4]:
        penalty_display = f"${total_penalty / 1e9:.2f}B" if total_penalty >= 1e9 else f"${total_penalty / 1e6:.1f}M"
        st.markdown(f"""
        <div class="kpi-card">
            <p class="kpi-value kpi-blue">{penalty_display}</p>
            <p class="kpi-label">Total Penalties</p>
        </div>""", unsafe_allow_html=True)

    with cols[5]:
        st.markdown(f"""
        <div class="kpi-card">
            <p class="kpi-value kpi-neutral">{states_count}</p>
            <p class="kpi-label">States & Territories</p>
        </div>""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Urgency Tier Legend
# ---------------------------------------------------------------------------

def render_sales_priority_key():
    """Render a compact legend explaining each sales priority tier."""
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #1A1F2B 0%, #252B3B 100%);
        border: 1px solid #333;
        border-radius: 10px;
        padding: 14px 20px 10px 20px;
        margin-top: 4px;
        margin-bottom: 8px;
    ">
        <div style="display: flex; flex-wrap: wrap; gap: 14px; align-items: center; justify-content: center;">
            <span style="font-size: 0.8rem; color: #999; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; margin-right: 4px;">Lifecycle Stage</span>
            <span style="display: inline-flex; align-items: center; gap: 5px;">
                <span style="width: 10px; height: 10px; border-radius: 50%; background: #00C853; display: inline-block;"></span>
                <span style="font-size: 0.78rem; color: #00C853; font-weight: 600;">PRIME</span>
                <span style="font-size: 0.72rem; color: #999;">Issued ≤ 1 yr ago</span>
            </span>
            <span style="display: inline-flex; align-items: center; gap: 5px;">
                <span style="width: 10px; height: 10px; border-radius: 50%; background: #66BB6A; display: inline-block;"></span>
                <span style="font-size: 0.78rem; color: #66BB6A; font-weight: 600;">HIGH</span>
                <span style="font-size: 0.72rem; color: #999;">Issued 1–2 yrs ago or 10+ yrs to deadline</span>
            </span>
            <span style="display: inline-flex; align-items: center; gap: 5px;">
                <span style="width: 10px; height: 10px; border-radius: 50%; background: #42A5F5; display: inline-block;"></span>
                <span style="font-size: 0.78rem; color: #42A5F5; font-weight: 600;">MODERATE</span>
                <span style="font-size: 0.72rem; color: #999;">Issued 2–5 yrs ago or 5–10 yrs to deadline</span>
            </span>
            <span style="display: inline-flex; align-items: center; gap: 5px;">
                <span style="width: 10px; height: 10px; border-radius: 50%; background: #FFA726; display: inline-block;"></span>
                <span style="font-size: 0.78rem; color: #FFA726; font-weight: 600;">LATE</span>
                <span style="font-size: 0.72rem; color: #999;">1–5 yrs to deadline or issued 5+ yrs ago</span>
            </span>
            <span style="display: inline-flex; align-items: center; gap: 5px;">
                <span style="width: 10px; height: 10px; border-radius: 50%; background: #FF6B35; display: inline-block;"></span>
                <span style="font-size: 0.78rem; color: #FF6B35; font-weight: 600;">NEARING DEADLINE</span>
                <span style="font-size: 0.72rem; color: #999;">&lt; 1 yr to deadline</span>
            </span>
            <span style="display: inline-flex; align-items: center; gap: 5px;">
                <span style="width: 10px; height: 10px; border-radius: 50%; background: #FF4B4B; display: inline-block;"></span>
                <span style="font-size: 0.78rem; color: #FF4B4B; font-weight: 600;">OVERDUE</span>
                <span style="font-size: 0.72rem; color: #999;">Past deadline</span>
            </span>
            <span style="display: inline-flex; align-items: center; gap: 5px;">
                <span style="width: 10px; height: 10px; border-radius: 50%; background: #999999; display: inline-block;"></span>
                <span style="font-size: 0.78rem; color: #999999; font-weight: 600;">UNKNOWN</span>
                <span style="font-size: 0.72rem; color: #999;">No date info</span>
            </span>
            <span style="display: inline-flex; align-items: center; gap: 5px;">
                <span style="width: 10px; height: 10px; border-radius: 50%; background: #7E57C2; display: inline-block;"></span>
                <span style="font-size: 0.78rem; color: #7E57C2; font-weight: 600;">MONITORING</span>
                <span style="font-size: 0.72rem; color: #999;">DMR violations only — no enforcement action yet</span>
            </span>
        </div>
        <p style="font-size: 0.7rem; color: #777; text-align: center; margin: 8px 0 2px 0; line-height: 1.4;">
            Priority uses court-ordered deadline when available, otherwise years since issuance.
            Recently issued = early planning/procurement = highest opportunity for IPI pre-construction services.
        </p>
    </div>
    """, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------

def render_map(df: pd.DataFrame):
    """Render an interactive map of consent decree locations."""
    map_df = df.dropna(subset=["latitude", "longitude"]).copy()
    if map_df.empty:
        st.info("No records with coordinates to display on map.")
        return

    # Color by sales priority tier
    color_map = {
        "prime": "#00C853",
        "high": "#66BB6A",
        "moderate": "#42A5F5",
        "late": "#FFA726",
        "nearing deadline": "#FF6B35",
        "overdue": "#FF4B4B",
        "unknown": "#999999",
        "monitoring": "#7E57C2",
    }
    map_df["color"] = map_df["urgency_tier"].map(color_map).fillna("#999999")
    map_df["size"] = map_df["penalty_amount"].apply(
        lambda p: max(8, min(30, p / 500000)) if pd.notna(p) and p > 0 else 8
    )
    map_df["hover_text"] = map_df.apply(
        lambda r: (
            f"<b>{r['facility_name']}</b><br>"
            f"{r['city']}, {r['state']}<br>"
            f"Signal: {r.get('signal_type') or '—'}<br>"
            f"Penalty: {format_currency(r['penalty_amount'])}<br>"
            f"Stage: {r['urgency_tier'].upper()}<br>"
            f"Population: {format_population(r.get('population', 0))}"
            f" ({r.get('size_tier', 'Unknown')})"
        ),
        axis=1,
    )

    fig = go.Figure()

    # Render in order: prime first (most important), then descending
    for tier in ["prime", "high", "moderate", "late", "nearing deadline", "overdue", "unknown", "monitoring"]:
        tier_df = map_df[map_df["urgency_tier"] == tier]
        if tier_df.empty:
            continue
        fig.add_trace(go.Scattergeo(
            lon=tier_df["longitude"],
            lat=tier_df["latitude"],
            text=tier_df["hover_text"],
            hoverinfo="text",
            marker=dict(
                size=tier_df["size"],
                color=color_map.get(tier, "#999"),
                opacity=0.8,
                line=dict(width=1, color="#333"),
            ),
            name=tier.upper(),
            showlegend=False,  # Legend is rendered separately below the map
        ))

    fig.update_geos(
        scope="usa",
        showland=True,
        landcolor="#1A1F2B",
        showlakes=True,
        lakecolor="#0E1117",
        showcountries=False,
        bgcolor="#0E1117",
        showframe=False,
        coastlinecolor="#333",
        showsubunits=True,
        subunitcolor="#333",
    )

    fig.update_layout(
        height=500,
        margin=dict(l=0, r=0, t=10, b=0),
        paper_bgcolor="#0E1117",
        plot_bgcolor="#0E1117",
        font_color="#FAFAFA",
        showlegend=False,
        geo=dict(
            lonaxis_range=[-180, -64],
            lataxis_range=[17, 72],
        ),
    )

    st.plotly_chart(fig, use_container_width=True, config={
        "scrollZoom": True,
        "modeBarButtonsToRemove": ["select2d", "lasso2d"],
        "modeBarPosition": "bottomright",
    })


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------

def render_charts(df: pd.DataFrame):
    """Render analytics charts."""
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Records by Lifecycle Stage")
        urgency_counts = df["urgency_tier"].value_counts().reindex(
            ["prime", "high", "moderate", "late", "nearing deadline", "overdue", "unknown", "monitoring"],
            fill_value=0,
        )
        colors = ["#00C853", "#66BB6A", "#42A5F5", "#FFA726", "#FF6B35", "#FF4B4B", "#999999", "#7E57C2"]
        fig = go.Figure(go.Bar(
            x=urgency_counts.index.str.upper(),
            y=urgency_counts.values,
            marker_color=colors,
            text=urgency_counts.values,
            textposition="outside",
        ))
        fig.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=20, b=40),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#FAFAFA",
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor="#222"),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("#### Top 10 States by Penalty Amount")
        state_penalties = (
            df.groupby("state")["penalty_amount"]
            .sum()
            .sort_values(ascending=True)
            .tail(10)
        )
        fig = go.Figure(go.Bar(
            x=state_penalties.values,
            y=state_penalties.index,
            orientation="h",
            marker_color="#0066CC",
            text=[format_currency(v) for v in state_penalties.values],
            textposition="outside",
        ))
        fig.update_layout(
            height=350,
            margin=dict(l=40, r=80, t=20, b=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#FAFAFA",
            xaxis=dict(showgrid=True, gridcolor="#222", tickformat="$,.0f"),
            yaxis=dict(showgrid=False),
        )
        st.plotly_chart(fig, use_container_width=True)



# ---------------------------------------------------------------------------
# Data Table
# ---------------------------------------------------------------------------

def render_table(df: pd.DataFrame):
    """Render the detailed data table.

    Default order is signal-first (state enforcement > federal consent
    decree > other federal), then sales-priority recency, then penalty.
    """
    st.markdown("#### Enforcement Action Details")
    st.caption(
        "Ranked signal-first: state enforcement actions lead (earlier in the "
        "enforcement lifecycle), federal consent decrees follow as the "
        "secondary signal."
    )

    display_cols = [
        "facility_name", "city", "state", "signal_type", "size_tier",
        "case_status", "urgency_tier", "consent_decree_date",
        "compliance_end_date", "days_to_deadline", "penalty_amount",
        "lead_agency", "population", "action_type",
        "pipe_infrastructure_flag",
    ]
    available_cols = [c for c in display_cols if c in df.columns]
    display_df = df[available_cols].copy()

    # Format for display
    col_config = {
        "facility_name": st.column_config.TextColumn("Municipality / Facility", width="large"),
        "city": st.column_config.TextColumn("City"),
        "state": st.column_config.TextColumn("State", width="small"),
        "signal_type": st.column_config.TextColumn("Signal", width="medium"),
        "size_tier": st.column_config.TextColumn("Size", width="small"),
        "case_status": st.column_config.TextColumn("Status", width="small"),
        "urgency_tier": st.column_config.TextColumn("Stage", width="small"),
        "consent_decree_date": st.column_config.DateColumn("Decree Date", format="YYYY-MM-DD"),
        "compliance_end_date": st.column_config.DateColumn("Deadline", format="YYYY-MM-DD"),
        "days_to_deadline": st.column_config.NumberColumn("Days Left", format="%d"),
        "penalty_amount": st.column_config.NumberColumn("Penalty", format="$%,.0f"),
        "lead_agency": st.column_config.TextColumn("Lead Agency"),
        "population": st.column_config.NumberColumn("Population", format="%,d"),
        "action_type": st.column_config.TextColumn("Action Type"),
        "pipe_infrastructure_flag": st.column_config.CheckboxColumn("Pipe Flag"),
    }

    st.dataframe(
        display_df,
        column_config=col_config,
        use_container_width=True,
        height=500,
        hide_index=True,
    )

    # Download button
    csv = df.to_csv(index=False)
    st.download_button(
        label="Download filtered data as CSV",
        data=csv,
        file_name=f"ipi_consent_decrees_{date.today().isoformat()}.csv",
        mime="text/csv",
    )


# ---------------------------------------------------------------------------
# Data Sources
# ---------------------------------------------------------------------------

def render_data_sources():
    """Render an expandable data sources and methodology section."""
    with st.expander("Data Sources & Methodology"):
        st.markdown("""
## Signal Hierarchy (V2)

Records are ranked **signal-first** in the default view:

| Rank | Signal | Why |
|------|--------|-----|
| 1 | **State Enforcement Action** | Leading indicator — state agencies act earlier in the enforcement lifecycle. A state consent/penalty order often lands years before a federal decree, while remediation scope is still undefined. |
| 2 | **Federal Consent Decree** | Secondary signal — confirmed, court-ordered programs. Larger but later; by decree entry, engineering contracts are often scoped. |
| 3 | **Federal Enforcement Action** | Other federal orders (ACOs, penalty orders) — precursor signals. |
| 4 | **DMR Violation** | Near-real-time: facilities accumulating effluent (DMR) violations in EPA's Quarterly Noncompliance Report, with no enforcement action yet. Earliest possible signal — often 1-3 years ahead of a state order. |

Within each signal tier, records sort by sales-priority recency (PRIME first), then penalty.

---

## Quick Start — Finding Leads

This dashboard helps IPI identify municipalities under EPA/state enforcement that are likely
to need underground pipe infrastructure inspection services. Here's how to use it:

1. **Enable "Pipe infrastructure only"** in the sidebar to focus on municipal water/wastewater
   infrastructure cases (filters out industrial facilities, private properties, etc.).
2. **Set Case Status to "Active"** to see only current enforcement actions.
3. **Use a preset filter button** (see below) to select the right action types for your search.
4. **Sort by Lifecycle Stage** — PRIME and HIGH records are municipalities early in their
   compliance lifecycle, before remediation work begins. These are IPI's best opportunities.
5. **Filter by state** to focus on a target region, or leave blank to see the national picture.

---

## Preset Filter Buttons

Two preset buttons above the Action Type filter help you quickly select the right
combination of enforcement action types:

**Large Projects** — Selects consent decrees, state consent orders, civil judicial actions,
and state penalty administrative orders. These represent confirmed, enforceable compliance
programs where municipalities must invest in infrastructure improvements. This is the
broadest useful filter for finding IPI opportunities. Includes:
- Federal Consent Decrees (court-ordered, typically 10-30 year programs)
- Civil Judicial Actions (federal court enforcement)
- State Consent Orders and Agreed Orders (state-level equivalents by name — FDEP Consent
  Order, TCEQ Agreed Order, Ohio EPA Consent Order, etc.)
- State CWA Penalty Administrative Orders (state enforcement with financial penalties for
  Clean Water Act violations — indicates active compliance issues)

**Leading Indicators** — Selects Administrative Compliance Orders (ACOs) only. ACOs are
federal EPA administrative orders that often **precede** a consent decree. When EPA issues
an ACO, it signals that a municipality is under federal scrutiny and may soon face a
consent decree requiring major infrastructure work. These are early-stage leads — useful
for getting ahead of the competition, but the scope of required work may not yet be defined.

**Clear** — Resets the Action Type filter to show all enforcement action types.

**Tip**: Use "Large Projects" for your primary prospecting. Switch to "Leading Indicators"
periodically to spot municipalities that may be heading toward consent decrees in the
near future. A municipality that appears under "Leading Indicators" today may appear
under "Large Projects" in 6-18 months.

---

## Lifecycle Stage Tiers

Priority tiers are designed around IPI's sales cycle. IPI provides pre-construction
inspection services, so municipalities **early** in the consent decree lifecycle — before
remediation work begins — are the highest-value leads.

Tiers are evaluated top-to-bottom (first match wins):

| Tier | Criteria | What It Means for IPI |
|------|----------|-----------------------|
| **PRIME** | Issued within the last year | **Best leads** — early in lifecycle, pre-construction phase |
| **HIGH** | Issued 1-2 years ago, OR 10+ years to deadline | Strong leads — still in planning/procurement |
| **NEARING DEADLINE** | Less than 1 year to deadline | Urgent — construction likely underway or imminent |
| **OVERDUE** | Compliance deadline has passed | May need emergency/accelerated work |
| **LATE** | 1-5 years to deadline, or issued 5+ years ago | Construction likely underway; some opportunity may remain |
| **MODERATE** | Issued 2-5 years ago, OR 5-10 years to deadline | Mid-lifecycle — limited opportunity |
| **UNKNOWN** | No issuance date or deadline on record | Needs research before outreach |

**Why this order matters**: PRIME and HIGH are checked before deadline-driven tiers because
IPI's value is highest at the **start** of the consent decree lifecycle. A recently issued
consent decree is a better lead than an older one approaching its deadline — even if the
older one feels more "urgent." By the time a deadline is near, construction contracts are
typically already awarded.

**About compliance deadlines**: Many EPA records contain placeholder dates (end date = start
date). These are treated as "no real deadline" and the system falls back to recency of
issuance. Verified court-ordered deadlines are sourced from DOJ/EPA press releases.

---

## Pipe Infrastructure Filter

When **"Pipe infrastructure only"** is enabled, records are filtered to show only municipal
underground water/wastewater infrastructure cases. This is a two-step process:

**Step 1 — Include** records whose facility name, case name, or enforcement description
mentions pipe/water infrastructure:
- Sewer overflows (SSO, CSO), collection systems, interceptors, trunk lines
- Pipeline, conveyance, inflow & infiltration (I&I)
- Treatment plants (WWTP, WWTF, WPCF, POTW, STP, AWTF)
- Water reclamation, recycling, and pollution control facilities
- Municipal storm sewers (MS4), stormwater systems
- Sanitary districts, sewer authorities, public works
- Wastewater lagoons and treatment lagoons

**Step 2 — Exclude** non-municipal facilities (even if they match Step 1):
- Industrial/corporate (refineries, steel mills, food processors, chemical plants)
- Camps, resorts, hotels, lodges
- Private subdivisions, mobile home parks, HOAs
- Marinas, farms, ranches, commercial properties

**Municipal override**: Facilities with "City of," "Town of," "County," "District,"
"Authority," or "Municipal" in their name are always included regardless of other keywords.

When the filter is **off**, the dashboard shows all enforcement actions including industrial
and non-infrastructure cases.

---

## Action Types Explained

| Action Type | Level | Typical Duration | Significance |
|-------------|-------|------------------|--------------|
| **Consent Decree** | Federal (court-ordered) | 10-30 years | Most significant — large-scale, long-term infrastructure programs |
| **Civil Judicial Action** | Federal (court) | Varies | Federal court enforcement action |
| **Administrative Compliance Order (ACO)** | Federal (EPA admin) | 1-3 years | Often a precursor to a consent decree |
| **State Consent Orders** | State | 3-15 years | State-level equivalent of consent decrees (varies by state agency) |
| **State CWA Penalty AO** | State | 1-5 years | State penalty order for Clean Water Act violations — indicates active compliance issues |
| **State CWA Non Penalty AO** | State | 1-3 years | Corrective order without financial penalty |

State-specific order names vary by agency (e.g., "TCEQ Agreed Order" in Texas,
"FDEP Consent Order" in Florida, "Ohio EPA Consent Order" in Ohio). These are all
included in the "Large Projects" preset.

---

## Penalty Amounts

- Amounts reflect **civil penalties only** — not total capital investment or supplemental
  environmental project (SEP) costs. A consent decree requiring $500M in infrastructure
  work may show only a $1M civil penalty.
- Federal penalties are verified against DOJ/EPA press releases.
- State penalties come from NPDES formal enforcement records.

---

## Data Sources & Freshness

**Federal**: EPA ICIS-FE&C bulk download files — all federal CWA and SDWA enforcement actions.
Court-ordered deadlines supplemented from DOJ/EPA press releases.

**State**: EPA NPDES bulk download files — state-level formal enforcement from all 50 states
and US territories.

**Population**: US Census Bureau American Community Survey (ACS) estimates.

**Freshness**: EPA updates bulk files weekly (typically Monday). Use the **"Refresh EPA Data"**
button in the sidebar to download the latest files. After each refresh, automated validation
checks for placeholder deadlines, suspect penalties, and non-municipal facility flags.
        """)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """Main dashboard layout."""
    # Header
    st.markdown(
        '<p class="main-header">IPI — Enforcement &amp; Infrastructure Intelligence</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<p class="sub-header">'
        'Lead intelligence — state enforcement actions (leading indicator) '
        'and federal consent decrees (secondary signal) for municipalities '
        'likely to need infrastructure assessment and funding enablement'
        '</p>',
        unsafe_allow_html=True,
    )

    # Load data — active records by default; historical toggle pulls all
    include_history = st.sidebar.toggle(
        "Include closed/historical records",
        value=False,
        help="Adds ~12k closed and likely-closed enforcement records to the "
             "Map & Data views. Slower to load; not needed for lead work.",
    )
    with st.spinner("Loading data from BigQuery..."):
        df = load_data(include_history)

    if df.empty:
        st.warning(
            "No enforcement data found in BigQuery. "
            "Run the pipeline first: `bash refresh.sh` in the ETL directory."
        )
        return

    # Sidebar: global utilities only — filters live on the tabs they govern
    render_sidebar_utilities()

    tab_leads, tab_map, tab_records = st.tabs(
        ["Lead Pipeline", "Map & Analytics", "All Enforcement Data"]
    )

    # --- TAB 1: the lead workflow (find -> qualify -> research -> review) ---
    with tab_leads:
        targets = load_qualified_targets()
        render_lead_kpis(targets, load_stakeholder_counts())
        st.markdown("---")
        render_top_targets(targets)
        st.markdown("---")
        render_contact_review()
        st.markdown("---")
        render_incidents(load_incidents())

    # --- TAB 2: geographic + analytical views; owns the record filters ---
    with tab_map:
        filtered, selected_sizes = render_record_filters(df)
        # Size filter applies to the data tab's ranked list — the map keeps
        # all sizes visible (small municipalities deprioritized, not deleted).
        if selected_sizes and "size_tier" in filtered.columns:
            ranked = filtered[filtered["size_tier"].isin(selected_sizes)]
        else:
            ranked = filtered

        if filtered.empty:
            st.info("No records match the current filters.")
        else:
            st.markdown("---")
            render_kpis(ranked)
            st.markdown("")
            st.markdown("#### Enforcement Map (all municipality sizes)")
            render_map(filtered)
            render_sales_priority_key()
            st.caption(
                "Map shows continental US, Alaska, Hawaii, and Caribbean territories. "
                "Pacific territory data (GU, AS, MP) is included in the data tab. "
                "The map ignores the size-tier filter; charts and the data tab apply it."
            )
            st.markdown("---")
            render_charts(ranked)

    # --- TAB 3: the full record-grain table for analysts ---
    with tab_records:
        st.caption(
            "This table follows the **Record Filters set on the Map & "
            "Analytics tab** (including the size-tier selection)."
        )
        if filtered.empty:
            st.info("No records match the current filters.")
        else:
            hidden = len(filtered) - len(ranked)
            if hidden > 0:
                st.caption(
                    f"Size filter active: {hidden:,} records outside "
                    f"{', '.join(selected_sizes)} tiers are hidden from this list "
                    "(still shown on the map)."
                )
            render_table(ranked)
            st.markdown("---")
            render_data_sources()

    # Footer
    st.markdown("---")
    fresh = load_freshness()
    st.markdown(
        f"<p style='text-align:center; color:#666; font-size:0.8rem;'>"
        f"Enforcement data: {fresh['enforcement']} | Incidents: {fresh['incidents']} | "
        f"Dashboard loaded at {pd.Timestamp.now(tz='US/Eastern').strftime('%Y-%m-%d %I:%M %p ET')}"
        f"</p>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
