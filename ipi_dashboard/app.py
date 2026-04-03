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
_RUNNING_ON_CLOUD = os.getenv("STREAMLIT_SHARING_MODE") or hasattr(st, "secrets") and "gcp_service_account" in st.secrets


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
    page_title="IPI Pipe — Consent Decree Intelligence",
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

@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    """Load consent decree data from BigQuery."""
    project_id = os.getenv("GCP_PROJECT_ID", "ipi-consent-decree-dashboard")

    client = _get_bigquery_client(project_id)
    query = """
        SELECT *
        FROM `ipi_intelligence.consent_decrees`
        ORDER BY penalty_amount DESC
    """
    df = client.query(query).to_dataframe()

    # Recalculate sales priority from live dates
    if "compliance_end_date" in df.columns:
        today = date.today()
        df["days_to_deadline"] = df["compliance_end_date"].apply(
            lambda d: (d - today).days if pd.notna(d) else None
        )
        df["urgency_tier"] = df.apply(
            lambda r: _sales_priority(r, today), axis=1
        )

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


# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

def render_sidebar(df: pd.DataFrame) -> pd.DataFrame:
    """Render sidebar filters and return filtered DataFrame."""
    st.sidebar.markdown("## Filters")

    # Case status filter — default to Active only
    st.sidebar.markdown("### Case Status")
    if "case_status" in df.columns:
        status_options = sorted(df["case_status"].dropna().unique().tolist())
    else:
        status_options = ["Active", "Closed", "Likely Closed", "Unknown"]
    selected_status = st.sidebar.multiselect(
        "Case Status",
        options=status_options,
        default=["Active"] if "Active" in status_options else [],
        help="Active = confirmed open enforcement action | Likely Closed = state action older than 5 years",
    )

    # Pipe infrastructure filter — key for IPI's business
    pipe_only = st.sidebar.checkbox(
        "Pipe infrastructure only",
        value=False,
        help="Show only records flagged for sewer/collection system/pipe infrastructure issues "
             "(SSO, CSO, pipeline, wastewater, POTW, etc.)",
    )

    # Action type filter
    st.sidebar.markdown("### Action Type")
    if "action_type" in df.columns:
        action_types = sorted(df["action_type"].dropna().unique().tolist())
    else:
        action_types = []

    # Preset buttons
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

    preset_col1, preset_col2, preset_col3 = st.sidebar.columns(3)
    with preset_col1:
        if st.button("Large Projects", help="Consent decrees + state consent/penalty orders + civil judicial actions — confirmed enforcement programs requiring infrastructure work", use_container_width=True):
            st.session_state["_action_type_sel"] = [t for t in LARGE_PROJECT_TYPES if t in action_types]
    with preset_col2:
        if st.button("Leading Indicators", help="Federal Administrative Compliance Orders — often precede consent decrees; useful for identifying early-stage opportunities", use_container_width=True):
            st.session_state["_action_type_sel"] = [t for t in LEADING_INDICATOR_TYPES if t in action_types]
    with preset_col3:
        if st.button("Clear", help="Reset action type filter to show all", use_container_width=True):
            st.session_state["_action_type_sel"] = []

    selected_action_types = st.sidebar.multiselect(
        "Action Type",
        options=action_types,
        key="_action_type_sel",
        placeholder="All action types",
        help="Consent Decree = court-ordered, long-term compliance agreement | "
             "ACO = EPA administrative order (may escalate to consent decree) | "
             "State AO = state-level administrative orders & penalties",
    )

    # Recently Issued toggle
    recently_issued = st.sidebar.checkbox(
        "Recently issued only (≤ 1 year)",
        value=False,
        help="Show only enforcement actions issued since "
             + (date.today() - timedelta(days=365)).strftime("%B %Y"),
    )

    # Enforcement level filter (Federal vs State)
    st.sidebar.markdown("### Enforcement Level")
    enf_levels = sorted(
        df["enforcement_level"].dropna().unique().tolist()
    ) if "enforcement_level" in df.columns else []
    selected_levels = st.sidebar.multiselect(
        "Enforcement Level",
        options=enf_levels if enf_levels else ["Federal", "State", "Joint"],
        default=[],
        placeholder="All levels",
        help="Federal = EPA/DOJ consent decrees | State = state agency orders",
    )

    # Date range filter
    st.sidebar.markdown("### Date Range")
    if "consent_decree_date" in df.columns and len(df) > 0:
        valid_years = df["consent_decree_date"].dropna().apply(lambda d: d.year)
        min_year = int(valid_years.min()) if len(valid_years) > 0 else 1990
        max_year = int(valid_years.max()) if len(valid_years) > 0 else date.today().year
    else:
        min_year = 1990
        max_year = date.today().year
    year_range = st.sidebar.slider(
        "Consent decree year range",
        min_value=min_year,
        max_value=max_year,
        value=(min_year, max_year),
    )

    # State filter
    states = sorted(df["state"].dropna().unique().tolist())
    selected_states = st.sidebar.multiselect(
        "States & Territories",
        options=states,
        default=[],
        placeholder="All states & territories",
    )

    # Sales priority filter
    urgency_options = ["prime", "high", "moderate", "late", "nearing deadline", "overdue", "unknown"]
    selected_urgency = st.sidebar.multiselect(
        "Sales Priority",
        options=urgency_options,
        default=[],
        placeholder="All priorities",
    )

    # Penalty range
    st.sidebar.markdown("### Penalty Amount")
    max_penalty = int(df["penalty_amount"].max()) if len(df) > 0 else 1000000
    penalty_range = st.sidebar.slider(
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

    if selected_levels and "enforcement_level" in filtered.columns:
        filtered = filtered[filtered["enforcement_level"].isin(selected_levels)]

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

    # Summary in sidebar
    st.sidebar.markdown("---")
    federal_count = len(filtered[filtered.get("enforcement_level", pd.Series()) == "Federal"]) if "enforcement_level" in filtered.columns else 0
    state_count = len(filtered[filtered.get("enforcement_level", pd.Series()) == "State"]) if "enforcement_level" in filtered.columns else 0
    st.sidebar.markdown(
        f"**Showing {len(filtered)}** of {len(df)} records\n\n"
        f"Federal: **{federal_count}** | State: **{state_count}**"
    )

    # --- Data Refresh (local only — ETL can't run on Streamlit Cloud) ---
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Data Refresh")
    last_refresh = _get_last_refresh()
    if last_refresh:
        st.sidebar.caption(f"Last refreshed: **{last_refresh}**")
    else:
        st.sidebar.caption("Data loaded from BigQuery")

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

    return filtered


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
            <p class="kpi-label">Enforcement Actions</p>
        </div>""", unsafe_allow_html=True)

    with cols[1]:
        st.markdown(f"""
        <div class="kpi-card">
            <p class="kpi-value kpi-neutral">{federal} / {state_level}</p>
            <p class="kpi-label">Federal / State</p>
        </div>""", unsafe_allow_html=True)

    with cols[2]:
        st.markdown(f"""
        <div class="kpi-card">
            <p class="kpi-value kpi-prime">{prime}</p>
            <p class="kpi-label">Prime Leads</p>
        </div>""", unsafe_allow_html=True)

    with cols[3]:
        st.markdown(f"""
        <div class="kpi-card">
            <p class="kpi-value kpi-high">{prime + high}</p>
            <p class="kpi-label">Prime + High</p>
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
            <span style="font-size: 0.8rem; color: #999; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; margin-right: 4px;">Sales Priority</span>
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
    }
    map_df["color"] = map_df["urgency_tier"].map(color_map).fillna("#999999")
    map_df["size"] = map_df["penalty_amount"].apply(
        lambda p: max(8, min(30, p / 500000)) if pd.notna(p) and p > 0 else 8
    )
    map_df["hover_text"] = map_df.apply(
        lambda r: (
            f"<b>{r['facility_name']}</b><br>"
            f"{r['city']}, {r['state']}<br>"
            f"Penalty: {format_currency(r['penalty_amount'])}<br>"
            f"Priority: {r['urgency_tier'].upper()}<br>"
            f"Population: {format_population(r.get('population', 0))}"
        ),
        axis=1,
    )

    fig = go.Figure()

    # Render in order: prime first (most important), then descending
    for tier in ["prime", "high", "moderate", "late", "nearing deadline", "overdue", "unknown"]:
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
        st.markdown("#### Decrees by Sales Priority")
        urgency_counts = df["urgency_tier"].value_counts().reindex(
            ["prime", "high", "moderate", "late", "nearing deadline", "overdue", "unknown"],
            fill_value=0,
        )
        colors = ["#00C853", "#66BB6A", "#42A5F5", "#FFA726", "#FF6B35", "#FF4B4B", "#999999"]
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
    """Render the detailed data table."""
    st.markdown("#### Enforcement Action Details")

    display_cols = [
        "facility_name", "city", "state", "enforcement_level",
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
        "enforcement_level": st.column_config.TextColumn("Level", width="small"),
        "case_status": st.column_config.TextColumn("Status", width="small"),
        "urgency_tier": st.column_config.TextColumn("Priority", width="small"),
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
## Quick Start — Finding Leads

This dashboard helps IPI identify municipalities under EPA/state enforcement that are likely
to need underground pipe infrastructure inspection services. Here's how to use it:

1. **Enable "Pipe infrastructure only"** in the sidebar to focus on municipal water/wastewater
   infrastructure cases (filters out industrial facilities, private properties, etc.).
2. **Set Case Status to "Active"** to see only current enforcement actions.
3. **Use a preset filter button** (see below) to select the right action types for your search.
4. **Sort by Sales Priority** — PRIME and HIGH leads are municipalities early in their
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

## Sales Priority Tiers

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
        '<p class="main-header">IPI Pipe — Consent Decree Intelligence</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<p class="sub-header">'
        'Sales intelligence dashboard — municipalities under CWA/SDWA consent '
        'decrees likely to need underground pipe infrastructure services'
        '</p>',
        unsafe_allow_html=True,
    )

    # Load data
    with st.spinner("Loading data from BigQuery..."):
        df = load_data()

    if df.empty:
        st.warning(
            "No consent decree data found in BigQuery. "
            "Run the ETL first: `python etl.py --state TX`"
        )
        return

    # Sidebar filters
    filtered = render_sidebar(df)

    if filtered.empty:
        st.info("No records match the current filters. Try adjusting the sidebar filters.")
        return

    # KPI cards
    render_kpis(filtered)
    st.markdown("")

    # Map
    st.markdown("---")
    st.markdown("#### Consent Decree Map")
    render_map(filtered)
    render_sales_priority_key()
    st.caption(
        "Map shows continental US, Alaska, Hawaii, and Caribbean territories. "
        "Pacific territory data (GU, AS, MP) is included in the table below."
    )

    # Charts
    st.markdown("---")
    render_charts(filtered)

    # Data table
    st.markdown("---")
    render_table(filtered)

    # Data sources
    st.markdown("---")
    render_data_sources()

    # Footer
    st.markdown("---")
    last_refresh = _get_last_refresh() or "never"
    st.markdown(
        f"<p style='text-align:center; color:#666; font-size:0.8rem;'>"
        f"EPA data last refreshed: {last_refresh} | "
        f"Dashboard loaded at {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
        f"</p>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
