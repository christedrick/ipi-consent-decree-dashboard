"""
Layer 3a (V2): Qualified target-list export + stakeholder staging schema.

Produces two BigQuery tables:

1. `ipi_intelligence.qualified_targets` — municipality-grain export of every
   municipality that clears the size filter (Medium/Large, i.e. service
   population >= 100k) and carries at least one active signal. This is the
   hand-off artifact for stakeholder research (Layer 3b, run separately in
   Cowork: Ballotpedia for top-100 cities, Clay/Sales Navigator waterfall
   for the rest — Texas first as the pilot batch).

   Rebuilt from scratch on every run (CREATE OR REPLACE) — it's a derived
   view of consent_decrees, never hand-edited.

   Includes priority_score (Layer 4 composite). Components:
     - signal strength (best signal rank): state action 40 > federal CD 30
       > federal other 20 > DMR-only 10
     - signal volume bonus: min(n_signals, 5) * 4  (persistent attention)
     - size tier: Large 20, Medium 10
     - recency: signal in last 12mo +15, last 24mo +8
     - pipe infrastructure flag: +10
     - stakeholder reachability: +15 when the municipality has verified or
       approved contacts in stakeholders_staging (self-updates as Layer 3b
       research lands)
     - funding angle: +0 placeholder — SRF/BRIC eligibility enrichment is
       phase-2; column structure is in place so the score formula is stable

2. `ipi_intelligence.stakeholders_staging` — the landing spot for Layer 3b
   research output. DECISION (per scope doc): stakeholder data lands in a
   staging table FIRST, not directly in HubSpot — Clay's government-contact
   coverage is unproven, so rows carry hubspot_sync_status
   (pending -> approved -> synced / rejected) and only approved rows get
   pushed by the Layer 5 HubSpot sync. Created if missing, never replaced
   (it accumulates research output).

Usage:
    python export_targets.py            # rebuild qualified_targets + ensure staging
    python export_targets.py --min-tier Small   # include small municipalities
"""

import argparse
import os
import sys

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()


QUALIFIED_TARGETS_SQL = """
CREATE OR REPLACE TABLE `{project}.ipi_intelligence.qualified_targets` AS
WITH active AS (
  SELECT *
  FROM `{project}.ipi_intelligence.consent_decrees`
  WHERE case_status = 'Active'
    AND state IS NOT NULL AND state != ''
),
keyed AS (
  SELECT
    CONCAT(
      COALESCE(NULLIF(LOWER(TRIM(city)), ''), LOWER(TRIM(facility_name))),
      '|', state
    ) AS municipality_key,
    *
  FROM active
),
grouped AS (
  SELECT
    municipality_key,
    -- Prefer the most common non-empty city spelling; fall back to facility name
    ARRAY_AGG(NULLIF(TRIM(city), '') IGNORE NULLS ORDER BY last_updated DESC LIMIT 1)[SAFE_OFFSET(0)] AS city,
    ANY_VALUE(state) AS state,
    ARRAY_AGG(NULLIF(TRIM(county), '') IGNORE NULLS LIMIT 1)[SAFE_OFFSET(0)] AS county,
    ARRAY_AGG(facility_name IGNORE NULLS ORDER BY penalty_amount DESC LIMIT 1)[SAFE_OFFSET(0)] AS primary_facility,
    MAX(population) AS population,
    MIN(COALESCE(signal_rank, 5)) AS best_signal_rank,
    COUNT(*) AS n_signals,
    COUNTIF(signal_rank = 1) AS n_state_actions,
    COUNTIF(signal_rank = 2) AS n_federal_decrees,
    COUNTIF(signal_rank = 3) AS n_federal_other,
    COUNTIF(signal_rank = 4) AS n_dmr,
    MAX(consent_decree_date) AS latest_signal_date,
    SUM(COALESCE(penalty_amount, 0)) AS total_penalties,
    LOGICAL_OR(COALESCE(pipe_infrastructure_flag, FALSE)) AS pipe_flagged,
    ARRAY_AGG(DISTINCT action_type IGNORE NULLS LIMIT 10) AS action_types,
    AVG(latitude) AS latitude,
    AVG(longitude) AS longitude
  FROM keyed
  GROUP BY municipality_key
),
reachability AS (
  -- Approved/verified stakeholder contacts per municipality (Layer 3b output).
  -- Feeds the reachability component so the score self-updates as research lands.
  SELECT municipality_key, COUNT(*) AS n_contacts
  FROM `{project}.ipi_intelligence.stakeholders_staging`
  WHERE verified OR hubspot_sync_status IN ('approved', 'synced')
  GROUP BY municipality_key
),
scored AS (
  SELECT
    grouped.*,
    COALESCE(reachability.n_contacts, 0) AS n_stakeholder_contacts,
    CASE
      WHEN population IS NULL THEN 'Unknown'
      WHEN population < 100000 THEN 'Small'
      WHEN population < 500000 THEN 'Medium'
      ELSE 'Large'
    END AS size_tier,
    CASE best_signal_rank
      WHEN 1 THEN 'State Enforcement Action'
      WHEN 2 THEN 'Federal Consent Decree'
      WHEN 3 THEN 'Federal Enforcement Action'
      WHEN 4 THEN 'DMR Violation'
      ELSE 'Unclassified'
    END AS best_signal_type,
    -- Layer 4 composite priority score (see module docstring for weights)
    (
      CASE best_signal_rank
        WHEN 1 THEN 40 WHEN 2 THEN 30 WHEN 3 THEN 20 WHEN 4 THEN 10 ELSE 0
      END
      + LEAST(COUNT_SIGNALS_PLACEHOLDER, 5) * 4
      + CASE
          WHEN population >= 500000 THEN 20
          WHEN population >= 100000 THEN 10
          ELSE 0
        END
      + CASE
          WHEN latest_signal_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) THEN 15
          WHEN latest_signal_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH) THEN 8
          ELSE 0
        END
      + IF(pipe_flagged, 10, 0)
      + IF(COALESCE(reachability.n_contacts, 0) > 0, 15, 0)  -- reachability
      + 0  -- funding angle: phase-2 SRF/BRIC enrichment
    ) AS priority_score
  FROM grouped
  LEFT JOIN reachability USING (municipality_key)
)
SELECT
  municipality_key, city, state, county, primary_facility,
  population, size_tier,
  best_signal_rank, best_signal_type,
  n_signals, n_state_actions, n_federal_decrees, n_federal_other, n_dmr,
  latest_signal_date, total_penalties, pipe_flagged, action_types,
  latitude, longitude,
  priority_score,
  n_stakeholder_contacts,
  n_stakeholder_contacts > 0 AS has_stakeholders,
  CURRENT_TIMESTAMP() AS exported_at
FROM scored
WHERE size_tier IN ({tier_list})
ORDER BY priority_score DESC, total_penalties DESC
"""

STAKEHOLDERS_STAGING_DDL = """
CREATE TABLE IF NOT EXISTS `{project}.ipi_intelligence.stakeholders_staging` (
  stakeholder_id STRING NOT NULL,          -- uuid, assigned by researcher/pipeline
  municipality_key STRING NOT NULL,        -- FK -> qualified_targets.municipality_key
  city STRING,
  state STRING,
  full_name STRING NOT NULL,
  role_title STRING,                       -- e.g. "Mayor", "Council Member, District 4"
  role_category STRING,                    -- mayor | council | city_manager |
                                           -- public_works | finance | county_commissioner |
                                           -- state_legislator | other
  committee STRING,                        -- public works / infrastructure / finance, if any
  email STRING,
  phone STRING,
  linkedin_url STRING,
  source STRING,                           -- ballotpedia | clay | linkedin_sales_nav |
                                           -- municipal_website | manual
  source_url STRING,
  confidence STRING,                       -- high | medium | low (researcher-assessed)
  verified BOOL DEFAULT FALSE,             -- human-reviewed before HubSpot push
  research_notes STRING,
  ipi_audience_segment STRING DEFAULT 'State Representative',  -- confirmed V2 value
  hubspot_sync_status STRING DEFAULT 'pending',  -- pending | approved | synced | rejected
  hubspot_contact_id STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
"""


def main():
    parser = argparse.ArgumentParser(description="Layer 3a target export")
    parser.add_argument(
        "--min-tier", choices=["Large", "Medium", "Small"], default="Medium",
        help="Smallest size tier to include (default Medium => Medium+Large)",
    )
    args = parser.parse_args()

    tiers = {"Large": ["Large"],
             "Medium": ["Large", "Medium"],
             "Small": ["Large", "Medium", "Small"]}[args.min_tier]
    tier_list = ", ".join(f"'{t}'" for t in tiers)

    project_id = os.getenv("GCP_PROJECT_ID", "ipi-consent-decree-dashboard")
    client = bigquery.Client(project=project_id)

    # Staging must exist BEFORE the export — the reachability CTE joins it
    client.query(STAKEHOLDERS_STAGING_DDL.format(project=project_id)).result()

    # SQL placeholder for signal-volume inside aggregate scope
    sql = QUALIFIED_TARGETS_SQL.replace(
        "COUNT_SIGNALS_PLACEHOLDER", "n_signals"
    ).format(project=project_id, tier_list=tier_list)
    client.query(sql).result()

    count_q = f"""
    SELECT size_tier, COUNT(*) AS n, ROUND(AVG(priority_score), 1) AS avg_score
    FROM `{project_id}.ipi_intelligence.qualified_targets`
    GROUP BY size_tier ORDER BY avg_score DESC
    """
    print("qualified_targets rebuilt:")
    total = 0
    for row in client.query(count_q).result():
        print(f"  {row.size_tier:8} | {row.n:,} municipalities | avg score {row.avg_score}")
        total += row.n
    print(f"  TOTAL    | {total:,}")

    print("stakeholders_staging ready (created if missing; existing rows preserved)")

    top_q = f"""
    SELECT city, state, size_tier, best_signal_type, n_signals,
           priority_score, total_penalties
    FROM `{project_id}.ipi_intelligence.qualified_targets`
    ORDER BY priority_score DESC LIMIT 10
    """
    print("\nTop 10 targets by priority score:")
    for r in client.query(top_q).result():
        print(f"  {r.priority_score:>3} | {(r.city or '?'):22} {r.state} | {r.size_tier:6} | "
              f"{r.best_signal_type:26} | {r.n_signals:3} signals | ${r.total_penalties:,.0f}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Export failed: {exc}", file=sys.stderr)
        sys.exit(1)
