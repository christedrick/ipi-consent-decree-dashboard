"""
Backfill derived columns in BigQuery (V2): signal classification + size tier.

signal_type / signal_rank (Layer 1) derive from enforcement_level +
action_type. size_tier (Layer 2) derives from population. Both are computed
in-place with UPDATEs — no bulk re-download needed.

Runs as a refresh.sh step AFTER populate_population.py so size tiers see
final population values. size_tier is intentionally NOT in etl_bulk's
BQ_SCHEMA merge list: records are built before population enrichment, so
merging it would wipe values on every refresh. This script owns it.

Signal ranking: state enforcement action (1) > federal consent decree (2) >
other federal action (3) > DMR violation (4) > unclassified (5).

Size tiers (confirmed V2 breakpoints):
    Small  < 100,000 | Medium 100,000-500,000 | Large 500,000+ | Unknown NULL

Usage:
    python backfill_signals.py            # backfill NULL signal rows + all size tiers
    python backfill_signals.py --all      # recompute signals on every row too
"""

import argparse
import os
import sys

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
load_dotenv(os.path.expanduser("~/.config/ipi-etl/.env"))  # secrets live outside the synced repo dir

SIGNAL_CASE_SQL = """
CASE
  WHEN LOWER(COALESCE(enforcement_level, '')) = 'state'
    THEN {state_val}
  WHEN LOWER(COALESCE(enforcement_level, '')) = 'federal' AND (
       LOWER(COALESCE(action_type, '')) LIKE '%consent decree%'
    OR LOWER(COALESCE(action_type, '')) LIKE '%consent agreement/final order%'
    OR LOWER(COALESCE(action_type, '')) LIKE '%civil judicial action%'
  )
    THEN {fed_cd_val}
  WHEN LOWER(COALESCE(enforcement_level, '')) = 'federal'
    THEN {fed_other_val}
  ELSE {unknown_val}
END
"""


def main():
    parser = argparse.ArgumentParser(description="Backfill signal_type/signal_rank")
    parser.add_argument("--all", action="store_true",
                        help="Recompute all rows, not just NULLs")
    args = parser.parse_args()

    project_id = os.getenv("GCP_PROJECT_ID", "ipi-consent-decree-dashboard")
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.ipi_intelligence.consent_decrees"

    # Ensure columns exist (signal cols also added by etl_bulk schema update;
    # size_tier is owned exclusively by this script — see module docstring)
    ddl = f"""
    ALTER TABLE `{table_id}`
      ADD COLUMN IF NOT EXISTS signal_type STRING,
      ADD COLUMN IF NOT EXISTS signal_rank INT64,
      ADD COLUMN IF NOT EXISTS size_tier STRING
    """
    client.query(ddl).result()
    print(f"Columns ready on {table_id}")

    type_expr = SIGNAL_CASE_SQL.format(
        state_val="'State Enforcement Action'",
        fed_cd_val="'Federal Consent Decree'",
        fed_other_val="'Federal Enforcement Action'",
        unknown_val="'Unclassified'",
    )
    rank_expr = SIGNAL_CASE_SQL.format(
        state_val="1", fed_cd_val="2", fed_other_val="3", unknown_val="5",
    )

    # Never touch DMR rows — they're classified at ingest by process_qncr_dmr
    # and their enforcement_level ("Monitoring") isn't derivable here.
    dmr_guard = "COALESCE(signal_type, '') != 'DMR Violation'"
    where = (
        dmr_guard if args.all
        else f"(signal_type IS NULL OR signal_rank IS NULL) AND {dmr_guard}"
    )
    update_sql = f"""
    UPDATE `{table_id}`
    SET signal_type = {type_expr},
        signal_rank = {rank_expr}
    WHERE {where}
    """
    job = client.query(update_sql)
    job.result()
    print(f"Backfilled {job.num_dml_affected_rows or 0} rows "
          f"({'all rows' if args.all else 'NULL rows only'})")

    # --- Size tier (Layer 2) — always recomputed, population may have changed
    size_sql = f"""
    UPDATE `{table_id}`
    SET size_tier = CASE
        WHEN population IS NULL THEN 'Unknown'
        WHEN population < 100000 THEN 'Small'
        WHEN population < 500000 THEN 'Medium'
        ELSE 'Large'
      END
    WHERE TRUE
    """
    job = client.query(size_sql)
    job.result()
    print(f"Size tiers set on {job.num_dml_affected_rows or 0} rows")

    # Quick verification
    verify_sql = f"""
    SELECT signal_type, signal_rank, COUNT(*) AS n
    FROM `{table_id}`
    GROUP BY signal_type, signal_rank
    ORDER BY signal_rank
    """
    print("\nSignal distribution:")
    for row in client.query(verify_sql).result():
        print(f"  rank {row.signal_rank} | {row.signal_type:28} | {row.n:,}")

    size_verify_sql = f"""
    SELECT size_tier, COUNT(*) AS n, COUNTIF(case_status = 'Active') AS active_n
    FROM `{table_id}`
    GROUP BY size_tier
    ORDER BY CASE size_tier
      WHEN 'Large' THEN 0 WHEN 'Medium' THEN 1
      WHEN 'Small' THEN 2 ELSE 3 END
    """
    print("\nSize tier distribution:")
    for row in client.query(size_verify_sql).result():
        print(f"  {row.size_tier:8} | total {row.n:,} | active {row.active_n:,}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Backfill failed: {exc}", file=sys.stderr)
        sys.exit(1)
