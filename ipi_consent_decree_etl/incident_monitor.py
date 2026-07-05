"""
Incident monitoring (V2 news layer): near-real-time water-infrastructure
incidents for qualified municipalities.

This is the "cheap version" from the V2 scope — a keyword news monitor run
ONLY against the qualified target list (Medium/Large, post-size-filter),
not nationally, to keep noise down. Google News RSS search needs no API
key and can be queried programmatically per municipality (unlike Google
Alerts, which must be created by hand).

For each municipality in `qualified_targets`, queries Google News RSS for
sewer-overflow / boil-water / water-infrastructure keywords, classifies
matches, and MERGEs into `ipi_intelligence.incident_reports` keyed on a
URL hash (idempotent — safe to re-run on any cadence).

The Layer 4 score in export_targets.py adds +10 for municipalities with an
incident in the last 90 days, so fresh incidents float targets up the
ranked list. Run this BEFORE export_targets.py.

Cadence: refresh.sh runs it on the biweekly refresh; for the
"reach out ASAP" goal, cron it daily:
    0 7 * * * cd <etl dir> && ./venv/bin/python incident_monitor.py

Usage:
    python incident_monitor.py                # all Medium+Large targets
    python incident_monitor.py --state TX     # pilot batch
    python incident_monitor.py --limit 20     # smoke test
"""

import argparse
import hashlib
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Optional

import requests
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
load_dotenv(os.path.expanduser("~/.config/ipi-etl/.env"))  # secrets live outside the synced repo dir

RSS_URL = "https://news.google.com/rss/search"

# Search keywords — quoted phrases OR'd together per municipality
INCIDENT_QUERY = (
    '"sewer overflow" OR "sanitary sewer overflow" OR "sewage spill" OR '
    '"boil water" OR "water main break" OR "wastewater violation" OR '
    '"EPA violation" OR "consent decree"'
)

# Classification: first matching pattern wins (ordered by specificity).
# NOTE: items whose HEADLINE matches none of these are dropped entirely —
# Google News also matches article bodies, and body-only matches proved to
# be mostly tangential (regional roundups, listicles).
INCIDENT_TYPES = [
    ("sewer_overflow",   re.compile(r"sanitary sewer overflow|sewer overflow|sewage spill|sewage overflow|sso\b", re.I)),
    ("boil_water",       re.compile(r"boil[- ]water", re.I)),
    ("water_main_break", re.compile(r"water main break|main break", re.I)),
    ("consent_decree",   re.compile(r"consent decree", re.I)),
    ("epa_violation",    re.compile(r"epa violation|wastewater violation|clean water act", re.I)),
]

# Retrospective / listicle noise — never an outreach trigger
LISTICLE_RE = re.compile(
    r"\btop \d+\b|most[- ]read|year in review|looking back|a look back", re.I
)

LOOKBACK_DAYS = 30          # ignore items older than this
REQUEST_DELAY_S = 0.75      # politeness delay between RSS queries

INCIDENTS_DDL = """
CREATE TABLE IF NOT EXISTS `{project}.ipi_intelligence.incident_reports` (
  incident_id STRING NOT NULL,        -- sha1 of article URL
  municipality_key STRING NOT NULL,   -- FK -> qualified_targets
  city STRING,
  state STRING,
  headline STRING,
  url STRING,
  news_source STRING,
  incident_type STRING,               -- sewer_overflow | boil_water |
                                      -- water_main_break | consent_decree |
                                      -- epa_violation | other
  published_at TIMESTAMP,
  first_seen_at TIMESTAMP,
  dismissed BOOL DEFAULT FALSE        -- analyst marks false positives TRUE;
                                      -- dismissed incidents don't score
)
"""


def classify(text: str) -> Optional[str]:
    """Return incident type, or None if the headline isn't a real incident."""
    for label, pattern in INCIDENT_TYPES:
        if pattern.search(text):
            return label
    return None


def fetch_targets(client, project_id, state=None, limit=None):
    where = "WHERE size_tier IN ('Medium', 'Large')"
    if state:
        where += f" AND state = '{state}'"
    sql = f"""
    SELECT municipality_key, city, state
    FROM `{project_id}.ipi_intelligence.qualified_targets`
    {where}
    ORDER BY priority_score DESC
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    return list(client.query(sql).result())


def query_news(city: str, state: str, session: requests.Session) -> list[dict]:
    """One Google News RSS query for a municipality. Returns parsed items.

    Precision rules (tuned on the Texas pilot, where a single Round Rock
    story matched six other cities via body text):
      1. headline must match an incident pattern (body-only matches dropped)
      2. the city must appear in the headline, or in the outlet name
         (a local outlet reporting an incident is reporting on its city)
      3. listicle/retrospective headlines dropped
    """
    q = f'"{city}" "{state}" ({INCIDENT_QUERY})'
    try:
        resp = session.get(
            RSS_URL,
            params={"q": q, "hl": "en-US", "gl": "US", "ceid": "US:en"},
            timeout=30,
        )
        if resp.status_code != 200:
            return []
        root = ET.fromstring(resp.content)
    except (requests.RequestException, ET.ParseError):
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    city_l = city.lower()
    items = []
    for item in root.iter("item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        source = (item.findtext("source") or "").strip()
        if not title or not link:
            continue
        try:
            published = parsedate_to_datetime(pub)
        except (TypeError, ValueError):
            continue
        if published < cutoff:
            continue

        incident_type = classify(title)
        if incident_type is None:
            continue
        if LISTICLE_RE.search(title):
            continue
        if city_l not in title.lower() and city_l not in source.lower():
            continue

        items.append({
            "headline": title,
            "url": link,
            "news_source": source,
            "published_at": published.isoformat(),
            "incident_type": incident_type,
        })
    return items


def main():
    parser = argparse.ArgumentParser(description="V2 incident news monitor")
    parser.add_argument("--state", help="Only monitor targets in this state (e.g. TX)")
    parser.add_argument("--limit", type=int, help="Max municipalities to query")
    parser.add_argument("--dry-run", action="store_true", help="No BigQuery write")
    args = parser.parse_args()

    project_id = os.getenv("GCP_PROJECT_ID", "ipi-consent-decree-dashboard")
    client = bigquery.Client(project=project_id)
    client.query(INCIDENTS_DDL.format(project=project_id)).result()

    targets = fetch_targets(client, project_id, args.state, args.limit)
    print(f"Monitoring {len(targets)} Medium/Large municipalities "
          f"(lookback {LOOKBACK_DAYS}d)...")

    session = requests.Session()
    session.headers.update({"User-Agent": "IPIIncidentMonitor/1.0"})

    rows = []
    for i, t in enumerate(targets, 1):
        if not t.city:
            continue
        items = query_news(t.city, t.state, session)
        for it in items:
            rows.append({
                "incident_id": hashlib.sha1(it["url"].encode()).hexdigest(),
                "municipality_key": t.municipality_key,
                "city": t.city,
                "state": t.state,
                "headline": it["headline"][:500],
                "url": it["url"],
                "news_source": it["news_source"][:200],
                "incident_type": it["incident_type"],
                "published_at": it["published_at"],
                "first_seen_at": datetime.now(timezone.utc).isoformat(),
            })
        if items:
            print(f"  [{i}/{len(targets)}] {t.city}, {t.state}: {len(items)} item(s)")
        time.sleep(REQUEST_DELAY_S)

    print(f"\nTotal incident items found: {len(rows)}")
    if not rows or args.dry_run:
        if args.dry_run and rows:
            for r in rows[:10]:
                print(f"  [dry-run] {r['incident_type']:16} | {r['city']}, {r['state']} | {r['headline'][:80]}")
        return

    # MERGE on incident_id — idempotent, preserves reviewed flags
    schema = [
        bigquery.SchemaField("incident_id", "STRING"),
        bigquery.SchemaField("municipality_key", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("headline", "STRING"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("news_source", "STRING"),
        bigquery.SchemaField("incident_type", "STRING"),
        bigquery.SchemaField("published_at", "TIMESTAMP"),
        bigquery.SchemaField("first_seen_at", "TIMESTAMP"),
    ]
    temp_id = f"{project_id}.ipi_intelligence._temp_incidents_{int(time.time())}"
    client.create_table(bigquery.Table(temp_id, schema=schema))
    errors = client.insert_rows_json(temp_id, rows)
    if errors:
        print(f"Insert errors: {errors[:3]}", file=sys.stderr)
        client.delete_table(temp_id, not_found_ok=True)
        sys.exit(1)

    merge_sql = f"""
    MERGE `{project_id}.ipi_intelligence.incident_reports` T
    USING `{temp_id}` S
    ON T.incident_id = S.incident_id
    WHEN NOT MATCHED THEN
      INSERT (incident_id, municipality_key, city, state, headline, url,
              news_source, incident_type, published_at, first_seen_at, dismissed)
      VALUES (S.incident_id, S.municipality_key, S.city, S.state, S.headline,
              S.url, S.news_source, S.incident_type, S.published_at,
              S.first_seen_at, FALSE)
    """
    job = client.query(merge_sql)
    job.result()
    client.delete_table(temp_id, not_found_ok=True)
    print(f"New incidents merged: {job.num_dml_affected_rows or 0}")

    summary_sql = f"""
    SELECT incident_type, COUNT(*) n
    FROM `{project_id}.ipi_intelligence.incident_reports`
    WHERE published_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    GROUP BY incident_type ORDER BY n DESC
    """
    print("\nIncidents on record (last 30 days):")
    for r in client.query(summary_sql).result():
        print(f"  {r.incident_type:18} {r.n}")


if __name__ == "__main__":
    main()
