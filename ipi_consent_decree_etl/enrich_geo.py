"""
County enrichment for qualified targets (persistent lookup table).

EPA enforcement records mostly lack county names (41 of 254 targets had
one), but county is how regional events get reported — flood coverage says
"Kerr County", FEMA declarations designate counties. This script reverse-
geocodes each qualified municipality's lat/lon via the FCC Area API (free,
no key) and stores the result in `ipi_intelligence.municipality_geo`.

The table is persistent and incremental — qualified_targets gets rebuilt
daily, but this table only grows, and export_targets.py joins it to fill
the county column. Only municipalities missing from the table are looked up
(~1 API call each, once ever).

Usage:
    python enrich_geo.py
"""

import os
import sys
import time

import requests
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
load_dotenv(os.path.expanduser("~/.config/ipi-etl/.env"))

FCC_AREA_URL = "https://geo.fcc.gov/api/census/area"

GEO_DDL = """
CREATE TABLE IF NOT EXISTS `{project}.ipi_intelligence.municipality_geo` (
  municipality_key STRING NOT NULL,
  county STRING,
  county_fips STRING,
  enriched_at TIMESTAMP
)
"""


def main():
    project_id = os.getenv("GCP_PROJECT_ID", "ipi-consent-decree-dashboard")
    client = bigquery.Client(project=project_id)
    client.query(GEO_DDL.format(project=project_id)).result()

    todo = list(client.query(f"""
        SELECT q.municipality_key, q.latitude, q.longitude
        FROM `{project_id}.ipi_intelligence.qualified_targets` q
        LEFT JOIN `{project_id}.ipi_intelligence.municipality_geo` g
          USING (municipality_key)
        WHERE g.municipality_key IS NULL
          AND q.latitude IS NOT NULL AND q.longitude IS NOT NULL
    """).result())
    print(f"Municipalities needing county lookup: {len(todo)}")
    if not todo:
        return

    session = requests.Session()
    rows = []
    for i, t in enumerate(todo, 1):
        try:
            resp = session.get(
                FCC_AREA_URL,
                params={"lat": f"{t.latitude:.5f}", "lon": f"{t.longitude:.5f}",
                        "format": "json"},
                timeout=20,
            )
            results = resp.json().get("results", []) if resp.status_code == 200 else []
            county = results[0].get("county_name") if results else None
            fips = results[0].get("county_fips") if results else None
        except Exception:
            county, fips = None, None
        rows.append({
            "municipality_key": t.municipality_key,
            "county": county,
            "county_fips": fips,
            "enriched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
        if i % 50 == 0:
            print(f"  {i}/{len(todo)} geocoded...")
        time.sleep(0.2)

    resolved = sum(1 for r in rows if r["county"])
    temp_id = f"{project_id}.ipi_intelligence._temp_geo_{int(time.time())}"
    schema = [
        bigquery.SchemaField("municipality_key", "STRING"),
        bigquery.SchemaField("county", "STRING"),
        bigquery.SchemaField("county_fips", "STRING"),
        bigquery.SchemaField("enriched_at", "TIMESTAMP"),
    ]
    client.create_table(bigquery.Table(temp_id, schema=schema))
    errors = client.insert_rows_json(temp_id, rows)
    if errors:
        print(f"Insert errors: {errors[:3]}", file=sys.stderr)
        client.delete_table(temp_id, not_found_ok=True)
        sys.exit(1)
    client.query(f"""
        MERGE `{project_id}.ipi_intelligence.municipality_geo` T
        USING `{temp_id}` S
        ON T.municipality_key = S.municipality_key
        WHEN NOT MATCHED THEN
          INSERT (municipality_key, county, county_fips, enriched_at)
          VALUES (S.municipality_key, S.county, S.county_fips, S.enriched_at)
    """).result()
    client.delete_table(temp_id, not_found_ok=True)
    print(f"Enriched {resolved}/{len(rows)} with county names")


if __name__ == "__main__":
    main()
