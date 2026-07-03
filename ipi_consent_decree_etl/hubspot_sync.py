"""
Layer 5 (V2): Push approved stakeholders into HubSpot.

Reads `ipi_intelligence.stakeholders_staging` rows with
hubspot_sync_status = 'approved', upserts them as HubSpot contacts (keyed
by email), tags each with municipality, signal type, and priority score
from `qualified_targets`, then writes back hubspot_contact_id and flips
status to 'synced'.

Guard rails:
  - ONLY 'approved' rows sync — the staging review gate (Layer 3a decision)
    protects the CRM from unproven Clay/research output.
  - Rows without an email are skipped (logged) — HubSpot upsert is keyed on
    email; a contact you can't reach isn't outreach-ready anyway.
  - Not wired into refresh.sh — run manually (or cron it once Layer 3b
    research starts producing approved rows at volume).

Audience segmentation: contacts get ipi_audience_segment =
'State Representative' (confirmed V2 value covering mayors, council/board
members, and state legislators). If the property is an enumeration and the
option is missing, it gets added automatically.

Setup:
  1. HubSpot -> Settings -> Integrations -> Private Apps -> create app with
     scopes: crm.objects.contacts.read/write, crm.schemas.contacts.read/write
  2. Add to .env:  HUBSPOT_PRIVATE_APP_TOKEN=pat-...

Usage:
    python hubspot_sync.py --dry-run     # show what would sync
    python hubspot_sync.py               # sync approved rows
"""

import argparse
import os
import sys
import time

import requests
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

HS_BASE = "https://api.hubapi.com"

# Contact properties IPI's segmentation relies on. ipi_audience_segment is
# assumed to exist (established segmentation); the others are created if
# missing so the sync is self-contained.
CUSTOM_PROPERTIES = {
    "ipi_municipality_key": ("Municipality Key (IPI)", "string", "text"),
    "ipi_signal_type": ("Enforcement Signal Type (IPI)", "string", "text"),
    "ipi_priority_score": ("Priority Score (IPI)", "number", "number"),
    "ipi_role_category": ("Government Role Category (IPI)", "string", "text"),
}

AUDIENCE_SEGMENT_PROPERTY = "ipi_audience_segment"
AUDIENCE_SEGMENT_VALUE = "State Representative"


def _hs_headers():
    token = os.getenv("HUBSPOT_PRIVATE_APP_TOKEN")
    if not token:
        print("HUBSPOT_PRIVATE_APP_TOKEN not set in .env — see module docstring.",
              file=sys.stderr)
        sys.exit(1)
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def ensure_properties(headers, dry_run: bool, segment_values: set = None):
    """Create missing custom contact properties; extend the audience-segment
    enumeration with any segment values the sync is about to write."""
    for name, (label, ptype, field) in CUSTOM_PROPERTIES.items():
        r = requests.get(f"{HS_BASE}/crm/v3/properties/contacts/{name}",
                         headers=headers, timeout=30)
        if r.status_code == 200:
            continue
        if dry_run:
            print(f"  [dry-run] would create contact property: {name}")
            continue
        body = {
            "name": name, "label": label, "type": ptype, "fieldType": field,
            "groupName": "contactinformation",
        }
        cr = requests.post(f"{HS_BASE}/crm/v3/properties/contacts",
                           headers=headers, json=body, timeout=30)
        if cr.status_code in (200, 201):
            print(f"  Created contact property: {name}")
        else:
            print(f"  WARNING: could not create property {name}: "
                  f"{cr.status_code} {cr.text[:200]}")

    # Audience segment enumeration options — ensure every segment value the
    # sync will write exists as an option (rows can carry per-role segments).
    needed_values = {AUDIENCE_SEGMENT_VALUE} | (segment_values or set())
    r = requests.get(f"{HS_BASE}/crm/v3/properties/contacts/{AUDIENCE_SEGMENT_PROPERTY}",
                     headers=headers, timeout=30)
    if r.status_code == 200:
        prop = r.json()
        if prop.get("type") == "enumeration":
            options = {o.get("value") for o in prop.get("options", [])}
            missing = sorted(needed_values - options)
            if missing:
                if dry_run:
                    print(f"  [dry-run] would add options to "
                          f"{AUDIENCE_SEGMENT_PROPERTY}: {missing}")
                else:
                    new_options = prop.get("options", []) + [{
                        "label": v, "value": v,
                        "displayOrder": len(prop.get("options", [])) + i,
                    } for i, v in enumerate(missing)]
                    pr = requests.patch(
                        f"{HS_BASE}/crm/v3/properties/contacts/{AUDIENCE_SEGMENT_PROPERTY}",
                        headers=headers, json={"options": new_options}, timeout=30)
                    if pr.status_code == 200:
                        print(f"  Added {AUDIENCE_SEGMENT_PROPERTY} options: {missing}")
                    else:
                        print(f"  WARNING: couldn't extend {AUDIENCE_SEGMENT_PROPERTY}: "
                              f"{pr.status_code} {pr.text[:200]}")
    else:
        print(f"  NOTE: {AUDIENCE_SEGMENT_PROPERTY} not found ({r.status_code}). "
              "Create it in HubSpot or contacts will sync without segmentation.")


def fetch_approved(client, project_id):
    sql = f"""
    SELECT s.stakeholder_id, s.municipality_key, s.city, s.state,
           s.full_name, s.role_title, s.role_category, s.email, s.phone,
           s.ipi_audience_segment,
           q.best_signal_type, q.priority_score
    FROM `{project_id}.ipi_intelligence.stakeholders_staging` s
    LEFT JOIN `{project_id}.ipi_intelligence.qualified_targets` q
      USING (municipality_key)
    WHERE s.hubspot_sync_status = 'approved'
    """
    return list(client.query(sql).result())


def sync_contacts(rows, headers, dry_run: bool):
    """Upsert contacts by email. Returns list of (stakeholder_id, contact_id)."""
    synced = []
    skipped_no_email = 0
    inputs = []
    row_by_email = {}

    for row in rows:
        if not row.email:
            skipped_no_email += 1
            print(f"  SKIP (no email): {row.full_name} — {row.city}, {row.state}")
            continue
        parts = (row.full_name or "").strip().split()
        first = parts[0] if parts else ""
        last = " ".join(parts[1:]) if len(parts) > 1 else ""
        props = {
            "email": row.email,
            "firstname": first,
            "lastname": last,
            "jobtitle": row.role_title or "",
            "city": row.city or "",
            "state": row.state or "",
            # Per-row segment: political contacts default to "State
            # Representative"; operational contacts (water/utility directors,
            # public works) carry whatever segment 3b research set on the row.
            AUDIENCE_SEGMENT_PROPERTY: row.ipi_audience_segment or AUDIENCE_SEGMENT_VALUE,
            "ipi_municipality_key": row.municipality_key,
            "ipi_signal_type": row.best_signal_type or "",
            "ipi_priority_score": row.priority_score,
            "ipi_role_category": row.role_category or "",
        }
        if row.phone:
            props["phone"] = row.phone
        inputs.append({"idProperty": "email", "id": row.email, "properties": props})
        row_by_email[row.email.lower()] = row

    if skipped_no_email:
        print(f"  {skipped_no_email} row(s) skipped for missing email "
              "(left in 'approved' state)")

    if dry_run:
        for i in inputs:
            print(f"  [dry-run] would upsert: {i['id']}")
        return synced

    BATCH = 100
    for i in range(0, len(inputs), BATCH):
        chunk = inputs[i:i + BATCH]
        r = requests.post(
            f"{HS_BASE}/crm/v3/objects/contacts/batch/upsert",
            headers=headers, json={"inputs": chunk}, timeout=60,
        )
        if r.status_code not in (200, 201):
            print(f"  ERROR batch {i // BATCH + 1}: {r.status_code} {r.text[:300]}")
            continue
        for res in r.json().get("results", []):
            email = (res.get("properties", {}).get("email") or "").lower()
            row = row_by_email.get(email)
            if row:
                synced.append((row.stakeholder_id, res.get("id")))
        time.sleep(0.25)  # rate-limit courtesy

    return synced


def mark_synced(client, project_id, synced):
    """Write contact IDs back and flip status via a temp-table join."""
    if not synced:
        return
    schema = [bigquery.SchemaField("sid", "STRING"),
              bigquery.SchemaField("cid", "STRING")]
    temp_id = f"{project_id}.ipi_intelligence._temp_hs_sync_{int(time.time())}"
    table = bigquery.Table(temp_id, schema=schema)
    client.create_table(table)
    client.insert_rows_json(temp_id, [{"sid": s, "cid": c} for s, c in synced])
    client.query(f"""
        UPDATE `{project_id}.ipi_intelligence.stakeholders_staging` s
        SET hubspot_sync_status = 'synced',
            hubspot_contact_id = t.cid,
            updated_at = CURRENT_TIMESTAMP()
        FROM `{temp_id}` t
        WHERE s.stakeholder_id = t.sid
    """).result()
    client.delete_table(temp_id, not_found_ok=True)
    print(f"  Marked {len(synced)} staging row(s) as synced")


def main():
    parser = argparse.ArgumentParser(description="Layer 5 HubSpot sync")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    project_id = os.getenv("GCP_PROJECT_ID", "ipi-consent-decree-dashboard")
    client = bigquery.Client(project=project_id)

    rows = fetch_approved(client, project_id)
    print(f"Approved stakeholders awaiting sync: {len(rows)}")
    if not rows:
        return

    headers = _hs_headers()
    print("Ensuring HubSpot properties...")
    segments = {r.ipi_audience_segment for r in rows if r.ipi_audience_segment}
    ensure_properties(headers, args.dry_run, segments)

    print("Syncing contacts...")
    synced = sync_contacts(rows, headers, args.dry_run)
    print(f"Synced: {len(synced)}")

    if not args.dry_run:
        mark_synced(client, project_id, synced)


if __name__ == "__main__":
    main()
