"""
Layer 3b helper: BigQuery plumbing for the ipi-contact-research skill.

The research itself is done by Claude (daily scheduled task or an
interactive session); this script owns every read/write against
research_queue and stakeholders_staging so those writes are parameterized,
single-statement, validated, and idempotent. (The BigQuery console hung on
multi-statement scripts during the Cowork runs, and a cancelled INSERT can
still commit — hence NOT EXISTS guards on every insert.)

All output is JSON on stdout; warnings go to stderr.

Usage (from ipi_consent_decree_etl/, with the venv python):
    python research_cli.py next                 # queued municipalities + target context
    python research_cli.py recover              # reset 'researching' rows stuck > 20h
    python research_cli.py claim KEY            # queued -> researching
    python research_cli.py existing KEY         # staging rows already on file for KEY
    python research_cli.py insert rows.json     # validate + insert pending rows
    python research_cli.py done KEY             # researching -> done
    python research_cli.py release KEY          # researching -> queued (couldn't finish)
    python research_cli.py approved             # approved rows awaiting HubSpot sync
    python research_cli.py mark-synced SID=CID [SID=CID ...]
    python research_cli.py status               # queue + staging counts
"""

import json
import os
import sys
import uuid
import warnings

warnings.filterwarnings("ignore")  # py3.9 / LibreSSL deprecation noise

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
load_dotenv(os.path.expanduser("~/.config/ipi-etl/.env"))  # secrets live outside the synced repo dir

PROJECT = os.getenv("GCP_PROJECT_ID", "ipi-consent-decree-dashboard")
DS = f"`{PROJECT}.ipi_intelligence"
QUEUE = f"{DS}.research_queue`"
STAGING = f"{DS}.stakeholders_staging`"
TARGETS = f"{DS}.qualified_targets`"

ROLE_CATEGORIES = {
    "water_director", "mayor", "council", "city_manager", "public_works",
    "finance", "county_commissioner", "state_legislator", "other",
}
# Must match the HubSpot ipi_audience_segment enumeration.
SEGMENTS = {"State Representative", "Municipal Water", "Water Utility"}
CONFIDENCE = {"high", "medium", "low"}
STRING_FIELDS = [
    "full_name", "role_title", "role_category", "committee", "email", "phone",
    "linkedin_url", "source", "source_url", "confidence", "research_notes",
    "ipi_audience_segment",
]

client = bigquery.Client(project=PROJECT)


def _q(sql, **params):
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter(k, "STRING", v) if isinstance(v, list)
        else bigquery.ScalarQueryParameter(k, "STRING", v)
        for k, v in params.items()
    ])
    return client.query(sql, job_config=cfg)


def _rows(sql, **params):
    return [dict(r) for r in _q(sql, **params).result()]


def _out(obj):
    print(json.dumps(obj, indent=2, default=str))


def _dml(sql, **params):
    job = _q(sql, **params)
    job.result()
    return job.num_dml_affected_rows or 0


def cmd_next():
    _out(_rows(f"""
        SELECT rq.municipality_key, rq.city, rq.state, rq.priority_score,
               rq.queued_at, q.county, q.primary_facility, q.population,
               q.size_tier, q.best_signal_type, q.n_signals, q.total_penalties
        FROM {QUEUE} rq
        LEFT JOIN {TARGETS} q USING (municipality_key)
        WHERE rq.status = 'queued'
        ORDER BY rq.priority_score DESC
    """))


def cmd_recover():
    n = _dml(f"""
        UPDATE {QUEUE} SET status = 'queued'
        WHERE status = 'researching'
          AND queued_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 20 HOUR)
    """)
    _out({"reset_to_queued": n})


def _transition(key, frm, to):
    n = _dml(f"""
        UPDATE {QUEUE} SET status = @to
        WHERE municipality_key = @key AND status = @frm
    """, key=key, frm=frm, to=to)
    _out({"municipality_key": key, "from": frm, "to": to, "updated": n})
    if n == 0:
        sys.exit(f"No row in status '{frm}' for {key}")


def cmd_claim(key):
    # queued_at doubles as the claim timestamp so `recover` can spot stalls.
    n = _dml(f"""
        UPDATE {QUEUE} SET status = 'researching', queued_at = CURRENT_TIMESTAMP()
        WHERE municipality_key = @key AND status = 'queued'
    """, key=key)
    _out({"municipality_key": key, "claimed": bool(n)})
    if n == 0:
        sys.exit(f"{key} is not queued (already claimed or removed)")


def cmd_existing(key):
    _out(_rows(f"""
        SELECT stakeholder_id, full_name, role_title, role_category, email,
               linkedin_url, hubspot_sync_status, created_at
        FROM {STAGING}
        WHERE municipality_key = @key
        ORDER BY role_category, full_name
    """, key=key))


def _validate(r, key):
    errs = []
    if r.get("municipality_key") != key:
        errs.append("municipality_key mismatch")
    if not (r.get("full_name") or "").strip():
        errs.append("full_name missing")
    if not (r.get("email") or r.get("linkedin_url")):
        errs.append("needs email and/or linkedin_url")
    if r.get("linkedin_url") and "linkedin.com/in/" not in r["linkedin_url"]:
        errs.append("linkedin_url must be a /in/ profile URL")
    if r.get("role_category") not in ROLE_CATEGORIES:
        errs.append(f"role_category must be one of {sorted(ROLE_CATEGORIES)}")
    if r.get("ipi_audience_segment") not in SEGMENTS:
        errs.append(f"ipi_audience_segment must be one of {sorted(SEGMENTS)}")
    if r.get("confidence") not in CONFIDENCE:
        errs.append("confidence must be high|medium|low")
    if not r.get("source_url"):
        errs.append("source_url missing")
    return errs


def cmd_insert(path):
    with open(path) as f:
        rows = json.load(f)
    if not isinstance(rows, list) or not rows:
        sys.exit("rows file must be a non-empty JSON array")
    keys = {r.get("municipality_key") for r in rows}
    if len(keys) != 1:
        sys.exit("insert one municipality at a time")
    key = keys.pop()
    target = _rows(f"SELECT city, state FROM {QUEUE} WHERE municipality_key = @key", key=key)
    if not target:
        sys.exit(f"{key} is not in research_queue")

    rejected, accepted = [], []
    for r in rows:
        errs = _validate(r, key)
        if errs:
            rejected.append({"full_name": r.get("full_name"), "errors": errs})
        else:
            accepted.append(r)

    inserted, skipped_dupes = [], []
    for r in accepted:
        sid = str(uuid.uuid4())
        params = {k: (r.get(k) or None) for k in STRING_FIELDS}
        params.update(sid=sid, key=key, city=target[0]["city"], state=target[0]["state"])
        # Dedup on (municipality, name) and on email/linkedin anywhere in staging.
        n = _dml(f"""
            INSERT INTO {STAGING}
              (stakeholder_id, municipality_key, city, state, full_name, role_title,
               role_category, committee, email, phone, linkedin_url, source,
               source_url, confidence, verified, research_notes,
               ipi_audience_segment, hubspot_sync_status, created_at, updated_at)
            SELECT @sid, @key, @city, @state, @full_name, @role_title,
                   @role_category, @committee, @email, @phone, @linkedin_url, @source,
                   @source_url, @confidence, FALSE, @research_notes,
                   @ipi_audience_segment, 'pending', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            FROM (SELECT 1)
            WHERE NOT EXISTS (
              SELECT 1 FROM {STAGING} s
              WHERE (s.municipality_key = @key
                     AND LOWER(TRIM(s.full_name)) = LOWER(TRIM(@full_name)))
                 OR (@email IS NOT NULL AND LOWER(s.email) = LOWER(@email))
                 OR (@linkedin_url IS NOT NULL
                     AND RTRIM(LOWER(s.linkedin_url), '/') = RTRIM(LOWER(@linkedin_url), '/'))
            )
        """, **params)
        (inserted if n else skipped_dupes).append(r["full_name"])

    _out({"municipality_key": key, "inserted": inserted,
          "skipped_duplicates": skipped_dupes, "rejected": rejected})


def cmd_approved():
    _out(_rows(f"""
        SELECT s.stakeholder_id, s.municipality_key, s.city, s.state, s.full_name,
               s.role_title, s.role_category, s.committee, s.email, s.phone,
               s.linkedin_url, s.confidence, s.ipi_audience_segment,
               q.best_signal_type, q.priority_score
        FROM {STAGING} s
        LEFT JOIN {TARGETS} q USING (municipality_key)
        WHERE s.hubspot_sync_status = 'approved'
        ORDER BY s.state, s.city, s.role_category
    """))


def cmd_mark_synced(pairs):
    done = 0
    for p in pairs:
        sid, _, cid = p.partition("=")
        if not (sid and cid):
            sys.exit(f"bad pair {p!r}; expected STAKEHOLDER_ID=HUBSPOT_CONTACT_ID")
        done += _dml(f"""
            UPDATE {STAGING}
            SET hubspot_sync_status = 'synced', hubspot_contact_id = @cid,
                updated_at = CURRENT_TIMESTAMP()
            WHERE stakeholder_id = @sid AND hubspot_sync_status = 'approved'
        """, sid=sid, cid=cid)
    _out({"marked_synced": done})


def cmd_status():
    _out({
        "queue": _rows(f"SELECT status, COUNT(*) n, STRING_AGG(city, ', ') cities FROM {QUEUE} GROUP BY status"),
        "staging": _rows(f"SELECT hubspot_sync_status, COUNT(*) n FROM {STAGING} GROUP BY 1"),
    })


def main():
    a = sys.argv[1:]
    if not a:
        sys.exit(__doc__)
    cmd, rest = a[0], a[1:]
    one = {"claim": cmd_claim, "existing": cmd_existing, "insert": cmd_insert}
    if cmd in one and len(rest) == 1:
        one[cmd](rest[0])
    elif cmd == "done" and len(rest) == 1:
        _transition(rest[0], "researching", "done")
    elif cmd == "release" and len(rest) == 1:
        _transition(rest[0], "researching", "queued")
    elif cmd == "mark-synced" and rest:
        cmd_mark_synced(rest)
    elif cmd in ("next", "recover", "approved", "status") and not rest:
        {"next": cmd_next, "recover": cmd_recover,
         "approved": cmd_approved, "status": cmd_status}[cmd]()
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
