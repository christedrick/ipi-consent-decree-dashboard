# Layer 3b Handoff — Stakeholder Research (Cowork task)

This build (Layers 1–5) produces the qualified target list; finding the
humans is a **separate research task** to run in Cowork against the
artifacts below. Texas is the pilot batch (existing HB 500-style funding
angle); confirmed full scope is all 50 states + USVI.

## Input: `ipi_intelligence.qualified_targets` (BigQuery)

One row per qualified municipality (Medium/Large, active signal), rebuilt on
every ETL refresh. Key columns:

- `municipality_key` — join key; carry it through to every stakeholder row
- `city`, `state`, `county`, `primary_facility`
- `size_tier`, `population`
- `best_signal_type`, `n_signals`, `latest_signal_date`, `total_penalties`
- `priority_score` — work the list top-down
- `has_stakeholders` — flips true once contacts land; skip those unless refreshing

Pilot query:

```sql
SELECT * FROM `ipi-consent-decree-dashboard.ipi_intelligence.qualified_targets`
WHERE state = 'TX'
ORDER BY priority_score DESC
```

## Roster to find, per municipality

1. Mayor
2. City council / board members — flag anyone on public works,
   infrastructure, or finance committees
3. City manager / public works director
4. County commissioners — where the utility is county-run
5. State legislator(s) covering the district — funding-advocacy angle

## Sources (in preference order)

1. **Ballotpedia** — top-100 cities have full, current rosters
2. **Municipal websites** — authoritative for smaller cities
3. **Clay waterfall** — coverage on government contacts UNPROVEN; treat
   output as low-confidence until spot-checked
4. **LinkedIn Sales Navigator** — verification + fallback

## Output: `ipi_intelligence.stakeholders_staging` (BigQuery)

Write one row per person. Schema highlights (full DDL in export_targets.py):

- `stakeholder_id` (uuid), `municipality_key` (from qualified_targets)
- `full_name`, `role_title`, `role_category`
  (mayor | council | city_manager | public_works | finance |
   county_commissioner | state_legislator | other)
- `committee` if applicable
- `email`, `phone`, `linkedin_url`
- `source`, `source_url`, `confidence` (high/medium/low)
- `verified` — leave FALSE; human review flips it
- `hubspot_sync_status` — leave 'pending'; review flips to 'approved',
  then hubspot_sync.py pushes to HubSpot with
  ipi_audience_segment = "State Representative"

## USVI caveat (verify before applying the roster template)

USVI is a territory without general-purpose municipalities: government runs
through a Governor + 15-member territorial Senate + district administration
(St. Thomas–St. John, St. Croix). Water/wastewater runs through VIWMA
(waste) and WAPA (water/power) authorities, not city utilities. Verify
ECHO/SDWIS coverage for VI and map the roster to: Governor's office,
territorial senators for the district, VIWMA/WAPA leadership.
