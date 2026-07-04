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

1. Municipal water director / water utility director (operational buyer)
2. Mayor
3. City council / board members — flag anyone on public works,
   infrastructure, or finance committees
4. City manager / public works director
5. County commissioners — where the utility is county-run
6. State legislator(s) covering the district — funding-advocacy angle

## Contact-data bar (what makes a row outreach-ready)

Every row needs **email and/or LinkedIn profile URL** — that's the
activation requirement, not a nice-to-have:
- **email** → HubSpot Sales Sequence (synced via hubspot_sync.py; rows
  without email are skipped by the sync)
- **linkedin_url** → HeyReach campaign

A name with neither is research debt, not a lead. Prefer official .gov
emails; personal emails only when nothing official exists.

Set `ipi_audience_segment` per role:
- "State Representative" — mayors, council/board members, county
  commissioners, state legislators (political persona)
- leave the existing IPI operational segment for water/utility directors
  and public-works staff (check current values in HubSpot before writing;
  the sync extends the enumeration automatically with whatever you set)

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

## Preferred flow: the dashboard research queue

The Top Priority Targets table on the dashboard supports row selection —
tick municipalities, click **Queue for contact research**, and they land in
`ipi_intelligence.research_queue` with status 'queued'. The dashboard shows
a paste-ready Cowork prompt that processes the queue and flips each
municipality to 'done' as research completes. Use the batch prompt below
only when you want a whole-state sweep instead of hand-picked targets.

## Paste-ready Cowork prompt (Texas pilot batch)

> I'm doing stakeholder research for IPI (Impact Pipe Inspection —
> infrastructure-intelligence positioning, see ipi-pipe.com). Input: the
> BigQuery table `ipi-consent-decree-dashboard.ipi_intelligence.qualified_targets`
> — start with `WHERE state = 'TX' ORDER BY priority_score DESC` (52
> Medium/Large municipalities). For each municipality, working top-down by
> priority_score, find: the municipal water / water-utility director, the
> mayor, city council or board members (flag public-works / infrastructure /
> finance committee members), the city manager or public works director,
> county commissioners where the utility is county-run, and the state
> legislator(s) for the district. For EVERY person capture: full name, exact
> title, role_category (water_director | mayor | council | city_manager |
> public_works | finance | county_commissioner | state_legislator | other),
> committee if any, official email, phone if listed, LinkedIn profile URL,
> source + source URL, and confidence (high/medium/low). Every row must have
> email and/or LinkedIn URL — a name with neither doesn't count. Sources in
> preference order: Ballotpedia (top-100 cities), the municipality's own
> website, Clay waterfall (treat as low-confidence until spot-checked),
> LinkedIn Sales Navigator for verification. Write results as rows into
> `ipi_intelligence.stakeholders_staging` (schema documented in
> export_targets.py in the IPI Dashboard repo; stakeholder_id = new UUID,
> municipality_key from qualified_targets, verified = FALSE,
> hubspot_sync_status = 'pending', ipi_audience_segment = "State
> Representative" for political roles, existing IPI operational segment for
> water/utility staff). If BigQuery access isn't available from this
> session, produce a CSV with exactly those columns instead and I'll load it.

## USVI caveat (verify before applying the roster template)

USVI is a territory without general-purpose municipalities: government runs
through a Governor + 15-member territorial Senate + district administration
(St. Thomas–St. John, St. Croix). Water/wastewater runs through VIWMA
(waste) and WAPA (water/power) authorities, not city utilities. Verify
ECHO/SDWIS coverage for VI and map the roster to: Governor's office,
territorial senators for the district, VIWMA/WAPA leadership.
