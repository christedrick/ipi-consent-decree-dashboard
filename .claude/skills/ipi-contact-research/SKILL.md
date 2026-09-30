---
name: ipi-contact-research
description: Research government stakeholder contacts for IPI (Impact Pipe Inspection) target municipalities from the consent-decree dashboard's research queue, write them to BigQuery staging for review, and sync approved contacts to HubSpot. Use for "research the IPI queue", "find contacts for <city>", "sync approved IPI contacts to HubSpot", or the daily IPI research task.
---

# IPI contact research

IPI sells pipe inspection to water/wastewater utilities under enforcement
pressure. The consent-decree dashboard ranks municipalities; the user
ticks the ones worth pursuing, which queues them in BigQuery. This skill
finds the people at those municipalities and stages them for human review.

**Two modes, never mixed:**

| Mode | Trigger | Touches HubSpot? |
|---|---|---|
| **Research** | Daily noon task, or "research the IPI queue / <city>" | Read-only (dedup lookups) |
| **Sync** | User explicitly asks to sync approved contacts | Writes, after user confirms in chat |

The review gate is non-negotiable: research only ever writes
`hubspot_sync_status = 'pending'` rows. The user approves them on the
dashboard (Lead Pipeline → review contacts). Nothing reaches HubSpot until
then.

## Environment

- Repo: `~/Library/CloudStorage/OneDrive-Personal/Documents/2026 Freelance/Banquo Labs/IPI/IPI Dashboard Consent Decree/`
- All BigQuery access goes through the helper (credentials load automatically):
  ```bash
  cd "<repo>/ipi_consent_decree_etl" && venv/bin/python research_cli.py <command>
  ```
  Commands: `next`, `recover`, `claim KEY`, `existing KEY`, `insert FILE`,
  `done KEY`, `release KEY`, `approved`, `mark-synced SID=CID ...`, `status`.
  Output is JSON. Don't hand-write SQL against these tables.
- Write temp files (the rows JSON) to the session scratchpad or `/tmp`,
  never into the OneDrive repo.
- **No shell available** (claude.ai Project fallback)? Do the research the
  same way, then output a CSV with the row fields below plus
  `municipality_key` and tell the user to load it; don't attempt BigQuery.

## Research mode

1. `recover` (resets municipalities stuck in 'researching' from a crashed
   run), then `next`. Empty list → report "queue empty" and stop.
2. For each municipality, highest priority first:
   1. `claim KEY`. If it fails, someone else has it; skip.
   2. `existing KEY` — don't re-research people already on file.
   3. Research the roster (below). Budget ~15–25 minutes per municipality;
      breadth over perfection — the reviewer will reject weak rows.
   4. Write rows to a JSON file and `insert FILE`. Read the result: fix and
      re-insert anything in `rejected`; `skipped_duplicates` is fine.
   5. `done KEY`. If you couldn't finish (tool failure, blocked sites), run
      `release KEY` instead so tomorrow's run retries it.
3. Finish with the run report (below).

### Roster, per municipality

Use the target context from `next` (county, primary_facility) to figure out
**who runs the utility** first — it changes the roster:

- **City-run utility**: water/utility director, public works director,
  city manager, mayor, full city council.
- **Separate utility authority** (e.g. `…WATER UTILITY AUTHORITY`,
  `…MUD`, `…SANITARY DISTRICT`): authority executive director/CEO and
  operations/engineering leads, the authority's board (often mixed city
  and county electeds), plus the mayor and council of the named city.
- **County-run utility** or authority with county seats: add the county
  commissioners.
- **Always**: the state legislators (house + senate) whose districts cover
  the utility's service area. Flag anyone on a water, natural resources,
  infrastructure, appropriations/finance committee.

Flag council/board members on public works, infrastructure, utilities, or
finance committees in `committee`.

### Contact bar

A row counts only with **email and/or a LinkedIn /in/ profile URL**.
Email → HubSpot sequence; LinkedIn → exported from HubSpot to HeyReach.
A name with neither is not a lead — leave it out and mention it in the
report.

- Prefer official .gov/.org emails from the official site. Infer an
  address from a published pattern (e.g. first initial + last name@cabq.gov)
  only when at least two published addresses confirm the pattern; mark
  such rows `confidence = medium` and say "pattern-inferred email" in
  `research_notes`.
- Generic inboxes (council@, mayor@) are acceptable for electeds when no
  personal address exists — note it.
- LinkedIn: only a profile you've matched on name + current role + place.
  Never guess a URL.

### Sources, in order

1. Official sites (city, county, utility authority, state legislature) —
   authoritative; `confidence = high`.
2. Ballotpedia — current rosters for larger cities and legislators.
3. Clay connector (`search-contacts-by-name`, `add-contact-data-points`) to
   fill missing emails/LinkedIn. Clay's government coverage is unproven:
   `confidence = low` unless an official source corroborates.
4. Web search for LinkedIn profiles and recent news (title changes,
   resignations). Do not scrape LinkedIn or Sales Navigator.

Before inserting, check HubSpot read-only for each person (search contacts
by email, then by name). If they already exist in HubSpot, still insert
the row but put `already in HubSpot: <contact id>` in `research_notes` so
the reviewer knows.

### Row fields (JSON array, one object per person)

```json
{
  "municipality_key": "albuquerque|NM",
  "full_name": "Jane Doe",
  "role_title": "Councilor, District 4",
  "role_category": "council",
  "committee": "Finance & Government Operations",
  "email": "jdoe@cabq.gov",
  "phone": "505-555-0100",
  "linkedin_url": "https://www.linkedin.com/in/janedoe/",
  "source": "city_website",
  "source_url": "https://www.cabq.gov/council/...",
  "confidence": "high",
  "research_notes": "",
  "ipi_audience_segment": "State Representative"
}
```

- `role_category`: water_director | mayor | council | city_manager |
  public_works | finance | county_commissioner | state_legislator | other
- `ipi_audience_segment` (must match HubSpot's options exactly):
  - `State Representative` — every elected/political role (mayor, council,
    board members who are electeds, county commissioners, legislators)
  - `Municipal Water` — staff of a city-run water/wastewater department
    (incl. public works, city manager)
  - `Water Utility` — staff of a separate utility authority/district
- `source`: city_website | county_website | utility_website |
  state_legislature | ballotpedia | clay_waterfall | linkedin | news | other
- Omit keys you have no value for (or use null). The helper assigns
  stakeholder_id, city/state, verified = FALSE, status = 'pending', and
  dedups against all of staging (so a legislator already staged for a
  neighboring municipality will be skipped — expected).

### Run report

End every research run with a short summary (this is what the scheduled
task's notification shows):

- Per municipality: rows inserted (email+LinkedIn / email only /
  LinkedIn only), duplicates skipped, notable gaps (e.g. "no email for
  2 board members").
- Anyone flagged as already in HubSpot.
- "N contacts waiting for review on the dashboard."

## Sync mode (interactive only — never from the scheduled task)

1. `approved`. Empty → say so and stop.
2. For each row, look up HubSpot for an existing contact: by
   `ipi_stakeholder_id`, then email, then first + last name with matching
   `ipi_municipality_key` or state. Matches → update, not create. This
   matters most for LinkedIn-only contacts, which have no email to dedup on.
3. Show the user a table (name, role, municipality, email?, LinkedIn?,
   create vs update) and wait for a clear yes.
4. Create/update via the HubSpot connector, ≤10 per call, with:
   `firstname`, `lastname`, `email`, `phone`, `jobtitle` (= role_title),
   `city` + `state` (full state name, e.g. "New Mexico"), `hs_linkedin_url`,
   `lifecyclestage = lead`, `ipi_audience_segment`, `ipi_municipality_key`,
   `ipi_role_category`, `ipi_committee`, `ipi_confidence`,
   `ipi_stakeholder_id`.
5. `mark-synced SID=CID ...` for every contact that landed.
6. Tell the user how many LinkedIn-only contacts were added, since those
   need a HubSpot → HeyReach export (filter on `ipi_municipality_key` and
   LinkedIn URL known, email unknown).

LinkedIn-only contacts *do* go to HubSpot (unlike `hubspot_sync.py`, which
skips them) so HubSpot stays the single list HeyReach exports come from.
