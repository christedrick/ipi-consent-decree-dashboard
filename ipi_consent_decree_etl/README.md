# IPI Enforcement Intelligence ETL (V2)

Pulls EPA ECHO enforcement data for municipal water/wastewater systems,
enriches with Census population data, and loads into Google BigQuery.

Built for **IPI** to power a lead-generation and stakeholder-intelligence
system. V2 reframes the signal hierarchy: **state enforcement actions are the
primary (leading) signal**; federal consent decrees are kept as the secondary
signal; QNCR/DMR effluent noncompliance is a near-real-time discovery tier.

## V2 signal hierarchy

| Rank | signal_type | Source |
|------|-------------|--------|
| 1 | State Enforcement Action | NPDES formal enforcement (state-issued) |
| 2 | Federal Consent Decree | ICIS-FE&C + NPDES (EPA/DOJ decrees, CJAs) |
| 3 | Federal Enforcement Action | ICIS-FE&C (ACOs, penalty orders, etc.) |
| 4 | DMR Violation | NPDES_QNCR_HISTORY (persistent effluent noncompliance, no enforcement action yet) |

## V2 pipeline scripts (run order — see refresh.sh)

1. `etl_bulk.py` — bulk download + signal classification + QNCR/DMR tier
2. `seed_data.py` — hand-curated federal decree corrections
3. `deadline_lookup.py` — real compliance deadlines from DOJ/EPA sources
4. `populate_population.py --update` — Census population fill-in
5. `backfill_signals.py` — derived columns: signal_type/signal_rank on seed
   rows + `size_tier` (Small <100k / Medium 100k-500k / Large 500k+)
6. `incident_monitor.py` — news monitor (Google News RSS) for sewer
   overflows / boil-water / main breaks across Medium+Large targets →
   `incident_reports`; fresh incidents add +10 to priority score.
   **Cron daily for same-day outreach triggers** (refresh.sh also runs it):
   `0 7 * * * cd <etl dir> && ./venv/bin/python incident_monitor.py`
7. `export_targets.py` — **Layer 3a**: rebuilds `qualified_targets`
   (municipality-grain, priority-scored) + ensures `stakeholders_staging`
8. `validate_data.py --fix` — data quality checks

## Data freshness

- EPA publishes both bulk files weekly (typically Sat/Sun) — enforcement
  data lags reality by ≤ ~1 week at refresh time
- QNCR (DMR tier) is quarterly by nature — expect 1-4 months of lag; the
  news monitor exists precisely to close that gap to same-day
- Census ACS population: annual (fine for size tiers)

Additional (manual):
- `hubspot_sync.py` — **Layer 5**: pushes `approved` staging rows to HubSpot
  (needs `HUBSPOT_PRIVATE_APP_TOKEN` in .env)
- `tune_qncr_floor.py` — one-off QNCR noise-floor analysis (caches CSVs
  to `qncr_cache/`)

## Data Sources

- **EPA ECHO API** — Enforcement and Compliance History Online (free, no key required)
- **US Census Bureau ACS** — American Community Survey 5-year population estimates (free, key required)

## Setup

```bash
# 1. Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment variables
cp .env.example .env
# Edit .env with your values:
#   - GCP_PROJECT_ID: Your Google Cloud project ID
#   - GOOGLE_APPLICATION_CREDENTIALS: Path to your BigQuery service account JSON key
#   - CENSUS_API_KEY: Free key from https://api.census.gov/data/key_signup.html
```

## Usage

```bash
# Full run — all states, load to BigQuery
python etl.py

# Dry run — pull and transform only, no BigQuery write
python etl.py --dry-run

# Single state for testing
python etl.py --state TX

# Combine flags
python etl.py --dry-run --state TX
```

## Output

- **BigQuery**: `ipi_intelligence.consent_decrees` table with UPSERT logic (no duplicates on rerun)
- **Logs**: `logs/etl.log` (rotating, 5 MB max, 5 backups)
- **Console**: Real-time progress and run summary

## Schema

| Field | Type | Description |
|-------|------|-------------|
| case_number | STRING | Unique case identifier (merge key) |
| registry_id | STRING | EPA Registry ID |
| fips_code | STRING | County FIPS code |
| facility_name | STRING | Municipality / facility name |
| city | STRING | City |
| state | STRING | Two-letter state code |
| zip_code | STRING | ZIP code |
| county | STRING | County name |
| consent_decree_date | DATE | Date consent decree was issued |
| compliance_end_date | DATE | Compliance schedule end date |
| lead_agency | STRING | EPA Region, State, or DOJ |
| action_type | STRING | Type of enforcement action |
| violation_type | STRING | Violation description |
| penalty_amount | FLOAT64 | Penalty amount in USD |
| statute | STRING | Governing statute (CWA, SDWA) |
| population | INT64 | County population (Census ACS) |
| days_to_deadline | INT64 | Days until compliance deadline |
| urgency_tier | STRING | critical / high / medium / low / overdue / unknown |
| latitude | FLOAT64 | Facility latitude |
| longitude | FLOAT64 | Facility longitude |
| last_updated | TIMESTAMP | Last ETL refresh timestamp |
