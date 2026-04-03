"""
Populate Population Data — IPI Consent Decree Dashboard

Downloads US Census Bureau population estimates (2024 vintage) and matches
them against city/state pairs in BigQuery that are missing population data.

Uses two Census data sources:
  1. Sub-county population estimates (cities, towns, villages, CDPs)
     - https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/cities/totals/
  2. County population estimates
     - https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/counties/totals/

Matching strategy (in priority order):
  1. Exact match: city name → Census place name (after normalization)
  2. County match: "X County" → Census county name
  3. Fuzzy match: longest common substring for remaining unmatched

Usage:
    python populate_population.py            # dry run — show matches
    python populate_population.py --update   # update BigQuery
"""

import csv
import io
import os
import re
import sys
from collections import defaultdict

import requests
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ipi-consent-decree-dashboard")
TABLE = f"{PROJECT_ID}.ipi_intelligence.consent_decrees"

# Census data URLs (2024 vintage — most recent as of April 2026)
PLACES_URL = "https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/cities/totals/sub-est2024.csv"
COUNTIES_URL = "https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/counties/totals/co-est2024-alldata.csv"

# State name → abbreviation mapping
STATE_ABBREVS = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "District of Columbia": "DC", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI",
    "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME",
    "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE",
    "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
    "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Puerto Rico": "PR",
    "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD",
    "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT",
    "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY",
    # Territories
    "American Samoa": "AS", "Guam": "GU",
    "Northern Mariana Islands": "MP", "U.S. Virgin Islands": "VI",
}


def normalize_city(name):
    """Normalize a city name for matching.

    Strips suffixes like 'city', 'town', 'village', 'borough', etc.,
    removes punctuation, and lowercases.
    """
    if not name:
        return ""
    s = name.lower().strip()
    # Remove common Census suffixes (longest first to avoid partial matches)
    suffixes = [
        " city and borough",  # Alaska consolidated entities (Juneau, Sitka)
        " consolidated government (balance)",
        " unified government (balance)",
        " consolidated government",
        " unified government",
        " metropolitan government",
        " metro government",
        " urban county",
        " municipality",
        " township",
        " borough",
        " village",
        " city",
        " town",
        " cdp",
    ]
    for suffix in suffixes:
        if s.endswith(suffix):
            s = s[: -len(suffix)]
            break
    # Remove parenthetical notes like "(pt.)" or "(balance)"
    s = re.sub(r"\s*\(.*?\)\s*", " ", s)
    # Remove punctuation
    s = re.sub(r"[.,'\"-]", "", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_county(name):
    """Normalize a county name for matching."""
    if not name:
        return ""
    s = name.lower().strip()
    # Remove "County" suffix
    s = re.sub(r"\s+county$", "", s)
    # Remove common prefixes/suffixes
    s = re.sub(r"\s+parish$", "", s)  # Louisiana
    s = re.sub(r"\s+borough$", "", s)  # Alaska
    s = re.sub(r"\s+census area$", "", s)  # Alaska
    s = re.sub(r"\s+municipality$", "", s)  # Alaska
    s = re.sub(r"[.,'\"-]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def download_census_places():
    """Download and parse Census sub-county population estimates.

    Returns dict: (normalized_city, state_abbrev) → population
    """
    print("Downloading Census place population estimates...")
    resp = requests.get(PLACES_URL, timeout=60)
    resp.raise_for_status()
    print(f"  Downloaded {len(resp.content) / 1024 / 1024:.1f} MB")

    reader = csv.DictReader(io.StringIO(resp.text))
    lookup = {}
    raw_names = {}  # for debugging

    for row in reader:
        sumlev = row.get("SUMLEV", "")
        # Only include places (162=incorporated places, 061=towns, 170/172=consolidated)
        if sumlev not in ("162", "061", "071", "157", "170", "172"):
            continue

        state_name = row.get("STNAME", "")
        state_abbrev = STATE_ABBREVS.get(state_name)
        if not state_abbrev:
            continue

        name = row.get("NAME", "")
        pop_str = row.get("POPESTIMATE2024", "0")
        try:
            pop = int(pop_str)
        except (ValueError, TypeError):
            continue

        if pop <= 0:
            continue

        norm = normalize_city(name)
        key = (norm, state_abbrev)
        # Keep the larger population if duplicate names
        if key not in lookup or pop > lookup[key]:
            lookup[key] = pop
            raw_names[key] = name

    print(f"  Loaded {len(lookup)} place entries")
    return lookup, raw_names


def download_census_counties():
    """Download and parse Census county population estimates.

    Returns dict: (normalized_county, state_abbrev) → population
    """
    print("Downloading Census county population estimates...")
    resp = requests.get(COUNTIES_URL, timeout=60)
    resp.raise_for_status()
    print(f"  Downloaded {len(resp.content) / 1024 / 1024:.1f} MB")

    reader = csv.DictReader(io.StringIO(resp.text))
    lookup = {}

    for row in reader:
        sumlev = row.get("SUMLEV", "")
        if sumlev != "050":  # County level only
            continue

        state_name = row.get("STNAME", "")
        state_abbrev = STATE_ABBREVS.get(state_name)
        if not state_abbrev:
            continue

        county_name = row.get("CTYNAME", "")
        pop_str = row.get("POPESTIMATE2024", "0")
        try:
            pop = int(pop_str)
        except (ValueError, TypeError):
            continue

        if pop <= 0:
            continue

        norm = normalize_county(county_name)
        key = (norm, state_abbrev)
        lookup[key] = pop

    print(f"  Loaded {len(lookup)} county entries")
    return lookup


def get_missing_populations(client):
    """Get distinct city/state pairs missing population from BigQuery."""
    q = f"""
    SELECT DISTINCT city, state
    FROM `{TABLE}`
    WHERE (population IS NULL OR population = 0)
      AND city IS NOT NULL AND city != ''
      AND state IS NOT NULL AND state != ''
    ORDER BY state, city
    """
    rows = list(client.query(q).result())
    return [(r.city, r.state) for r in rows]


def match_populations(missing_pairs, place_lookup, county_lookup, place_raw_names):
    """Match city/state pairs against Census data.

    Returns list of (city, state, population, match_type, census_name) tuples.
    """
    matches = []
    unmatched = []

    for city, state in missing_pairs:
        city_upper = (city or "").upper().strip()
        state_upper = (state or "").upper().strip()

        # --- Strategy 1: Check if it's a county reference ---
        if "COUNTY" in city_upper:
            county_norm = normalize_county(city_upper)
            key = (county_norm, state_upper)
            if key in county_lookup:
                matches.append((city, state, county_lookup[key], "county", city_upper))
                continue

        # --- Strategy 2: Exact normalized match against places ---
        city_norm = normalize_city(city_upper)
        key = (city_norm, state_upper)
        if key in place_lookup:
            matches.append((
                city, state, place_lookup[key], "exact",
                place_raw_names.get(key, city_upper),
            ))
            continue

        # --- Strategy 3: Try removing common EPA prefixes ---
        # EPA data often has "CITY OF X" or "TOWN OF X" format
        alt = city_upper
        for prefix in ["CITY OF ", "TOWN OF ", "VILLAGE OF ", "BOROUGH OF "]:
            if alt.startswith(prefix):
                alt = alt[len(prefix):]
                break
        if alt != city_upper:
            alt_norm = normalize_city(alt)
            alt_key = (alt_norm, state_upper)
            if alt_key in place_lookup:
                matches.append((
                    city, state, place_lookup[alt_key], "prefix_strip",
                    place_raw_names.get(alt_key, alt),
                ))
                continue

        # --- Strategy 4: Try appending "city" back for multi-word names ---
        # Handles "ALEXANDER CITY" → normalize_city("alexander city city")
        # strips "city" → "alexander city" which matches Census
        # Also try "X city" directly for names like "Oklahoma City"
        city_with_suffix = city_norm + " city"
        city_suffix_key = (city_with_suffix, state_upper)
        if city_suffix_key in place_lookup:
            matches.append((
                city, state, place_lookup[city_suffix_key], "name_has_city",
                place_raw_names.get(city_suffix_key, city_upper),
            ))
            continue

        # --- Strategy 5: Try matching just the first word(s) ---
        # Handles "METLAKATLA INDIAN COMMUNITY" → "Metlakatla"
        words = city_norm.split()
        found = False
        # Try progressively shorter prefixes (but at least 1 word)
        for n in range(len(words), 0, -1):
            partial = " ".join(words[:n])
            partial_key = (partial, state_upper)
            if partial_key in place_lookup:
                matches.append((
                    city, state, place_lookup[partial_key], "partial",
                    place_raw_names.get(partial_key, partial),
                ))
                found = True
                break
        if found:
            continue

        # --- Strategy 6: Try county match without "COUNTY" suffix ---
        # Match city name directly against county names (handles borough
        # and parish names too)
        county_key = (city_norm, state_upper)
        if county_key in county_lookup:
            matches.append((city, state, county_lookup[county_key], "county_infer", city_upper))
            continue

        # --- Strategy 7: Try "saint" / "st" variants ---
        if city_norm.startswith("st "):
            saint_key = ("saint " + city_norm[3:], state_upper)
            if saint_key in place_lookup:
                matches.append((
                    city, state, place_lookup[saint_key], "saint_variant",
                    place_raw_names.get(saint_key, city_upper),
                ))
                continue
        elif city_norm.startswith("saint "):
            st_key = ("st " + city_norm[6:], state_upper)
            if st_key in place_lookup:
                matches.append((
                    city, state, place_lookup[st_key], "saint_variant",
                    place_raw_names.get(st_key, city_upper),
                ))
                continue

        # --- Strategy 8: Try "fort" / "ft" variants ---
        if city_norm.startswith("ft "):
            fort_key = ("fort " + city_norm[3:], state_upper)
            if fort_key in place_lookup:
                matches.append((
                    city, state, place_lookup[fort_key], "fort_variant",
                    place_raw_names.get(fort_key, city_upper),
                ))
                continue
        elif city_norm.startswith("fort "):
            ft_key = ("ft " + city_norm[5:], state_upper)
            if ft_key in place_lookup:
                matches.append((
                    city, state, place_lookup[ft_key], "fort_variant",
                    place_raw_names.get(ft_key, city_upper),
                ))
                continue

        # --- Strategy 9: Try "mount" / "mt" variants ---
        if city_norm.startswith("mt "):
            mount_key = ("mount " + city_norm[3:], state_upper)
            if mount_key in place_lookup:
                matches.append((
                    city, state, place_lookup[mount_key], "mount_variant",
                    place_raw_names.get(mount_key, city_upper),
                ))
                continue
        elif city_norm.startswith("mount "):
            mt_key = ("mt " + city_norm[6:], state_upper)
            if mt_key in place_lookup:
                matches.append((
                    city, state, place_lookup[mt_key], "mount_variant",
                    place_raw_names.get(mt_key, city_upper),
                ))
                continue

        unmatched.append((city, state))

    return matches, unmatched


def update_bigquery(client, matches):
    """Update BigQuery with matched population data."""
    if not matches:
        print("No matches to update.")
        return 0

    # Build CASE statement for batch update
    # Group by population value to reduce query size
    pop_groups = defaultdict(list)
    for city, state, pop, match_type, census_name in matches:
        pop_groups[pop].append((city, state))

    # Build update in batches (BigQuery has query size limits)
    total_updated = 0
    batch_size = 200
    all_items = list(matches)

    for i in range(0, len(all_items), batch_size):
        batch = all_items[i : i + batch_size]
        cases = []
        whens = []
        for city, state, pop, match_type, census_name in batch:
            city_escaped = city.replace("'", "\\'")
            whens.append(
                f"WHEN city = '{city_escaped}' AND state = '{state}' THEN {pop}"
            )

        # Build WHERE clause for this batch
        conditions = []
        for city, state, pop, match_type, census_name in batch:
            city_escaped = city.replace("'", "\\'")
            conditions.append(f"(city = '{city_escaped}' AND state = '{state}')")

        q = f"""
        UPDATE `{TABLE}`
        SET population = CASE
            {chr(10).join(whens)}
            ELSE population
        END
        WHERE ({' OR '.join(conditions)})
          AND (population IS NULL OR population = 0)
        """
        job = client.query(q)
        job.result()
        affected = job.num_dml_affected_rows or 0
        total_updated += affected
        print(f"  Batch {i // batch_size + 1}: updated {affected} records")

    return total_updated


def main():
    do_update = "--update" in sys.argv

    os.environ.setdefault(
        "GOOGLE_APPLICATION_CREDENTIALS",
        os.path.join(os.path.dirname(__file__), "service-account.json"),
    )
    client = bigquery.Client(project=PROJECT_ID)

    # Download Census data
    place_lookup, place_raw_names = download_census_places()
    county_lookup = download_census_counties()

    # Get missing populations from BigQuery
    print("\nQuerying BigQuery for records missing population...")
    missing = get_missing_populations(client)
    print(f"  Found {len(missing)} distinct city/state pairs without population")

    # Match
    print("\nMatching against Census data...")
    matches, unmatched = match_populations(
        missing, place_lookup, county_lookup, place_raw_names
    )

    # Report
    print(f"\n{'=' * 70}")
    print(f"POPULATION MATCHING REPORT")
    print(f"{'=' * 70}")
    print(f"Total city/state pairs:  {len(missing)}")
    print(f"Matched:                 {len(matches)} ({len(matches)/len(missing)*100:.1f}%)")
    print(f"Unmatched:               {len(unmatched)} ({len(unmatched)/len(missing)*100:.1f}%)")

    # Breakdown by match type
    by_type = defaultdict(int)
    for _, _, _, mt, _ in matches:
        by_type[mt] += 1
    print(f"\nMatch breakdown:")
    for mt, cnt in sorted(by_type.items(), key=lambda x: -x[1]):
        print(f"  {mt:<20} {cnt:>5}")

    # Show some matches
    print(f"\nSample matches:")
    for city, state, pop, mt, census_name in matches[:20]:
        print(f"  {city}, {state} → {pop:,} ({mt}: {census_name})")

    # Show unmatched
    if unmatched:
        print(f"\nUnmatched ({len(unmatched)}):")
        for city, state in unmatched[:30]:
            print(f"  {city}, {state}")
        if len(unmatched) > 30:
            print(f"  ... and {len(unmatched) - 30} more")

    # Update BigQuery
    if do_update:
        print(f"\n{'=' * 70}")
        print("UPDATING BIGQUERY...")
        total = update_bigquery(client, matches)
        print(f"Total records updated: {total}")
    else:
        print(f"\nDry run — use --update to write to BigQuery")

    return 0


if __name__ == "__main__":
    sys.exit(main())
