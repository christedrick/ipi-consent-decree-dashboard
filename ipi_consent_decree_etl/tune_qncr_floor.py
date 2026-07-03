"""One-off analysis: distribution of QNCR noncompliance across municipal
facilities, to pick a noise floor for the DMR signal (Layer 1, V2).

Downloads npdes_downloads.zip once, caches the three needed CSVs to
./qncr_cache/, and prints a (snc_quarters x e90_total) matrix of facility
counts so the floor can be chosen from real data.
"""

import os
from collections import Counter

from etl_bulk import (
    NPDES_DOWNLOADS_URL, download_zip, read_csv_from_zip,
    MUNICIPAL_SIC_CODES, _parse_float, log,
)
from datetime import date

CACHE_DIR = os.path.join(os.path.dirname(__file__), "qncr_cache")


def load_cached_or_download():
    import csv, io
    names = ["NPDES_QNCR_HISTORY.csv", "ICIS_FACILITIES.csv", "NPDES_SICS.csv"]
    if all(os.path.exists(os.path.join(CACHE_DIR, n)) for n in names):
        log.info("Using cached CSVs from %s", CACHE_DIR)
        out = {}
        for n in names:
            with open(os.path.join(CACHE_DIR, n), encoding="utf-8") as f:
                out[n] = list(csv.DictReader(f))
        return out

    zip_bytes = download_zip(NPDES_DOWNLOADS_URL, "NPDES (npdes_downloads.zip)")
    os.makedirs(CACHE_DIR, exist_ok=True)
    out = {}
    for n in names:
        rows = read_csv_from_zip(zip_bytes, n)
        out[n] = rows
        if rows:
            with open(os.path.join(CACHE_DIR, n), "w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=rows[0].keys())
                w.writeheader()
                w.writerows(rows)
    return out


def main():
    data = load_cached_or_download()
    facilities = data["ICIS_FACILITIES.csv"]
    sics = data["NPDES_SICS.csv"]
    qncr = data["NPDES_QNCR_HISTORY.csv"]

    # Municipal universe (same logic as process_npdes)
    fac_by_npdes = {(f.get("NPDES_ID") or "").strip(): f for f in facilities}
    municipal = set()
    for s in sics:
        if (s.get("SIC_CODE") or "").strip() in MUNICIPAL_SIC_CODES:
            municipal.add((s.get("NPDES_ID") or "").strip())
    KEYWORDS = ["water", "sewer", "wastewater", "wwtp", "potw", "sanitary",
                "utility", "utilities", "treatment", "reclamation", "aqueduct",
                "waterworks", "msd", "mwrd", "water authority", "sewer authority",
                "water district", "sewer district", "metropolitan", "public works",
                "stormwater", "drainage"]
    for nid, fac in fac_by_npdes.items():
        if nid in municipal:
            continue
        name = (fac.get("FACILITY_NAME") or "").lower()
        if any(k in name for k in KEYWORDS):
            municipal.add(nid)
    print(f"Municipal facilities: {len(municipal):,}")

    # Window: last 8 quarters
    today = date.today()
    y, q = today.year, (today.month - 1) // 3 + 1
    for _ in range(8):
        q -= 1
        if q == 0:
            y, q = y - 1, 4
    cutoff = y * 10 + q

    agg = {}
    for row in qncr:
        nid = (row.get("NPDES_ID") or "").strip()
        if nid not in municipal:
            continue
        try:
            yq = int((row.get("YEARQTR") or "0").strip())
        except ValueError:
            continue
        if yq < cutoff:
            continue
        e90 = int(_parse_float(row.get("NUME90Q") or "0"))
        hlrnc = (row.get("HLRNC") or "").strip().upper()
        is_snc = hlrnc in {"S", "T", "X", "D", "E", "U", "V"}
        if e90 <= 0 and not is_snc:
            continue
        a = agg.setdefault(nid, {"e90": 0, "snc": 0, "qtrs": 0})
        a["e90"] += e90
        a["snc"] += 1 if is_snc else 0
        a["qtrs"] += 1

    print(f"Facilities with any noncompliance in window: {len(agg):,}\n")

    def bucket_e90(v):
        for lo, hi, lbl in [(0, 0, "0"), (1, 5, "1-5"), (6, 19, "6-19"),
                            (20, 49, "20-49"), (50, 10**9, "50+")]:
            if lo <= v <= hi:
                return lbl

    def bucket_snc(v):
        return {0: "0", 1: "1", 2: "2", 3: "3"}.get(v, "4+") if v < 4 else "4+"

    matrix = Counter()
    for a in agg.values():
        matrix[(bucket_snc(a["snc"]), bucket_e90(a["e90"]))] += 1

    e90_cols = ["0", "1-5", "6-19", "20-49", "50+"]
    print(f"{'snc_qtrs':>9} | " + " | ".join(f"e90 {c:>6}" for c in e90_cols))
    print("-" * 70)
    for snc_row in ["0", "1", "2", "3", "4+"]:
        cells = [f"{matrix.get((snc_row, c), 0):>10,}" for c in e90_cols]
        print(f"{snc_row:>9} | " + " | ".join(cells))

    # Candidate floors
    print("\nCandidate floor yields:")
    floors = [
        ("current (e90>=3 or snc>=1)", lambda a: a["e90"] >= 3 or a["snc"] >= 1),
        ("snc>=2", lambda a: a["snc"] >= 2),
        ("snc>=2 or e90>=20", lambda a: a["snc"] >= 2 or a["e90"] >= 20),
        ("snc>=3 or e90>=30", lambda a: a["snc"] >= 3 or a["e90"] >= 30),
        ("snc>=4 or e90>=50", lambda a: a["snc"] >= 4 or a["e90"] >= 50),
        ("(snc>=2 and e90>=10) or e90>=40", lambda a: (a["snc"] >= 2 and a["e90"] >= 10) or a["e90"] >= 40),
    ]
    for label, fn in floors:
        n = sum(1 for a in agg.values() if fn(a))
        print(f"  {label:35} -> {n:,}")


if __name__ == "__main__":
    main()
