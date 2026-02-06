import csv
import os
from datetime import date, timedelta
import requests

API_URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"

FIELDS = [
    "Award ID",
    "Recipient Name",
    "Award Amount",
    "Awarding Agency",
    "Awarding Sub Agency",
    "Funding Agency",
    "Funding Sub Agency",
    "PSC",
    "NAICS",
    "Start Date",
    "End Date",
    "Last Modified Date",
    "Description",
]

def _normalize(v):
    # USAspending sometimes returns objects for PSC/NAICS, etc.
    if isinstance(v, dict):
        if "code" in v and "name" in v:
            return f"{v['code']} - {v['name']}"
        if "code" in v:
            return str(v["code"])
        return str(v)
    if isinstance(v, list):
        return "; ".join(_normalize(x) for x in v)
    return "" if v is None else str(v)

def _get_end_date(row):
    # Prefer your requested label, but fall back to common keys if needed
    return (
        row.get("End Date")
        or row.get("period_of_performance_current_end_date")
        or row.get("Period of Performance Current End Date")
        or row.get("period_of_performance_potential_end_date")
        or row.get("Period of Performance Potential End Date")
        or ""
    )

def _parse_ymd(s):
    """Parse YYYY-MM-DD (or ISO-ish) to a date; return None if invalid."""
    if not s:
        return None
    s = str(s).strip()
    if len(s) >= 10:
        s = s[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None

def main():
    start_dt = date.today()
    horizon_days = int(os.getenv("HORIZON_DAYS", "365"))
    end_dt = start_dt + timedelta(days=horizon_days)

    # PSC codes passed from workflow env
    psc_codes = [s.strip() for s in os.getenv("PSC_CODES", "D310").split(",") if s.strip()]

    # DISA filter (award OR funding)
    disa_agency_filters = [
        {"type": "awarding", "tier": "subtier", "name": "Defense Information Systems Agency"},
        {"type": "funding",  "tier": "subtier", "name": "Defense Information Systems Agency"},
    ]

    body = {
        "subawards": False,
        "limit": 100,
        "page": 1,
        "sort": "End Date",
        "order": "asc",
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "psc_codes": psc_codes,
            "agencies": disa_agency_filters,
            # NOTE: Intentionally NOT using time_period here.
            # We filter by End Date locally below.
        },
        "fields": FIELDS,
    }

    print(f"End Date window: {start_dt.isoformat()} to {end_dt.isoformat()}")
    print(f"PSC codes: {psc_codes}")
    print("Agency filter (DISA): Defense Information Systems Agency (awarding or funding)")

    all_rows = []
    page = 1

    while True:
        body["page"] = page
        r = requests.post(API_URL, json=body, timeout=60)
        r.raise_for_status()
        data = r.json()

        rows = data.get("results", []) or []
        meta = data.get("page_metadata", {}) or {}

        if page == 1:
            print(f"Page 1 results: {len(rows)}")
            if rows:
                print(f"Sample keys: {list(rows[0].keys())[:15]}")
                print(f"Sample End Date: {_get_end_date(rows[0])}")

        if not rows:
            break

        all_rows.extend(rows)

        if not meta.get("hasNext"):
            break

        page += 1
        if page > 25:  # safety stop
            print("Stopping at 25 pages (safety stop).")
            break

    # Filter locally by End Date window (safe parsing)
    filtered = []
    for row in all_rows:
        ed_raw = _get_end_date(row)
        ed_dt = _parse_ymd(ed_raw)
        if ed_dt and (start_dt <= ed_dt <= end_dt):
            filtered.append(row)

    os.makedirs("output", exist_ok=True)
    outpath = "output/disa_cyber_expiring.csv"

    with open(outpath, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for row in filtered:
            w.writerow({k: _normalize(row.get(k, "")) for k in FIELDS})

    print(f"Pulled {len(all_rows)} rows total from API.")
    print(f"Kept  {len(filtered)} rows in End Date window.")
    print(f"Wrote: {outpath}")

    if len(all_rows) == 0:
        print("NOTE: API returned 0 rows. If this happens, DISA name matching may differ in USAspending.")
        print("      Next step would be to use DISA codes instead of name once we confirm them.")

if __name__ == "__main__":
    main()
