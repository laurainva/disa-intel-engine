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
        or row.get("period_of_performance_potential_end_date")
        or ""
    )

def main():
    start_dt = date.today()
    horizon_days = int(os.getenv("HORIZON_DAYS", "365"))  # change if you want
    end_dt = start_dt + timedelta(days=horizon_days)

    start = start_dt.isoformat()
    end = end_dt.isoformat()

    # You can pass PSC_CODES="D310,D311,D302" etc via workflow env
    psc_codes = [s.strip() for s in os.getenv("PSC_CODES", "D310").split(",") if s.strip()]

    body = {
        "subawards": False,
        "limit": 100,
        "page": 1,
        "sort": "End Date",
        "order": "asc",
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "psc_codes": psc_codes,
            # NOTE: Intentionally NOT using time_period here.
            # We filter by End Date locally below.
        },
        "fields": FIELDS,
    }

    print(f"Query window (End Date): {start} to {end}")
    print(f"PSC codes: {psc_codes}")

    all_rows = []
    page = 1

    while True:
        body["page"] = page
        r = requests.post(API_URL, json=body, timeout=60)
        r.raise_for_status()
        data = r.json()

        rows = data.get("results", [])
        meta = data.get("page_metadata", {}) or {}

        if page == 1:
            print(f"Page 1 results: {len(rows)}")
            # Optional: print a tiny sample of keys if something looks off
            if rows:
                print(f"Sample keys: {list(rows[0].keys())[:15]}")

        if not rows:
            break

        all_rows.extend(rows)

        if not meta.get("hasNext"):
            break

        page += 1
        if page > 25:  # safety stop
            print("Stopping at 25 pages (safety stop).")
            break

    # Filter locally by End Date window
    filtered = []
    for row in all_rows:
        ed = _get_end_date(row)
        if ed and (start <= ed <= end):
            filtered.append(row)

    os.makedirs("output", exist_ok=True)
    outpath = "output/disa_cyber_expiring.csv"

    with open(outpath, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for row in filtered:
            w.writerow({k: _normalize(row.get(k, "")) for k in FIELDS})

    print(f"Pulled {len(all_rows)} rows total.")
    print(f"Kept  {len(filtered)} rows in End Date window.")
    print(f"Wrote: {outpath}")

if __name__ == "__main__":
    main()
