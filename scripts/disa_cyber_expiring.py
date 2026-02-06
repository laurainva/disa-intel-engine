import csv
import os
from datetime import date
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

def main():
    # BROADEN query window so we actually get data back
    # NOTE: time_period is usually transaction/action date, not POP end date.
    query_start = "2018-01-01"
    query_end = "2026-03-01"

    # Broaden PSC beyond just D310 so we confirm the pipeline works.
    # You can tighten later once you see results.
    psc_codes = ["D310", "D399", "DA01", "DA10"]  # common-ish IT/cyber related buckets

    body = {
        "subawards": False,
        "limit": 100,
        "page": 1,
        "sort": "Award Amount",
        "order": "desc",
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "psc_codes": psc_codes,
            "time_period": [{"start_date": query_start, "end_date": query_end}],
        },
        "fields": FIELDS,
    }

    print("DEBUG query_start:", query_start, "query_end:", query_end)
    print("DEBUG psc_codes:", psc_codes)

    all_rows = []
    page = 1

    while True:
        body["page"] = page
        r = requests.post(API_URL, json=body, timeout=60)
        print("DEBUG status_code:", r.status_code)
        r.raise_for_status()

        data = r.json()
        rows = data.get("results", []) or []

        if page == 1:
            print("DEBUG page1_count:", len(rows))
            if rows:
                print("DEBUG sample_keys:", sorted(rows[0].keys()))
                print("DEBUG sample_end_date:", rows[0].get("End Date"))

        if not rows:
            break

        all_rows.extend(rows)

        meta = data.get("page_metadata", {}) or {}
        has_next = meta.get("hasNext")
        if has_next is None:
            has_next = meta.get("has_next")

        if not has_next:
            break

        page += 1
        if page > 5:  # keep it small for now
            print("DEBUG stopping at 5 pages for safety")
            break

    os.makedirs("output", exist_ok=True)
    outpath = "output/disa_cyber_expiring.csv"

    with open(outpath, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for row in all_rows:
            w.writerow({k: row.get(k, "") for k in FIELDS})

    print(f"Pulled {len(all_rows)} rows total (debug mode).")
    print(f"Wrote: {outpath}")

if __name__ == "__main__":
    main()
