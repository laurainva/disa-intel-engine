import csv
import os
from datetime import date
import requests

API_URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"

def main():
    # Window: today through 2026-03-01
    start = date.today().isoformat()
    end = "2026-03-01"

    body = {
        "subawards": False,
        "limit": 100,
        "page": 1,
        "sort": "End Date",
        "order": "asc",
        "filters": {
            # Contract type codes (A-D) = procurement contracts (incl many task/delivery order records)
            "award_type_codes": ["A", "B", "C", "D"],

            # Cybersecurity PSC (D310). We'll start with strict D310, expand later if needed.
            "psc_codes": ["D310"],

            # Filter by period end window. In this endpoint, time_period is used for date-scoping.
            # We'll request End Date as a field and then enforce end-date filtering again locally
            # to be safe.
            "time_period": [{"start_date": start, "end_date": end}],
        },
        "fields": [
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
            "Description"
        ]
    }

    all_rows = []
    page = 1
    while True:
        body["page"] = page
        r = requests.post(API_URL, json=body, timeout=60)
        r.raise_for_status()
        data = r.json()
        rows = data.get("results", [])

        if not rows:
            break

        all_rows.extend(rows)

        meta = data.get("page_metadata", {})
        if not meta.get("hasNext"):
            break
        page += 1

        # Safety stop to avoid huge runs at this stage
        if page > 25:
            break

    # Enforce end-date cutoff locally (defensive)
    def in_window(row):
        ed = row.get("End Date") or ""
        return (ed >= start) and (ed <= end)

    filtered = [r for r in all_rows if in_window(r)]

    os.makedirs("output", exist_ok=True)
    outpath = "output/disa_cyber_expiring.csv"

    # Write CSV
    fields = body["fields"]
    with open(outpath, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in filtered:
            w.writerow({k: row.get(k, "") for k in fields})

    print(f"Pulled {len(all_rows)} rows, kept {len(filtered)} rows in window.")
    print(f"Wrote: {outpath}")

if __name__ == "__main__":
    main()
