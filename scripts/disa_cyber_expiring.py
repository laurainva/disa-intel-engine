import csv
import os
from datetime import date, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

def _build_session():
    s = requests.Session()

    retry = Retry(
        total=6,
        connect=6,
        read=6,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)

    s.headers.update(
        {
            "User-Agent": "disa-intel-engine/0.1 (github-actions)",
            "Accept": "application/json",
        }
    )
    return s

def _env_list(name, default):
    raw = os.getenv(name, default)
    return [x.strip() for x in raw.split(",") if x.strip()]

def _truthy(name, default="0"):
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "y")

def main():
    start_dt = date.today()

    end_date_override = os.getenv("END_DATE", "").strip()
    if end_date_override:
        end_dt = date.fromisoformat(end_date_override)
    else:
        horizon_days = int(os.getenv("HORIZON_DAYS", "365"))
        end_dt = start_dt + timedelta(days=horizon_days)

    start = start_dt.isoformat()
    end = end_dt.isoformat()

    psc_codes = _env_list("PSC_CODES", "D310")
    max_pages = int(os.getenv("MAX_PAGES", "25"))
    disa_only = _truthy("DISA_ONLY", "0")

    body = {
        "subawards": False,
        "limit": 100,
        "page": 1,
        "sort": "End Date",
        "order": "asc",
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "psc_codes": psc_codes,
        },
        "fields": FIELDS,
    }

    # Try to narrow to DISA at query-time.
    # If the API rejects the agencies filter (400), we fall back to local filtering.
    if disa_only:
        body["filters"]["agencies"] = [
            {
                "type": "awarding",
                "tier": "subtier",
                "name": "Defense Information Systems Agency",
                "toptier_name": "Department of Defense",
            }
        ]

    print(f"Query window (End Date): {start} to {end}")
    print(f"PSC codes: {psc_codes}")
    print(f"DISA_ONLY: {disa_only}")
    print(f"MAX_PAGES: {max_pages}")

    session = _build_session()

    all_rows = []
    page = 1

    while True:
        body["page"] = page

        try:
            r = session.post(API_URL, json=body, timeout=(10, 120))
            # If agencies filter isn't accepted, retry without it
            if r.status_code == 400 and disa_only and "agencies" in body["filters"]:
                print("API rejected agencies filter; retrying without it (will filter DISA locally).")
                del body["filters"]["agencies"]
                disa_only = False
                continue

            r.raise_for_status()
            data = r.json()

        except requests.RequestException as e:
            # Fail fast (workflow will show the error), but you’ll usually avoid this due to retries.
            raise SystemExit(f"Request failed on page {page}: {e}")

        rows = data.get("results", []) or []
        meta = data.get("page_metadata", {}) or {}

        if page == 1:
            print(f"Page 1 results: {len(rows)}")
            if rows:
                print(f"Sample keys: {list(rows[0].keys())[:15]}")

        if not rows:
            break

        all_rows.extend(rows)

        if not meta.get("hasNext"):
            break

        page += 1
        if page > max_pages:
            print(f"Stopping at {max_pages} pages (safety stop).")
            break

    # Filter locally by End Date window (defensive)
    filtered = []
    for row in all_rows:
        ed = _get_end_date(row)
        if ed and (start <= ed <= end):
            filtered.append(row)

    # Optional local DISA filter if you did NOT use query-time agencies filter
    # (or if it was rejected). This keeps the output focused if you want DISA-only.
    if _truthy("DISA_ONLY", "0"):
        def is_disa(row):
            hay = " ".join(
                [
                    str(row.get("Awarding Sub Agency", "")),
                    str(row.get("Funding Sub Agency", "")),
                    str(row.get("Awarding Agency", "")),
                    str(row.get("Funding Agency", "")),
                ]
            ).lower()
            return ("defense information systems agency" in hay) or ("disa" in hay)

        before = len(filtered)
        filtered = [r for r in filtered if is_disa(r)]
        print(f"Local DISA filter: {before} -> {len(filtered)} rows")

    os.makedirs("output", exist_ok=True)
    outpath = "output/disa_cyber_expiring.csv"

    with open(outpath, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for row in filtered:
            w.writerow({k: _normalize(row.get(k, "")) for k in FIELDS})

    print(f"Pulled {len(all_rows)} rows total (pre-window).")
    print(f"Kept  {len(filtered)} rows in End Date window.")
    print(f"Wrote: {outpath}")

if __name__ == "__main__":
    main()
