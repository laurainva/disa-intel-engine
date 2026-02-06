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
    return (
        row.get("End Date")
        or row.get("period_of_performance_current_end_date")
        or row.get("Period of Performance Current End Date")
        or row.get("period_of_performance_potential_end_date")
        or row.get("Period of Performance Potential End Date")
        or ""
    )

def _parse_ymd(s):
    if not s:
        return None
    s = str(s).strip()
    if len(s) >= 10:
        s = s[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None

def _build_session():
    # Retries for transient network errors + 5xx + 429
    retry = Retry(
        total=8,
        connect=8,
        read=8,
        backoff_factor=1.0,  # 1s, 2s, 4s, ...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["POST"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)

    s = requests.Session()
    s.mount("https://", adapter)
    s.headers.update({
        "User-Agent": "disa-intel-engine/0.1 (github-actions)",
        "Accept": "application/json",
        "Content-Type": "application/json",
    })
    return s

def main():
    start_dt = date.today()
    horizon_days = int(os.getenv("HORIZON_DAYS", "365"))
    end_dt = start_dt + timedelta(days=horizon_days)

    psc_codes = [s.strip() for s in os.getenv("PSC_CODES", "D310").split(",") if s.strip()]

    # DISA filter (awarding OR funding)
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
            # NOTE: no time_period; we filter by End Date locally
        },
        "fields": FIELDS,
    }

    print(f"End Date window: {start_dt.isoformat()} to {end_dt.isoformat()}")
    print(f"PSC codes: {psc_codes}")
    print("Agency filter: Defense Information Systems Agency (awarding or funding)")

    session = _build_session()

    all_rows = []
    page = 1

    while True:
        body["page"] = page

        # Use a connect/read timeout tuple (more reliable than a single number)
        resp = session.post(API_URL, json=body, timeout=(15, 120))

        # If we still got a non-200, print a small diagnostic
        if resp.status_code != 200:
            print("Non-200 status:", resp.status_code)
            # print only first ~300 chars to keep logs readable
            print("Response snippet:", (resp.text or "")[:300])
            resp.raise_for_status()

        data = resp.json()
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
        if page > 25:
            print("Stopping at 25 pages (safety stop).")
            break

    filtered = []
    for row in all_rows:
        ed_dt = _parse_ymd(_get_end_date(row))
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
        print("NOTE: API returned 0 rows. If this persists, DISA name matching may differ in USAspending.")
        print("      Next step: use subtier codes instead of name once we identify them.")

if __name__ == "__main__":
    main()
