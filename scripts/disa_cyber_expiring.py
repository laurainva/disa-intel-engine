import csv
import json
import os
import time
from datetime import date, datetime, timedelta
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

def _truthy(s: str) -> bool:
    return str(s).strip().lower() in ("1", "true", "yes", "y", "on")

def _normalize(v):
    # USAspending sometimes returns objects/lists for fields; keep CSV readable.
    if isinstance(v, dict):
        if "code" in v and "name" in v:
            return f"{v['code']} - {v['name']}"
        if "code" in v:
            return str(v["code"])
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, list):
        return "; ".join(_normalize(x) for x in v)
    return "" if v is None else str(v)

def _parse_iso_date(s):
    if not s:
        return None
    try:
        # Most USAspending dates are YYYY-MM-DD
        return datetime.fromisoformat(s[:10]).date()
    except Exception:
        return None

def _get_end_date(row):
    # Prefer "End Date" (your requested field)
    # but allow a couple of common fallbacks (sometimes appear in other endpoints/fields).
    return (
        row.get("End Date")
        or row.get("Period of Performance Current End Date")
        or row.get("Period of Performance Potential End Date")
        or ""
    )

def _is_disa(row) -> bool:
    # Most reliable: look for DISA in the sub-agency fields (and also check agency fields).
    needles = ("Defense Information Systems Agency", "DISA")
    hay_fields = (
        "Awarding Sub Agency",
        "Funding Sub Agency",
        "Awarding Agency",
        "Funding Agency",
    )
    for f in hay_fields:
        val = (row.get(f) or "")
        if any(n.lower() in val.lower() for n in needles):
            return True
    return False

def _post_with_retries(session, url, body, timeout=(20, 120), attempts=6):
    # Handles random disconnects + 429/5xx with backoff.
    for i in range(1, attempts + 1):
        try:
            r = session.post(url, json=body, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"retryable status {r.status_code}", response=r)
            return r
        except requests.RequestException as e:
            if i == attempts:
                raise
            sleep_s = min(60, 2 ** (i - 1))
            print(f"Request failed ({type(e).__name__}: {e}). Retry {i}/{attempts} in {sleep_s}s...")
            time.sleep(sleep_s)

def main():
    # Inputs from env
    horizon_days = int(os.getenv("HORIZON_DAYS", "365"))
    disa_only = _truthy(os.getenv("DISA_ONLY", "1"))
    psc_codes = [s.strip() for s in os.getenv("PSC_CODES", "D310").split(",") if s.strip()]
    max_pages = int(os.getenv("MAX_PAGES", "25"))

    start_dt = date.today()
    # Optional END_DATE override (useful for “through 2026-03-01” testing)
    end_date_override = os.getenv("END_DATE", "").strip()
    if end_date_override:
        end_dt = _parse_iso_date(end_date_override)
        if not end_dt:
            raise ValueError(f"END_DATE must be YYYY-MM-DD, got: {end_date_override!r}")
    else:
        end_dt = start_dt + timedelta(days=horizon_days)

    # Build request body (NOTE: we do NOT try to filter DISA inside the API call)
    body = {
        "subawards": False,
        "limit": 100,
        "page": 1,
        "sort": "End Date",
        "order": "asc",
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],  # procurement contracts
            "psc_codes": psc_codes,
        },
        "fields": FIELDS,
    }

    print(f"Query PSC codes: {psc_codes}")
    print(f"Filter DISA only: {disa_only}")
    print(f"End Date window: {start_dt.isoformat()} -> {end_dt.isoformat()}")

    session = requests.Session()
    session.headers.update({"User-Agent": "disa-intel-engine/1.0"})

    all_rows = []
    page = 1
    first_page_sample = None
    first_page_meta = None

    while True:
        body["page"] = page
        r = _post_with_retries(session, API_URL, body)
        r.raise_for_status()
        data = r.json()

        rows = data.get("results", []) or []
        meta = data.get("page_metadata", {}) or {}

        if page == 1:
            first_page_meta = meta
            first_page_sample = rows[:3]
            print(f"Page 1 results: {len(rows)}")
            if rows:
                print(f"Sample Award IDs: {[x.get('Award ID') for x in rows[:5]]}")

        if not rows:
            break

        all_rows.extend(rows)

        if not meta.get("hasNext"):
            break

        page += 1
        if page > max_pages:
            print(f"Stopping at {max_pages} pages (safety stop).")
            break

    # Local filters
    # 1) DISA only
    if disa_only:
        disa_rows = [row for row in all_rows if _is_disa(row)]
    else:
        disa_rows = list(all_rows)

    # 2) End Date window
    filtered = []
    for row in disa_rows:
        ed_raw = _get_end_date(row)
        ed = _parse_iso_date(ed_raw)
        if ed and (start_dt <= ed <= end_dt):
            filtered.append(row)

    # Write CSV
    os.makedirs("output", exist_ok=True)
    csv_path = "output/disa_cyber_expiring.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for row in filtered:
            w.writerow({k: _normalize(row.get(k, "")) for k in FIELDS})

    # Always write debug JSON (so if CSV is empty, you still see why)
    debug_path = "output/disa_cyber_expiring_debug.json"
    debug_obj = {
        "run_date": start_dt.isoformat(),
        "window": {"start": start_dt.isoformat(), "end": end_dt.isoformat()},
        "psc_codes": psc_codes,
        "disa_only": disa_only,
        "pages_pulled": page,
        "counts": {
            "pulled_total": len(all_rows),
            "after_disa_filter": len(disa_rows),
            "after_end_date_filter": len(filtered),
        },
        "first_page_metadata": first_page_meta,
        "first_page_sample": first_page_sample,
    }
    with open(debug_path, "w", encoding="utf-8") as f:
        json.dump(debug_obj, f, indent=2, ensure_ascii=False)

    print(f"Pulled total rows: {len(all_rows)}")
    print(f"After DISA filter: {len(disa_rows)}")
    print(f"After End Date window: {len(filtered)}")
    print(f"Wrote: {csv_path}")
    print(f"Wrote: {debug_path}")

if __name__ == "__main__":
    main()
