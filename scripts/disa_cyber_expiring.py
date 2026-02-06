import csv
import json
import os
import random
import re
import time
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
    # USAspending sometimes returns dicts/lists for PSC/NAICS, etc.
    if isinstance(v, dict):
        # common shapes: {"code": "...", "description": "..."} or {"code": "...", "name": "..."}
        code = v.get("code")
        desc = v.get("description") or v.get("name")
        if code and desc:
            return f"{code} - {desc}"
        if code:
            return str(code)
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, list):
        return "; ".join(_normalize(x) for x in v)
    return "" if v is None else str(v)


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")


def _date_only(s):
    """
    Return YYYY-MM-DD if it exists at the front of the string, else "".
    Works for "2026-02-01" and "2026-02-01T00:00:00" etc.
    """
    if not s:
        return ""
    m = _DATE_RE.match(str(s))
    return m.group(0) if m else ""


def _get_end_date(row):
    # Prefer the label you requested, but fall back to other common keys if present.
    return (
        row.get("End Date")
        or row.get("period_of_performance_current_end_date")
        or row.get("period_of_performance_potential_end_date")
        or row.get("Period of Performance Current End Date")
        or row.get("Period of Performance Potential End Date")
        or ""
    )


def _matches_agency(row, patterns):
    """
    patterns: list of lowercase substrings (e.g. ["defense information systems agency", "disa"])
    checks awarding/funding agency + sub agency fields.
    """
    if not patterns:
        return True

    hay = " | ".join(
        [
            str(row.get("Awarding Agency", "")),
            str(row.get("Awarding Sub Agency", "")),
            str(row.get("Funding Agency", "")),
            str(row.get("Funding Sub Agency", "")),
        ]
    ).lower()

    return any(p in hay for p in patterns)


def _build_session():
    # Retries help with RemoteDisconnected / transient network issues.
    retry = Retry(
        total=8,
        connect=8,
        read=8,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)

    s = requests.Session()
    s.mount("https://", adapter)
    s.headers.update(
        {
            "User-Agent": "disa-intel-engine/1.0",
            "Accept": "application/json",
        }
    )
    return s


def main():
    # Window
    start_dt = date.today()
    horizon_days = int(os.getenv("HORIZON_DAYS", "365"))
    end_dt = start_dt + timedelta(days=horizon_days)

    start = start_dt.isoformat()
    end = end_dt.isoformat()

    # PSC codes
    psc_codes = [s.strip() for s in os.getenv("PSC_CODES", "D310").split(",") if s.strip()]

    # Agency match (pipe-separated)
    raw_match = os.getenv("AGENCY_MATCH", "").strip()
    agency_patterns = [p.strip().lower() for p in raw_match.split("|") if p.strip()]

    max_pages = int(os.getenv("MAX_PAGES", "200"))
    debug_sample = os.getenv("DEBUG_SAMPLE", "0").strip() in ("1", "true", "TRUE", "yes", "YES")

    # IMPORTANT: sort DESC so we hit recent/future end dates sooner,
    # then stop once results fall below our window start.
    body = {
        "subawards": False,
        "limit": 100,
        "page": 1,
        "sort": "End Date",
        "order": "desc",
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "psc_codes": psc_codes,
        },
        "fields": FIELDS,
    }

    print(f"Query window (End Date): {start} to {end}")
    print(f"PSC codes: {psc_codes}")
    print(f"Agency match patterns: {agency_patterns if agency_patterns else '(none)'}")
    print(f"Max pages: {max_pages}")

    session = _build_session()

    os.makedirs("output", exist_ok=True)
    outpath = "output/disa_cyber_expiring.csv"

    all_scanned = 0
    kept = []

    page = 1
    while True:
        body["page"] = page

        # Small jitter helps if the API is touchy under load
        if page > 1:
            time.sleep(0.2 + random.random() * 0.3)

        r = session.post(API_URL, json=body, timeout=60)

        # If we get a non-200, print the body to logs (helps debugging)
        if r.status_code != 200:
            print(f"HTTP {r.status_code} on page {page}")
            try:
                print("Response:", r.text[:1000])
            except Exception:
                pass
            r.raise_for_status()

        data = r.json()
        rows = data.get("results", []) or []
        meta = data.get("page_metadata", {}) or {}

        if page == 1 and debug_sample:
            # Save a raw sample so you can see what keys/values are coming back.
            with open("output/debug_page1.json", "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "page_metadata": meta,
                        "sample_count": min(len(rows), 5),
                        "sample_rows": rows[:5],
                    },
                    f,
                    ensure_ascii=False,
                    indent=2,
                )

        print(f"Page {page}: {len(rows)} results (hasNext={meta.get('hasNext')})")

        if not rows:
            break

        all_scanned += len(rows)

        # Track the smallest End Date on this page (DESC order).
        page_end_dates = []
        for row in rows:
            ed = _date_only(_get_end_date(row))
            if ed:
                page_end_dates.append(ed)

            # Filter: agency text
            if not _matches_agency(row, agency_patterns):
                continue

            # Filter: end date window
            if not ed:
                continue
            if not (start <= ed <= end):
                continue

            kept.append(row)

        # Early stop: since we sort DESC, once the *minimum* end date on the page is below start,
        # remaining pages will only get older.
        if page_end_dates:
            min_ed = min(page_end_dates)
            if min_ed < start:
                print(f"Stopping early: page minimum End Date {min_ed} < window start {start}")
                break

        if not meta.get("hasNext"):
            break

        page += 1
        if page > max_pages:
            print(f"Stopping at {max_pages} pages (MAX_PAGES safety stop).")
            break

    # Write CSV
    with open(outpath, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for row in kept:
            w.writerow({k: _normalize(row.get(k, "")) for k in FIELDS})

    print(f"Scanned: {all_scanned} rows total.")
    print(f"Kept:   {len(kept)} rows in End Date window (and agency match).")
    print(f"Wrote:  {outpath}")


if __name__ == "__main__":
    main()
