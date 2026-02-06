import json
import requests

URL = "https://api.usaspending.gov/api/v2/references/toptier_agencies/"

def main():
    r = requests.get(URL, timeout=30)
    print("status_code:", r.status_code)
    r.raise_for_status()

    data = r.json()
    results = data.get("results", [])
    print("results_count:", len(results))

    if results:
        first = results[0]
        print(
            "first_agency_example:",
            json.dumps(
                {
                    "agency_name": first.get("agency_name"),
                    "toptier_code": first.get("toptier_code"),
                },
                indent=2,
            ),
        )

if __name__ == "__main__":
    main()
