import os
import json
import csv
import time
from dotenv import load_dotenv
import requests

# Load RDM API key from .env
load_dotenv()
API_KEY = os.getenv("RDM_API_KEY")
if not API_KEY:
    raise RuntimeError("RDM_API_KEY not found. Create a .env file from .env.example and set your key.")

# Debug: Check if API key is loaded correctly
print("API key loaded:", "yes" if API_KEY else "no")
print("API key length:", len(API_KEY) if API_KEY else 0)
print("API key preview:", (API_KEY[:4] + "..." + API_KEY[-4:]) if API_KEY else "N/A")

# Exact base path and endpoints used by the RDM interactive tester
BASE_URL = "https://api1.raildata.org.uk/1010-historical-service-performance-_hsp_v1"
SERVICE_METRICS_URL = f"{BASE_URL}/api/v1/serviceMetrics"
SERVICE_DETAILS_URL = f"{BASE_URL}/api/v1/serviceDetails"

# Hardcoded test batch parameters (as requested)
FROM_LOC = "EXD"         # Exeter
TO_LOC = "PAD"           # London Paddington
FROM_TIME = "1100"
TO_TIME = "1400"
FROM_DATE = "2025-08-21"
TO_DATE = "2025-08-21"
DAYS = "WEEKDAY"

# Use exactly the same custom headers as the RDM console: x-apikey and content-type
HEADERS = {
    "x-apikey": API_KEY,
    "content-type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

def hhmm_to_minutes(s: str):
    """Parse 'HHMM' or 'HH:MM' or 'HH:MM:SS' into minutes after midnight. Returns None if missing."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    s = s.replace(":", "")
    if len(s) < 4:
        return None
    # Take the last 4 characters as HHMM to be robust to 'HHMMSS' or 'HHMM...' strings
    hh = int(s[-4:-2])
    mm = int(s[-2:])
    return hh * 60 + mm

def compute_delay_minutes(gbtt_pta: str, actual_ta: str):
    """Return actual arrival minus planned arrival in minutes. Early arrivals are negative."""
    p = hhmm_to_minutes(gbtt_pta)
    a = hhmm_to_minutes(actual_ta)
    if p is None or a is None:
        return None
    return a - p

def fetch_rids():
    """Call /serviceMetrics and return a list of RIDs. Also write rids.json."""
    payload = {
        "from_loc": FROM_LOC,
        "to_loc": TO_LOC,
        "from_time": FROM_TIME,
        "to_time": TO_TIME,
        "from_date": FROM_DATE,
        "to_date": TO_DATE,
        "days": DAYS
        # No toc_filter1, no tolerance2
    }
    
    # Debug: Print the exact payload being sent
    print("Sending payload:", json.dumps(payload, indent=2))
    print("URL:", SERVICE_METRICS_URL)
    print("Headers:", HEADERS)
    
    resp = requests.post(SERVICE_METRICS_URL, headers=HEADERS, json=payload, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"/serviceMetrics failed: {resp.status_code} {resp.text}")

    data = resp.json()
    services = data.get("Services", []) or []
    rids = []
    for svc in services:
        attrs = svc.get("serviceAttributesMetrics", {}) or {}
        for rid in attrs.get("rids", []) or []:
            if isinstance(rid, str):
                rids.append(rid)

    # Save to file exactly as requested
    with open("rids.json", "w", encoding="utf-8") as f:
        json.dump(rids, f, indent=2)

    print(f"Collected {len(rids)} RIDs and saved to rids.json")
    return rids

def fetch_details_for_rid(rid: str):
    """Call /serviceDetails for a single RID. Return a dict with CSV-ready fields."""
    payload = {"rid": rid}
    resp = requests.post(SERVICE_DETAILS_URL, headers=HEADERS, json=payload, timeout=60)
    if resp.status_code != 200:
        # Return a row indicating failure but keep processing others
        return {
            "rid": rid,
            "origin": None,
            "dest": TO_LOC,
            "gbtt_pta": None,
            "actual_ta": None,
            "delay_min": None,
            "date": None,
        }

    data = resp.json()
    details = (data or {}).get("serviceAttributesDetails", {}) or {}
    date_of_service = details.get("date_of_service")
    locations = details.get("locations", []) or []

    # Origin is the first location in the list if present
    origin = locations[0]["location"] if locations else None

    # Find the destination row for the target CRS code
    dest_row = next((loc for loc in locations if loc.get("location") == TO_LOC), None)

    gbtt_pta = dest_row.get("gbtt_pta") if dest_row else None
    actual_ta = dest_row.get("actual_ta") if dest_row else None
    delay_min = compute_delay_minutes(gbtt_pta, actual_ta) if gbtt_pta and actual_ta else None

    return {
        "rid": rid,
        "origin": origin,
        "dest": TO_LOC,
        "gbtt_pta": gbtt_pta,
        "actual_ta": actual_ta,
        "delay_min": delay_min,
        "date": date_of_service,
    }

def write_csv(rows):
    """Write all rows to delays_output.csv with the required schema."""
    fieldnames = ["rid", "origin", "dest", "gbtt_pta", "actual_ta", "delay_min", "date"]
    with open("delays_output.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    print(f"Wrote {len(rows)} rows to delays_output.csv")

def main():
    rids = fetch_rids()
    rows = []
    for i, rid in enumerate(rids, start=1):
        row = fetch_details_for_rid(rid)
        rows.append(row)
        # Small pause to be polite to the API
        time.sleep(0.2)
        if i % 10 == 0:
            print(f"Processed {i} of {len(rids)} RIDs")

    write_csv(rows)

if __name__ == "__main__":
    main()
