#!/usr/bin/env python3
"""
Fetch SA (reporter 710) exports to all 27 EU member states for 2022, 2023, 2024
from the UN Comtrade public preview API.
Filter to customsCode == "C00" (standard total) to avoid double-counting.
Output: trade/data.json — array of {partner_name, iso2, year, value_usd}

Run with: python3 fetch_trade_data.py
"""

import urllib.request
import urllib.error
import json
import time
import os

# ── EU27 Comtrade partner codes ───────────────────────────────────────────────
# key = Comtrade numeric code, value = (ISO-2, display name)
EU27 = {
    40:  ("AT", "Austria"),
    56:  ("BE", "Belgium"),
    100: ("BG", "Bulgaria"),
    191: ("HR", "Croatia"),
    196: ("CY", "Cyprus"),
    203: ("CZ", "Czech Republic"),
    208: ("DK", "Denmark"),
    233: ("EE", "Estonia"),
    246: ("FI", "Finland"),
    251: ("FR", "France"),
    276: ("DE", "Germany"),
    300: ("GR", "Greece"),
    348: ("HU", "Hungary"),
    372: ("IE", "Ireland"),
    380: ("IT", "Italy"),
    428: ("LV", "Latvia"),
    440: ("LT", "Lithuania"),
    442: ("LU", "Luxembourg"),
    470: ("MT", "Malta"),
    528: ("NL", "Netherlands"),
    616: ("PL", "Poland"),
    620: ("PT", "Portugal"),
    642: ("RO", "Romania"),
    703: ("SK", "Slovakia"),
    705: ("SI", "Slovenia"),
    724: ("ES", "Spain"),
    752: ("SE", "Sweden"),
}

PERIODS = "2022,2023,2024"
REPORTER = 710          # South Africa
FLOW = "X"              # Exports
CMD = "TOTAL"
BASE = "https://comtradeapi.un.org/public/v1/preview/C/A/HS"

# Split into batches of 9 to stay well under the 500-row preview limit
all_codes = list(EU27.keys())
BATCH_SIZE = 9
batches = [all_codes[i:i+BATCH_SIZE] for i in range(0, len(all_codes), BATCH_SIZE)]


def fetch_batch(codes):
    partner_str = ",".join(str(c) for c in codes)
    url = (
        f"{BASE}"
        f"?reporterCode={REPORTER}"
        f"&partnerCode={partner_str}"
        f"&cmdCode={CMD}"
        f"&flowCode={FLOW}"
        f"&period={PERIODS}"
    )
    print(f"  GET {url}")
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=45) as r:
        return json.loads(r.read().decode("utf-8"))


records = []
for batch_num, batch in enumerate(batches, 1):
    print(f"\nBatch {batch_num}/{len(batches)} — {len(batch)} countries")
    try:
        resp = fetch_batch(batch)
        raw_count = resp.get("count", 0)
        rows = resp.get("data", [])
        print(f"  Received {raw_count} records")

        # Filter to standard customs total only
        c00_rows = [r for r in rows if r.get("customsCode") == "C00"]
        print(f"  After C00 filter: {len(c00_rows)} records")

        for row in c00_rows:
            pcode = row.get("partnerCode")
            year  = row.get("refYear")
            value = row.get("primaryValue")
            if pcode not in EU27 or value is None:
                continue
            iso2, name = EU27[pcode]
            records.append({
                "partner_name": name,
                "iso2": iso2,
                "year": year,
                "value_usd": round(value, 2),
            })

    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"  HTTP error {e.code}: {body[:300]}")
    except Exception as e:
        print(f"  Error: {e}")

    # Be polite to the public API between batches
    if batch_num < len(batches):
        time.sleep(1)

# ── Write output ──────────────────────────────────────────────────────────────
out_dir = os.path.join(os.path.dirname(__file__), "trade")
os.makedirs(out_dir, exist_ok=True)
out_path = os.path.join(out_dir, "data.json")

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(records, f, indent=2)

print(f"\n{'─'*60}")
print(f"Done. {len(records)} records written to {out_path}")

# Quick sanity check: show 2024 totals by country (top 10)
from collections import defaultdict
totals = defaultdict(float)
for r in records:
    if r["year"] == 2024:
        totals[r["partner_name"]] += r["value_usd"]

if totals:
    print("\nTop 10 SA export destinations (EU27, 2024, USD):")
    for name, val in sorted(totals.items(), key=lambda x: -x[1])[:10]:
        print(f"  {name:<20s}  ${val:>15,.0f}")
else:
    print("\nNo 2024 data found — check API response.")
