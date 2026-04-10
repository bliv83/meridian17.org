#!/usr/bin/env python3
"""
Fetch SA (reporter 710) exports to all 27 EU member states for 2022, 2023, 2024
from the UN Comtrade public preview API.

Outputs:
  trade/data.json     — totals: [{partner_name, iso2, year, value_usd}]
  trade/products.json — HS2 breakdown: [{iso2, partner_name, year, hs2_code, hs2_desc, value_usd}]

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
YEARS   = [2022, 2023, 2024]
REPORTER = 710          # South Africa
FLOW = "X"              # Exports
BASE = "https://comtradeapi.un.org/public/v1/preview/C/A/HS"

# HS2 chapter descriptions (top 20 for SA-EU trade; remainder labelled "Other")
HS2_DESC = {
    "01": "Live animals",
    "02": "Meat & edible offal",
    "03": "Fish & seafood",
    "08": "Fruit & nuts",
    "09": "Coffee, tea & spices",
    "22": "Beverages",
    "26": "Ores, slag & ash",
    "27": "Mineral fuels & oils",
    "28": "Inorganic chemicals",
    "29": "Organic chemicals",
    "38": "Miscellaneous chemicals",
    "39": "Plastics",
    "41": "Hides & leather",
    "71": "Precious metals & stones",
    "72": "Iron & steel",
    "74": "Copper",
    "76": "Aluminium",
    "84": "Machinery & equipment",
    "85": "Electrical equipment",
    "87": "Vehicles",
}

all_codes = list(EU27.keys())


def fetch_url(url):
    print(f"  GET {url}")
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=45) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_totals_batch(codes):
    partner_str = ",".join(str(c) for c in codes)
    url = (
        f"{BASE}"
        f"?reporterCode={REPORTER}"
        f"&partnerCode={partner_str}"
        f"&cmdCode=TOTAL"
        f"&flowCode={FLOW}"
        f"&period={PERIODS}"
    )
    return fetch_url(url)


def fetch_products_batch(codes, year):
    partner_str = ",".join(str(c) for c in codes)
    url = (
        f"{BASE}"
        f"?reporterCode={REPORTER}"
        f"&partnerCode={partner_str}"
        f"&cmdCode=AG2"
        f"&flowCode={FLOW}"
        f"&period={year}"
        f"&customsCode=C00"
        f"&motCode=0"
    )
    return fetch_url(url)


# ── Fetch totals (TOTAL, all years, batch 9 partners) ────────────────────────
print("\n=== Fetching country totals ===")
TOTAL_BATCH = 9
total_batches = [all_codes[i:i+TOTAL_BATCH] for i in range(0, len(all_codes), TOTAL_BATCH)]

records = []
for batch_num, batch in enumerate(total_batches, 1):
    print(f"\nBatch {batch_num}/{len(total_batches)} — {len(batch)} countries")
    try:
        resp = fetch_totals_batch(batch)
        raw_count = resp.get("count", 0)
        rows = resp.get("data", [])
        print(f"  Received {raw_count} records")

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

    if batch_num < len(total_batches):
        time.sleep(1)

# ── Fetch HS2 product breakdown (AG2, C00, motCode=0, batch 5 partners/year) ─
print("\n=== Fetching HS2 product breakdown ===")
PRODUCT_BATCH = 5
prod_batches = [all_codes[i:i+PRODUCT_BATCH] for i in range(0, len(all_codes), PRODUCT_BATCH)]

product_records = []
total_prod_batches = len(prod_batches) * len(YEARS)
req_num = 0

for year in YEARS:
    for batch in prod_batches:
        req_num += 1
        print(f"\nRequest {req_num}/{total_prod_batches} — year={year}, {len(batch)} partners")
        try:
            resp = fetch_products_batch(batch, year)
            raw_count = resp.get("count", 0)
            rows = resp.get("data", [])
            print(f"  Received {raw_count} records")

            if raw_count >= 500:
                print(f"  WARNING: hit 500-row cap — data may be truncated")

            for row in rows:
                pcode = row.get("partnerCode")
                value = row.get("primaryValue")
                hs2   = str(row.get("cmdCode", "")).zfill(2)
                if pcode not in EU27 or value is None:
                    continue
                iso2, name = EU27[pcode]
                product_records.append({
                    "iso2":         iso2,
                    "partner_name": name,
                    "year":         year,
                    "hs2_code":     hs2,
                    "hs2_desc":     HS2_DESC.get(hs2, "Other"),
                    "value_usd":    round(value, 2),
                })

        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            print(f"  HTTP error {e.code}: {body[:300]}")
        except Exception as e:
            print(f"  Error: {e}")

        if req_num < total_prod_batches:
            time.sleep(1)

# ── Write output ──────────────────────────────────────────────────────────────
out_dir = os.path.join(os.path.dirname(__file__), "trade")
os.makedirs(out_dir, exist_ok=True)

data_path = os.path.join(out_dir, "data.json")
with open(data_path, "w", encoding="utf-8") as f:
    json.dump(records, f, indent=2)

products_path = os.path.join(out_dir, "products.json")
with open(products_path, "w", encoding="utf-8") as f:
    json.dump(product_records, f, indent=2)

print(f"\n{'─'*60}")
print(f"Done.")
print(f"  {len(records)} total records written to {data_path}")
print(f"  {len(product_records)} product records written to {products_path}")

# ── Sanity checks ─────────────────────────────────────────────────────────────
from collections import defaultdict

# Country totals top 10 (2024)
totals = defaultdict(float)
for r in records:
    if r["year"] == 2024:
        totals[r["partner_name"]] += r["value_usd"]

if totals:
    print("\nTop 10 SA export destinations (EU27, 2024, USD):")
    for name, val in sorted(totals.items(), key=lambda x: -x[1])[:10]:
        print(f"  {name:<20s}  ${val:>15,.0f}")

# Germany HS2 top 10 (2024)
de_prods = defaultdict(float)
for r in product_records:
    if r["iso2"] == "DE" and r["year"] == 2024:
        de_prods[f"HS{r['hs2_code']} {r['hs2_desc']}"] += r["value_usd"]

if de_prods:
    print("\nTop 10 SA → Germany products (HS2, 2024, USD):")
    for label, val in sorted(de_prods.items(), key=lambda x: -x[1])[:10]:
        print(f"  {label:<35s}  ${val:>15,.0f}")
else:
    print("\nNo Germany product data found — check API response.")
