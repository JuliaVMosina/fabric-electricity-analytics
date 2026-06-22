"""
Local validation for the Finnish Electricity Market Analytics project.

Pulls a SMALL sample from each of the 3 open-data sources and prints what came back,
so you can confirm your API keys work *before* wiring anything up in Fabric.

Run (PowerShell):
    $env:FINGRID_API_KEY = "your-fingrid-key"
    $env:ENTSOE_TOKEN    = "your-entsoe-token"   # optional until it arrives
    python fetch_sample.py

FMI needs no key. ENTSO-E is skipped automatically if ENTSOE_TOKEN is unset.
"""

import os
import time
from datetime import datetime, timedelta, timezone
import xml.etree.ElementTree as ET

import requests

# Look back 7 days: some Fingrid series (e.g. consumption 363) lag by ~4 days,
# so a last-24h window returns nothing for them.
END = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
START = END - timedelta(days=7)

# Fingrid throttle: max 1 request / 2 seconds per key.
FINGRID_PAUSE_S = 2.1


def section(title):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


# ────────────────────────────────────────────────────────────────────
# 1. Fingrid — datasets API
#    Docs: https://data.fingrid.fi/en/instructions
#    GET https://data.fingrid.fi/api/datasets/{id}/data  (x-api-key header)
#    Verify each datasetId in the catalog: https://data.fingrid.fi/en/datasets
# ────────────────────────────────────────────────────────────────────
FINGRID_DATASETS = {
    # name -> datasetId.  NB: units and resolutions DIFFER — harmonized to MWh/hour in silver.
    "consumption":      124,   # Electricity consumption in Finland (TOTAL) — MWh/h, 15 min  (pairs with prod 74)
    "production_total":  74,   # Electricity production in Finland          — MWh/h, 15 min
    # (363 = consumption in distribution networks only — a subset; dropped in favour of total 124)
    "wind":             181,   # Wind power production (real-time)        — MW,    3 min
    "nuclear":          188,   # Nuclear power production (real-time)     — MW,    3 min
    "hydro":            191,   # Hydro power production (real-time)       — MW,    3 min
    "solar":            247,   # Solar power generation FORECAST          — MWh/h, 15 min  (label as forecast!)
}


def check_fingrid():
    section("FINGRID")
    key = os.environ.get("FINGRID_API_KEY")
    if not key:
        print("  SKIP — set $env:FINGRID_API_KEY first")
        return
    headers = {"x-api-key": key}
    params = {
        "startTime": START.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "endTime": END.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "format": "json",
        "pageSize": 5,
    }
    for i, (name, ds_id) in enumerate(FINGRID_DATASETS.items()):
        if i:
            time.sleep(FINGRID_PAUSE_S)  # respect 1 req / 2 s throttle
        url = f"https://data.fingrid.fi/api/datasets/{ds_id}/data"
        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
            if r.status_code == 200:
                payload = r.json()
                rows = payload.get("data", payload if isinstance(payload, list) else [])
                sample = rows[0] if rows else None
                print(f"  OK  {name:16s} (id {ds_id}): {len(rows)} rows  e.g. {sample}")
            else:
                print(f"  ERR {name:16s} (id {ds_id}): HTTP {r.status_code} — {r.text[:120]}")
        except Exception as e:
            print(f"  ERR {name:16s} (id {ds_id}): {e}")


# ────────────────────────────────────────────────────────────────────
# 2. ENTSO-E — day-ahead prices, bidding zone Finland
#    Docs: https://transparency.entsoe.eu/  (token via email, see README)
#    documentType A44 = day-ahead prices; domain 10YFI-1--------U = Finland
# ────────────────────────────────────────────────────────────────────
FI_ZONE = "10YFI-1--------U"


def check_entsoe():
    section("ENTSO-E (day-ahead price, Finland)")
    token = os.environ.get("ENTSOE_TOKEN")
    if not token:
        print("  SKIP — token not set yet (email transparency@entsoe.eu, ~3 working days)")
        return
    params = {
        "securityToken": token,
        "documentType": "A44",
        "in_Domain": FI_ZONE,
        "out_Domain": FI_ZONE,
        "periodStart": START.strftime("%Y%m%d%H%M"),
        "periodEnd": END.strftime("%Y%m%d%H%M"),
    }
    try:
        r = requests.get("https://web-api.tp.entsoe.eu/api", params=params, timeout=30)
        if r.status_code != 200:
            print(f"  ERR HTTP {r.status_code} — {r.text[:200]}")
            return
        # Strip namespace and count price points.
        root = ET.fromstring(r.content)
        prices = [e.text for e in root.iter() if e.tag.endswith("price.amount")]
        print(f"  OK  {len(prices)} price points; first few: {prices[:5]}")
    except Exception as e:
        print(f"  ERR {e}")


# ────────────────────────────────────────────────────────────────────
# 3. FMI — weather observations (no key), WFS time-value pairs
#    https://opendata.fmi.fi/wfs
# ────────────────────────────────────────────────────────────────────
def check_fmi():
    section("FMI (weather, Helsinki, temperature)")
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "getFeature",
        "storedquery_id": "fmi::observations::weather::timevaluepair",
        "place": "Helsinki",
        "parameters": "temperature",
        "starttime": START.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "endtime": END.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    try:
        r = requests.get("https://opendata.fmi.fi/wfs", params=params, timeout=30)
        if r.status_code != 200:
            print(f"  ERR HTTP {r.status_code} — {r.text[:200]}")
            return
        root = ET.fromstring(r.content)
        # keep only numeric measurement values (skips metadata like 'atmosphere')
        values = []
        for e in root.iter():
            if e.tag.endswith("value") and e.text:
                try:
                    values.append(float(e.text))
                except ValueError:
                    pass
        print(f"  OK  {len(values)} observations; first few: {values[:5]}")
    except Exception as e:
        print(f"  ERR {e}")


if __name__ == "__main__":
    print(f"Sample window (UTC): {START:%Y-%m-%d %H:%M} → {END:%Y-%m-%d %H:%M}")
    check_fingrid()
    check_entsoe()
    check_fmi()
    print("\nDone. Any 'ERR' above = fix that source before moving to Fabric.")
