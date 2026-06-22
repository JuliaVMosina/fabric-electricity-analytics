# Fabric notebook — 01_bronze_ingest
# ---------------------------------------------------------------------------
# Pulls raw data from open APIs and lands it AS-IS into the Lakehouse (bronze).
# No cleaning here — bronze keeps the source shape; transforms happen in silver.
#
# HOW TO USE IN FABRIC:
#   1. Create a Lakehouse called  electricity_lakehouse
#   2. New notebook, attach it to that lakehouse (default lakehouse)
#   3. Paste each "# ==== CELL ====" block into its own cell, run top to bottom
#
# Each block below = one notebook cell.
# ===========================================================================


# ==== CELL 1 — config ======================================================
# NB: paste your Fingrid key here to run, but DO NOT commit it / show it in
# screenshots. (For a published project, set it once, run, then clear the cell.)
FINGRID_API_KEY = "PASTE_YOUR_FINGRID_KEY_HERE"

# Ingestion window. Longer = more rows = more "Spark-worthy", but slower to pull.
START_DATE = "2024-01-01"      # YYYY-MM-DD (UTC)
END_DATE   = "2025-06-01"      # exclusive-ish upper bound

# Fingrid datasets (validated 2026-06-21). Units/resolutions differ on purpose
# — harmonised to MWh/hour in silver.
FINGRID_DATASETS = {
    "consumption":     124,    # total consumption, Finland     MWh/h, 15 min
    "production_total": 74,    # total production, Finland       MWh/h, 15 min
    "wind":            181,    # wind power (real-time)          MW,    3 min
    "nuclear":         188,    # nuclear power (real-time)       MW,    3 min
    "hydro":           191,    # hydro power (real-time)         MW,    3 min
    "solar":           247,    # solar generation FORECAST       MWh/h, 15 min
}

FINGRID_PAUSE_S = 2.1          # throttle: 1 request / 2 s per key
PAGE_SIZE = 20000              # > rows-per-month at 3-min, so 1 request per month chunk

# FMI weather (no key). One representative station near the main load centre.
FMI_PLACE = "Helsinki"
FMI_PARAMS = "temperature,windspeedms"


# ==== CELL 2 — helpers =====================================================
import time
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta   # available in Fabric runtime

def month_chunks(start_date, end_date):
    """Yield (chunk_start, chunk_end) month windows as UTC datetimes."""
    cur = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
    while cur < end:
        nxt = min(cur + relativedelta(months=1), end)
        yield cur, nxt
        cur = nxt

def iso_z(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ==== CELL 3 — Fingrid → bronze ============================================
def fetch_fingrid(ds_id, start_date, end_date):
    """Return a list of raw row dicts for one dataset over the window."""
    headers = {"x-api-key": FINGRID_API_KEY}
    rows = []
    for c_start, c_end in month_chunks(start_date, end_date):
        params = {
            "startTime": iso_z(c_start),
            "endTime": iso_z(c_end),
            "format": "json",
            "pageSize": PAGE_SIZE,
        }
        r = requests.get(
            f"https://data.fingrid.fi/api/datasets/{ds_id}/data",
            headers=headers, params=params, timeout=60,
        )
        r.raise_for_status()
        payload = r.json()
        chunk = payload.get("data", payload if isinstance(payload, list) else [])
        rows.extend(chunk)
        if len(chunk) >= PAGE_SIZE:
            print(f"    WARN id {ds_id} {c_start:%Y-%m}: hit pageSize {PAGE_SIZE}, may be truncated")
        time.sleep(FINGRID_PAUSE_S)
    return rows

for name, ds_id in FINGRID_DATASETS.items():
    print(f"Fingrid {name} (id {ds_id}) ...")
    raw = fetch_fingrid(ds_id, START_DATE, END_DATE)
    # keep only the stable columns; additionalJson varies by dataset
    pdf = pd.DataFrame(raw)[["datasetId", "startTime", "endTime", "value"]]
    pdf["series"] = name
    sdf = spark.createDataFrame(pdf)
    (sdf.write.format("delta").mode("overwrite")
        .saveAsTable(f"bronze_fingrid_{name}"))
    print(f"  -> bronze_fingrid_{name}: {pdf.shape[0]} rows")


# ==== CELL 4 — FMI weather → bronze ========================================
def fetch_fmi(place, parameters, start_date, end_date):
    """FMI WFS time-value pairs → tidy list of {time, parameter, value}."""
    ns = {"wml2": "http://www.opengis.net/waterml/2.0"}
    out = []
    for c_start, c_end in month_chunks(start_date, end_date):
        params = {
            "service": "WFS", "version": "2.0.0", "request": "getFeature",
            "storedquery_id": "fmi::observations::weather::timevaluepair",
            "place": place, "parameters": parameters,
            "starttime": iso_z(c_start), "endtime": iso_z(c_end),
        }
        r = requests.get("https://opendata.fmi.fi/wfs", params=params, timeout=120)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        # each MeasurementTimeseries carries one parameter; id ends with the param name
        for ts in root.iter("{http://www.opengis.net/waterml/2.0}MeasurementTimeseries"):
            pname = ts.get("{http://www.opengis.net/gml/3.2}id", "").split("-")[-1]
            for p in ts.findall(".//wml2:MeasurementTVP", ns):
                t = p.find("wml2:time", ns)
                v = p.find("wml2:value", ns)
                if t is not None and v is not None and v.text:
                    try:
                        out.append({"time": t.text, "parameter": pname, "value": float(v.text)})
                    except ValueError:
                        pass
    return out

print("FMI weather ...")
weather = fetch_fmi(FMI_PLACE, FMI_PARAMS, START_DATE, END_DATE)
wdf = spark.createDataFrame(pd.DataFrame(weather))
wdf.write.format("delta").mode("overwrite").saveAsTable("bronze_fmi_weather")
print(f"  -> bronze_fmi_weather: {len(weather)} rows")


# ==== CELL 5 — ENTSO-E price → bronze (FILL WHEN TOKEN ARRIVES) =============
# Day-ahead spot price, bidding zone Finland (10YFI-1--------U), documentType A44.
# Uncomment and set ENTSOE_TOKEN once the email access is granted.
#
# ENTSOE_TOKEN = "PASTE_TOKEN"
# def fetch_entsoe_price(start_date, end_date):
#     out = []
#     for c_start, c_end in month_chunks(start_date, end_date):
#         params = {
#             "securityToken": ENTSOE_TOKEN, "documentType": "A44",
#             "in_Domain": "10YFI-1--------U", "out_Domain": "10YFI-1--------U",
#             "periodStart": c_start.strftime("%Y%m%d%H%M"),
#             "periodEnd": c_end.strftime("%Y%m%d%H%M"),
#         }
#         r = requests.get("https://web-api.tp.entsoe.eu/api", params=params, timeout=120)
#         r.raise_for_status()
#         root = ET.fromstring(r.content)
#         # parse TimeSeries > Period (start + resolution) > Point (position, price.amount)
#         # -> expand each point to an absolute hourly timestamp
#         ...  # (will complete this together once you have the token + a sample response)
#     return out
#
# price = fetch_entsoe_price(START_DATE, END_DATE)
# spark.createDataFrame(pd.DataFrame(price)).write.format("delta")\
#      .mode("overwrite").saveAsTable("bronze_entsoe_price")


# ==== CELL 6 — sanity check ================================================
for name in FINGRID_DATASETS:
    n = spark.table(f"bronze_fingrid_{name}").count()
    print(f"bronze_fingrid_{name:16s}: {n:>8} rows")
print("bronze_fmi_weather       :", spark.table("bronze_fmi_weather").count(), "rows")
