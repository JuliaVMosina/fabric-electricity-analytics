# Finnish Electricity Market Analytics — Fabric pet-project #2

Code-first **PySpark medallion lakehouse** on real Finnish open data.
Companion to project #1 (F-Group Retail): #1 = low-code (Data Factory Copy job + T-SQL Warehouse),
#2 = **code-first PySpark notebooks + live multi-source APIs**.

Closes the **Databricks / distributed-computing GAP** (Konecranes) and maps onto **DP-600**
Spark + notebooks + semantic model (KONE). Forecast layer nods to "AI-assisted BI" (KONE).

---

## Data sources (3 heterogeneous → "data integration from multiple sources")

| Source | What | Auth | Format |
|---|---|---|---|
| **Fingrid Open Data** | consumption, production by type (wind/nuclear/hydro/solar), total | `x-api-key` header (free, instant) | JSON/CSV |
| **ENTSO-E Transparency** | day-ahead spot price, bidding zone Finland (`10YFI-1--------U`) | security token (free, **up to 3 working days**) | XML |
| **FMI Open Data** | weather: temperature, wind speed (drivers of demand & wind gen) | none | XML (WFS) |

### Getting access (do this first — ENTSO-E has a lead time)
1. **Fingrid** — register at https://data.fingrid.fi/en → developer portal → get personal `x-api-key`.
   Browse datasets at https://data.fingrid.fi/en/datasets and note the `datasetId` of each series you want.
   Limits: 10 000 req / 24 h, 1 req / 2 s.
2. **ENTSO-E** — register at https://transparency.entsoe.eu/ → then email **transparency@entsoe.eu**,
   subject **"RESTful API access"**, body = your registered email. Token arrives within ~3 working days.
   ⚠️ **Send this email today** so it isn't a blocker.
3. **FMI** — no key. WFS at https://opendata.fmi.fi/wfs.

Put keys in env vars before running the local check (PowerShell):
```powershell
$env:FINGRID_API_KEY = "your-key"
$env:ENTSOE_TOKEN    = "your-token"   # once it arrives
```

---

## Architecture — medallion

```
APIs ──► BRONZE (raw, as-fetched)        Lakehouse Files/, partitioned by source+date
          │   PySpark: parse, type-cast, dedupe, unit-normalize, UTC align
          ▼
        SILVER (clean, conformed)         Delta tables: fact_hourly + dim_date / dim_zone / dim_production_type
          │   PySpark: hourly join (consumption ⋈ production ⋈ price ⋈ weather)
          ▼
        GOLD (marts + forecast)           Delta: mart_energy_balance, mart_price_demand, pred_demand
          ▼
        Semantic model (DirectLake) ──► Power BI report
```

### Gold marts (planned)
- `mart_energy_balance` — production vs consumption, renewable share % by hour/day
- `mart_price_demand` — spot price ⋈ demand ⋈ temperature (correlation, peak hours)
- `pred_demand` — simple demand forecast (regression: demand ~ temp + hour + weekday), MAE reported

---

## Build steps in Fabric (once keys validated locally)
1. Create Lakehouse `electricity_lakehouse`.
2. Notebook `01_bronze_ingest` (PySpark) — call 3 APIs, write raw to `Files/bronze/...`.
3. Notebook `02_silver_transform` — clean + conform + hourly join → Delta tables.
4. Notebook `03_gold_marts` — aggregate marts + forecast.
5. Orchestrate the 3 notebooks in a Data Factory **pipeline** (scheduled).
6. Semantic model (DirectLake) + relationships → Power BI report.

---

## Files here
- `fetch_sample.py` — local validation: pulls a small sample from each API, prints shape. Run BEFORE Fabric.
- (notebooks added once keys validated)
