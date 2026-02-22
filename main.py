# ============================================================
# mesuradors-api
# main.py
#
# Version: 1.2.3
# Date: 2026-02-22
#
# Fix:
# - Insert row keys aligned to the REAL BigQuery schema you have
#   (event_time, meter_id, location, value, raw, uplink_id, unit,
#    raw_value, raw_unit, raw_payload, group_id)
# - Adds /health to show config + target table
# ============================================================

import os
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from fastapi import FastAPI, Request, HTTPException, Body
from google.cloud import bigquery


# -----------------------------
# CONFIG
# -----------------------------
PROJECT_ID = os.getenv("PROJECT_ID", "mesuradors-mdc")
DATASET_ID = os.getenv("DATASET_ID", "mesuradors")

TABLE_READINGS = os.getenv("TABLE_READINGS", "readings")
TABLE_METERS = os.getenv("TABLE_METERS", "meters")

VERSION = "1.2.3"

INGEST_SECRET = os.getenv("INGEST_SECRET", "massanet123")

app = FastAPI()
bq = bigquery.Client(project=PROJECT_ID)


# -----------------------------
# Helpers
# -----------------------------
def _to_cm(raw_value: float, raw_unit: Optional[str]) -> float:
    if raw_unit is None:
        return raw_value
    u = raw_unit.strip().lower()
    if u == "mm":
        return raw_value / 10.0
    if u == "cm":
        return raw_value
    if u == "m":
        return raw_value * 100.0
    return raw_value


def convert_value(raw_value: float, meter: Dict[str, Any], raw_unit: Optional[str]) -> float:
    scale = meter.get("scale_type")

    if scale == "gasoil_linear":
        h = meter.get("h_sensor_cm")
        z = meter.get("zm_sensor_cm")
        litres = meter.get("litres_diposit")

        if h is None or z is None or litres is None:
            return raw_value

        raw_cm = _to_cm(raw_value, raw_unit)
        level_cm = float(h) - float(raw_cm)
        value_l = (level_cm / float(z)) * float(litres)

        return round(value_l, 3)

    return raw_value


def get_meter(meter_id: str) -> Dict[str, Any]:
    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_METERS}`
    WHERE meter_id = @meter_id
    LIMIT 1
    """
    job = bq.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("meter_id", "STRING", meter_id)]
        ),
    )
    rows = list(job.result())
    if not rows:
        raise HTTPException(status_code=404, detail=f"Meter not found: {meter_id}")
    return dict(rows[0])


def insert_reading(row: Dict[str, Any]) -> None:
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_READINGS}"
    errors = bq.insert_rows_json(table_id, [row])
    if errors:
        raise HTTPException(status_code=500, detail={"table_id": table_id, "bq_insert_errors": errors})


def ingest_core(
    meter_id: str,
    raw_value: float,
    raw_unit: Optional[str],
    location: Optional[str],
    raw_payload: Dict[str, Any],
    uplink_id: Optional[str] = None,
) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)

    meter = get_meter(meter_id)
    group_id = meter.get("group_id")
    display_unit = meter.get("display_unit")

    value = convert_value(raw_value, meter, raw_unit)

    # IMPORTANT: align to your real table schema
    row = {
        "event_time": now,                # TIMESTAMP
        "meter_id": meter_id,             # STRING (required)
        "location": location,             # STRING
        "value": value,                   # FLOAT (required)
        "raw": None,                      # STRING (optional)
        "uplink_id": uplink_id,           # STRING (optional)
        "unit": display_unit,             # STRING
        "raw_value": raw_value,           # FLOAT
        "raw_unit": raw_unit,             # STRING
        "raw_payload": raw_payload,       # JSON
        "group_id": group_id,             # STRING
    }

    insert_reading(row)

    return {
        "status": "inserted",
        "meter_id": meter_id,
        "group_id": group_id,
        "raw_value": raw_value,
        "raw_unit": raw_unit,
        "value": value,
        "unit": display_unit,
        "timestamp": now.isoformat(),
        "table_id": f"{PROJECT_ID}.{DATASET_ID}.{TABLE_READINGS}",
        "version": VERSION,
    }


# -----------------------------
# ROUTES
# -----------------------------
@app.get("/health")
def health():
    return {
        "ok": True,
        "version": VERSION,
        "project": PROJECT_ID,
        "dataset": DATASET_ID,
        "table_readings": TABLE_READINGS,
        "table_id": f"{PROJECT_ID}.{DATASET_ID}.{TABLE_READINGS}",
        "secret_env_present": bool(os.getenv("INGEST_SECRET")),
    }


@app.post("/ingest/{secret}")
async def ingest(secret: str, request: Request):
    if secret != INGEST_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")

    body = await request.json()

    try:
        meter_id = body["meter_id"]
        raw_value = float(body["value"])
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid body. Required: meter_id, value")

    raw_unit = body.get("unit")
    location = body.get("location")

    return ingest_core(
        meter_id=meter_id,
        raw_value=raw_value,
        raw_unit=raw_unit,
        location=location,
        raw_payload=body,
        uplink_id=body.get("uplink_id"),
    )


@app.post("/ingest_chs/{secret}")
async def ingest_chs(secret: str, body: Dict[str, Any] = Body(...)):
    if secret != INGEST_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")

    device_name = (body.get("deviceInfo") or {}).get("deviceName")
    if not device_name:
        raise HTTPException(status_code=400, detail="Missing deviceInfo.deviceName")

    meter_id = "gasoil_escola" if device_name == "nivell_gasoil_escola" else device_name

    distancia_mm = (body.get("object") or {}).get("distancia_mm")
    if distancia_mm is None:
        raise HTTPException(status_code=400, detail="Missing object.distancia_mm")

    raw_value = float(distancia_mm)
    raw_unit = "mm"

    # Try to capture an uplink id if present in CHS event
    uplink_id = body.get("uplinkID") or body.get("uplink_id")

    return ingest_core(
        meter_id=meter_id,
        raw_value=raw_value,
        raw_unit=raw_unit,
        location=None,
        raw_payload=body,
        uplink_id=uplink_id,
    )
