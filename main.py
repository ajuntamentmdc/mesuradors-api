# ============================================================
# mesuradors-api
# main.py
#
# Version: 1.2.5
# Date: 2026-02-22 15:24
#
# Goals (consigna):
# - Cloud Run FastAPI backend
# - Ingest generic + ChirpStack adapter
# - Writes to BigQuery (schema aligned to your real readings table)
# - Reads meter config from BigQuery meters table
# - Avoid 500 "silent" errors: always return structured details
# - Provide /health for verification
# ============================================================

import os
import traceback
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

INGEST_SECRET = os.getenv("INGEST_SECRET", "massanet123")

VERSION = "1.2.5"

app = FastAPI()
bq = bigquery.Client(project=PROJECT_ID)


# -----------------------------
# UTIL
# -----------------------------
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _table_id(table_name: str) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{table_name}"


def _to_cm(raw_value: float, raw_unit: Optional[str]) -> float:
    """Normalize a distance to cm when unit is mm/cm/m."""
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


# -----------------------------
# SCALE FUNCTIONS
# -----------------------------
def convert_value(raw_value: float, raw_unit: Optional[str], meter: Dict[str, Any]) -> float:
    """
    Convert raw_value/raw_unit to display value according to meter.scale_type.
    Returns a float (value).
    """
    scale = meter.get("scale_type")

    if scale == "gasoil_linear":
        # Config required (stored in cm and litres)
        h = meter.get("h_sensor_cm")
        z = meter.get("zm_sensor_cm")
        litres = meter.get("litres_diposit")

        if h is None or z is None or litres is None:
            # fallback: return raw
            return float(raw_value)

        raw_cm = _to_cm(float(raw_value), raw_unit)

        # raw_cm is distance from sensor to liquid surface
        level_cm = float(h) - float(raw_cm)

        # litres = capacity per z-cm segment
        value_l = (level_cm / float(z)) * float(litres)

        # NOTE: optional clamp (uncomment if you want to avoid insane values)
        # value_l = max(0.0, min(value_l, float(litres)))

        return round(float(value_l), 3)

    # default: no scaling
    return float(raw_value)


# -----------------------------
# BIGQUERY ACCESS
# -----------------------------
def get_meter(meter_id: str) -> Dict[str, Any]:
    """Fetch one meter configuration row from BigQuery."""
    q = f"""
    SELECT *
    FROM `{_table_id(TABLE_METERS)}`
    WHERE meter_id = @meter_id
    LIMIT 1
    """
    job = bq.query(
        q,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("meter_id", "STRING", meter_id)]
        ),
    )
    rows = list(job.result())
    if not rows:
        raise HTTPException(status_code=404, detail=f"Meter not found: {meter_id}")
    return dict(rows[0])


def insert_reading(row: Dict[str, Any]) -> None:
    """Insert one row into BigQuery readings."""
    table = _table_id(TABLE_READINGS)
    errors = bq.insert_rows_json(table, [row])
    if errors:
        # Raise as HTTPException with details
        raise HTTPException(status_code=500, detail={"table_id": table, "bq_insert_errors": errors})


# -----------------------------
# CORE INGEST
# -----------------------------
def ingest_core(
    meter_id: str,
    raw_value: float,
    raw_unit: Optional[str],
    location: Optional[str],
    raw_payload: Dict[str, Any],
    uplink_id: Optional[str] = None,
    raw_str: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Common flow:
    - Load meter config
    - Convert raw -> value
    - Insert into BigQuery with exact schema fields
    - Return summary JSON
    """
    meter = get_meter(meter_id)

    group_id = meter.get("group_id")
    display_unit = meter.get("display_unit")

    value = convert_value(raw_value=raw_value, raw_unit=raw_unit, meter=meter)

    # IMPORTANT: align to your real BigQuery schema for readings
    row = {
        "event_time": _utc_now(),          # TIMESTAMP (required)
        "meter_id": meter_id,              # STRING (required)
        "location": location,              # STRING (nullable)
        "value": value,                    # FLOAT (required)
        "raw": raw_str,                    # STRING (nullable)
        "uplink_id": uplink_id,            # STRING (nullable)
        "unit": display_unit,              # STRING (nullable)
        "raw_value": float(raw_value) if raw_value is not None else None,  # FLOAT (nullable)
        "raw_unit": raw_unit,              # STRING (nullable)
        "raw_payload": raw_payload,        # JSON (nullable)
        "group_id": group_id,              # STRING (nullable)
    }

    insert_reading(row)

    return {
        "status": "inserted",
        "meter_id": meter_id,
        "group_id": group_id,
        "raw_value": float(raw_value),
        "raw_unit": raw_unit,
        "value": value,
        "unit": display_unit,
        "table_id": _table_id(TABLE_READINGS),
        "version": VERSION,
    }


# -----------------------------
# ROUTES
# -----------------------------
@app.get("/")
def root():
    return {"ok": True, "service": "mesuradors-api", "version": VERSION}


@app.get("/health")
def health():
    return {
        "ok": True,
        "version": VERSION,
        "project": PROJECT_ID,
        "dataset": DATASET_ID,
        "table_readings": TABLE_READINGS,
        "table_meters": TABLE_METERS,
        "table_id": _table_id(TABLE_READINGS),
        "secret_env_present": bool(os.getenv("INGEST_SECRET")),
    }


@app.post("/ingest/{secret}")
async def ingest(secret: str, request: Request):
    """
    Simple ingest.
    Expected JSON:
    {
      "meter_id": "gasoil_escola",
      "value": 350,
      "unit": "mm",
      "location": "..."
    }
    """
    if secret != INGEST_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")

    body = await request.json()

    if "meter_id" not in body or "value" not in body:
        raise HTTPException(status_code=400, detail="Missing required fields: meter_id, value")

    meter_id = str(body["meter_id"])
    raw_value = float(body["value"])
    raw_unit = body.get("unit")
    location = body.get("location")
    uplink_id = body.get("uplink_id")
    raw_str = body.get("raw")

    try:
        return ingest_core(
            meter_id=meter_id,
            raw_value=raw_value,
            raw_unit=raw_unit,
            location=location,
            raw_payload=body,
            uplink_id=uplink_id,
            raw_str=raw_str,
        )
    except HTTPException:
        raise
    except Exception as e:
        # Always return useful details (no silent 500)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Unhandled exception in /ingest",
                "message": str(e),
                "trace": traceback.format_exc(),
                "version": VERSION,
            },
        )


@app.post("/ingest_chs/{secret}")
async def ingest_chs(secret: str, body: Dict[str, Any] = Body(...)):
    """
    ChirpStack event adapter.
    Expects at least:
      body.deviceInfo.deviceName
      body.object.distancia_mm

    Current mapping:
      deviceName "nivell_gasoil_escola" -> meter_id "gasoil_escola"
      else: meter_id = deviceName
    """
    if secret != INGEST_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")

    try:
        device_name = (body.get("deviceInfo") or {}).get("deviceName")
        if not device_name:
            raise HTTPException(status_code=400, detail="Missing deviceInfo.deviceName")

        meter_id = "gasoil_escola" if device_name == "nivell_gasoil_escola" else device_name

        distancia_mm = (body.get("object") or {}).get("distancia_mm")
        if distancia_mm is None:
            raise HTTPException(status_code=400, detail="Missing object.distancia_mm")

        raw_value = float(distancia_mm)
        raw_unit = "mm"

        # Try to capture an uplink id if present
        uplink_id = body.get("uplinkID") or body.get("uplink_id") or body.get("uplinkId")

        return ingest_core(
            meter_id=meter_id,
            raw_value=raw_value,
            raw_unit=raw_unit,
            location=None,
            raw_payload=body,
            uplink_id=uplink_id,
            raw_str=None,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Unhandled exception in /ingest_chs",
                "message": str(e),
                "trace": traceback.format_exc(),
                "version": VERSION,
            },
        )
