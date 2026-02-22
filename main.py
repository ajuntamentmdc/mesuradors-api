# ============================================================
# mesuradors-api
# main.py
#
# Version: 1.2.2
# Date: 2026-02-22
#
# Features:
# - POST /ingest/{secret}     : ingest "simple" (meter_id/value/unit)
# - POST /ingest_chs/{secret} : ChirpStack adapter (deviceInfo + object.distancia_mm)
# - gasoil_linear:
#     - uses meters.h_sensor_cm, meters.zm_sensor_cm, meters.litres_diposit
#     - supports raw_unit in mm/cm/m (normalized to cm)
# - Writes to BigQuery: mesuradors.readings
# - Reads meter config from BigQuery: mesuradors.meters
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

VERSION = "1.2.2"

# IMPORTANT: idealment posar-ho com env var a Cloud Run
INGEST_SECRET = os.getenv("INGEST_SECRET", "massanet123")

app = FastAPI()
bq = bigquery.Client(project=PROJECT_ID)


# -----------------------------
# Helpers
# -----------------------------
def _to_cm(raw_value: float, raw_unit: Optional[str]) -> float:
    """Normalize distance to centimeters when possible (mm/cm/m)."""
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
    """Apply scaling based on meter.scale_type."""
    scale = meter.get("scale_type")

    if scale == "gasoil_linear":
        h = meter.get("h_sensor_cm")
        z = meter.get("zm_sensor_cm")
        litres = meter.get("litres_diposit")

        # si falta config, retornem el raw
        if h is None or z is None or litres is None:
            return raw_value

        # h i z estan en cm => normalitzem raw a cm
        raw_cm = _to_cm(raw_value, raw_unit)

        # raw_cm = distància del sensor fins la superfície del líquid
        level_cm = float(h) - float(raw_cm)
        value_l = (level_cm / float(z)) * float(litres)

        # clamp opcional (evitar números absurds si el sensor dona outliers)
        # value_l = max(0.0, min(value_l, float(litres)))

        return round(value_l, 3)

    # default: sense escala
    return raw_value


def get_meter(meter_id: str) -> Dict[str, Any]:
    """Load meter config row from BigQuery."""
    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_METERS}`
    WHERE meter_id = @meter_id
    LIMIT 1
    """
    job = bq.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("meter_id", "STRING", meter_id)
            ]
        ),
    )
    rows = list(job.result())
    if not rows:
        raise HTTPException(status_code=404, detail=f"Meter not found: {meter_id}")
    return dict(rows[0])


def insert_reading(row: Dict[str, Any]) -> None:
    """Insert one row into BigQuery readings table."""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_READINGS}"
    errors = bq.insert_rows_json(table_id, [row])
    if errors:
        raise HTTPException(status_code=500, detail={"bq_insert_errors": errors})


def ingest_core(
    meter_id: str,
    raw_value: float,
    raw_unit: Optional[str],
    location: Optional[str],
    raw_payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Common ingest flow: meter config -> scale -> insert -> return summary."""
    now = datetime.now(timezone.utc)

    meter = get_meter(meter_id)
    group_id = meter.get("group_id")
    display_unit = meter.get("display_unit")

    value = convert_value(raw_value, meter, raw_unit)

    row = {
        "event_time": now.isoformat(),
        "meter_id": meter_id,
        "group_id": group_id,
        "location": location,
        "raw_value": raw_value,
        "raw_unit": raw_unit,
        "value": value,
        "unit": display_unit,
        "raw_payload": raw_payload,
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
        "version": VERSION,
    }


# -----------------------------
# ROUTES
# -----------------------------
@app.get("/")
def health():
    return {"ok": True, "service": "mesuradors-api", "version": VERSION}


@app.post("/ingest/{secret}")
async def ingest(secret: str, request: Request):
    """
    Ingest simple:
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
    )


@app.post("/ingest_chs/{secret}")
async def ingest_chs(secret: str, body: Dict[str, Any] = Body(...)):
    """
    ChirpStack event adapter.
    Expects:
      body.deviceInfo.deviceName == "nivell_gasoil_escola"
      body.object.distancia_mm   (number)
    """
    if secret != INGEST_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")

    device_name = (body.get("deviceInfo") or {}).get("deviceName")
    if not device_name:
        raise HTTPException(status_code=400, detail="Missing deviceInfo.deviceName")

    # Map CHS deviceName -> our meter_id
    # (segons els teus logs, el CHS device és "nivell_gasoil_escola"
    #  i al BigQuery meters és "gasoil_escola")
    meter_id = "gasoil_escola" if device_name == "nivell_gasoil_escola" else device_name

    distancia_mm = (body.get("object") or {}).get("distancia_mm")
    if distancia_mm is None:
        raise HTTPException(status_code=400, detail="Missing object.distancia_mm")

    raw_value = float(distancia_mm)
    raw_unit = "mm"

    return ingest_core(
        meter_id=meter_id,
        raw_value=raw_value,
        raw_unit=raw_unit,
        location=None,
        raw_payload=body,
    )
