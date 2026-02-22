# ============================================================
# mesuradors-api
# main.py
#
# Version: 1.2.1
# Date: 2026-02-22
#
# NEW:
# - /ingest_chs/{secret} adapter (ChirpStack -> ingest)
# - gasoil_linear supports raw_unit=mm (mm->cm)
# ============================================================

import os
from datetime import datetime, timezone
from fastapi import FastAPI, Request, HTTPException
from google.cloud import bigquery

PROJECT_ID = os.getenv("PROJECT_ID", "mesuradors-mdc")
DATASET_ID = os.getenv("DATASET_ID", "mesuradors")

TABLE_READINGS = "readings"
TABLE_METERS = "meters"

VERSION = "1.2.1"

INGEST_SECRET = os.getenv("INGEST_SECRET", "massanet123")

app = FastAPI()
bq = bigquery.Client(project=PROJECT_ID)


def _to_cm(raw_value: float, raw_unit: str | None) -> float:
    """Normalize distance to centimeters when possible."""
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


def convert_value(raw_value: float, meter: dict, raw_unit: str | None):
    scale = meter.get("scale_type")

    if scale == "gasoil_linear":
        h = meter.get("h_sensor_cm")
        z = meter.get("zm_sensor_cm")
        litres = meter.get("litres_diposit")

        if None in (h, z, litres):
            return raw_value

        # h and z are in cm, so normalize raw to cm too
        raw_cm = _to_cm(raw_value, raw_unit)

        # raw_cm = distance from sensor to liquid surface
        level_cm = h - raw_cm
        value_l = (level_cm / z) * litres

        return round(value_l, 3)

    return raw_value


def get_meter(meter_id: str) -> dict:
    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_METERS}`
    WHERE meter_id = @meter_id
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


def insert_reading(row: dict):
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_READINGS}"
    errors = bq.insert_rows_json(table_id, [row])
    if errors:
        raise HTTPException(status_code=500, detail=errors)


def ingest_core(meter_id: str, raw_value: float, raw_unit: str | None, location: str | None, raw_payload: dict):
    now = datetime.now(timezone.utc)

    meter = get_meter(meter_id)
    group_id = meter.get("group_id")

    value = convert_value(raw_value, meter, raw_unit)
    unit = meter.get("display_unit")

    row = {
        "event_time": now.isoformat(),
        "meter_id": meter_id,
        "group_id": group_id,
        "location": location,
        "raw_value": raw_value,
        "raw_unit": raw_unit,
        "value": value,
        "unit": unit,
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
        "unit": unit,
        "timestamp": now.isoformat(),
        "version": VERSION,
    }


@app.post("/ingest/{secret}")
async def ingest(secret: str, request: Request):
    if secret != INGEST_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")

    body = await request.json()

    meter_id = body["meter_id"]
    raw_value = float(body["value"])
    raw_unit = body.get("unit")  # could be "mm" or "cm"
    location = body.get("location")

    return ingest_core(meter_id, raw_value, raw_unit, location, body)


@app.post("/ingest_chs/{secret}")
async def ingest_chs(secret: str, request: Request):
    """
    ChirpStack event adapter.
    Expects:
      body.deviceInfo.deviceName == "nivell_gasoil_escola"
      body.object.distancia_mm (number)
    """
    if secret != INGEST_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")

    body = await request.json()

    device_name = (body.get("deviceInfo", {}) or {}).get("deviceName")
    if not device_name:
        raise HTTPException(status_code=400, detail="Missing deviceInfo.deviceName")

    # map CHS deviceName -> our meter_id
    meter_id = "gasoil_escola" if device_name == "nivell_gasoil_escola" else device_name

    distancia_mm = (body.get("object", {}) or {}).get("distancia_mm")
    if distancia_mm is None:
        raise HTTPException(status_code=400, detail="Missing object.distancia_mm")

    raw_value = float(distancia_mm)
    raw_unit = "mm"
    location = None

    return ingest_core(meter_id, raw_value, raw_unit, location, body)
