# ============================================================
# mesuradors-api — main.py
# Version: 1.2.7
#
# Consignes:
# - FastAPI on Cloud Run
# - Endpoints:
#   - GET  /health
#   - POST /ingest/{secret}       (generic)
#   - POST /ingest_chs/{secret}   (ChirpStack adapter)
# - BigQuery:
#   - Read meter config from `mesuradors.meters`
#   - Write readings to `mesuradors.readings`
#   - Align to schema:
#       event_time TIMESTAMP (REQUIRED)
#       meter_id   STRING    (REQUIRED)
#       location   STRING    (NULLABLE)
#       value      FLOAT     (REQUIRED)
#       raw        STRING    (NULLABLE)
#       uplink_id  STRING    (NULLABLE)
#       unit       STRING    (NULLABLE)
#       raw_value  FLOAT     (NULLABLE)
#       raw_unit   STRING    (NULLABLE)
#       raw_payload JSON/RECORD (NULLABLE)  <-- IMPORTANT: currently behaves like RECORD in insert
#       group_id   STRING    (NULLABLE)
#
# IMPORTANT FIX:
# - BigQuery is rejecting raw_payload when sent as JSON dict:
#     "raw_payload is not a record"
# - Therefore: store full payload in `raw` as JSON string, and set raw_payload=None.
#
# Also:
# - Enable CORS for Swagger "Try it out".
# ============================================================

import os
import json
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from fastapi import FastAPI, Request, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery


# -----------------------------
# CONFIG (env + defaults)
# -----------------------------
PROJECT_ID = os.getenv("PROJECT_ID", "mesuradors-mdc")
DATASET_ID = os.getenv("DATASET_ID", "mesuradors")
TABLE_METERS = os.getenv("TABLE_METERS", "meters")
TABLE_READINGS = os.getenv("TABLE_READINGS", "readings")

INGEST_SECRET = os.getenv("INGEST_SECRET", "massanet123")

# Optional: strict mapping for known CHS deviceName -> meter_id
CHS_DEVICE_MAP = {
    "nivell_gasoil_escola": "gasoil_escola",
}

VERSION = "1.2.7"


def table_id(table_name: str) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{table_name}"


def utc_now_iso() -> str:
    # BigQuery insert_rows_json accepts timestamp as ISO8601 string
    return datetime.now(timezone.utc).isoformat()


# -----------------------------
# APP
# -----------------------------
app = FastAPI(title="mesuradors-api", version=VERSION)

# CORS to allow Swagger "Try it out" from browser
# (Cloud Run + browser often needs this; harmless for our use here)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # or restrict later
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# BigQuery client (project fallback)
bq = bigquery.Client(project=PROJECT_ID)


# -----------------------------
# METERS: fetch config
# -----------------------------
def get_meter_config(meter_id: str) -> Dict[str, Any]:
    q = f"""
    SELECT *
    FROM `{table_id(TABLE_METERS)}`
    WHERE meter_id = @meter_id
    LIMIT 1
    """
    job = bq.query(
        q,
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


# -----------------------------
# CONVERSION
# -----------------------------
def _distance_to_cm(value: float, unit: Optional[str]) -> float:
    if unit is None:
        return value
    u = unit.strip().lower()
    if u == "mm":
        return value / 10.0
    if u == "cm":
        return value
    if u == "m":
        return value * 100.0
    return value


def convert_value(raw_value: float, raw_unit: Optional[str], meter: Dict[str, Any]) -> Tuple[float, Optional[str]]:
    """
    Returns: (value, display_unit)
    """
    scale_type = meter.get("scale_type")
    display_unit = meter.get("display_unit")

    # Default: pass-through
    if not scale_type:
        return float(raw_value), display_unit

    if scale_type == "gasoil_linear":
        # expects (from meters table):
        # h_sensor_cm, zm_sensor_cm, litres_diposit
        h = meter.get("h_sensor_cm")
        z = meter.get("zm_sensor_cm")
        litres = meter.get("litres_diposit")

        if h is None or z is None or litres is None:
            # fallback, do not break ingestion
            return float(raw_value), display_unit

        raw_cm = _distance_to_cm(float(raw_value), raw_unit)
        level_cm = float(h) - raw_cm
        value_l = (level_cm / float(z)) * float(litres)
        return round(float(value_l), 3), display_unit

    # Unknown scale_type: pass-through
    return float(raw_value), display_unit


# -----------------------------
# BIGQUERY INSERT
# -----------------------------
def insert_reading(row: Dict[str, Any]) -> None:
    errors = bq.insert_rows_json(table_id(TABLE_READINGS), [row])
    if errors:
        raise HTTPException(status_code=500, detail={"bq_errors": errors})


# -----------------------------
# CORE INGEST
# -----------------------------
def ingest_core(
    meter_id: str,
    raw_value: float,
    raw_unit: Optional[str],
    location: Optional[str],
    raw_payload_obj: Dict[str, Any],
    uplink_id: Optional[str] = None,
) -> Dict[str, Any]:

    meter = get_meter_config(meter_id)
    group_id = meter.get("group_id")

    value, display_unit = convert_value(raw_value, raw_unit, meter)

    # IMPORTANT:
    # - BigQuery is rejecting raw_payload as dict ("not a record")
    # - Store full JSON payload as STRING in `raw`
    # - Set `raw_payload` to None to match whatever RECORD-type column exists
    raw_payload_str = json.dumps(raw_payload_obj, ensure_ascii=False)

    row = {
        "event_time": utc_now_iso(),          # TIMESTAMP (string ISO ok)
        "meter_id": meter_id,                # required
        "location": location,                # nullable
        "value": float(value),               # required
        "raw": raw_payload_str,              # nullable STRING -> keep full payload here
        "uplink_id": uplink_id,              # nullable
        "unit": display_unit,                # nullable
        "raw_value": float(raw_value),       # nullable
        "raw_unit": raw_unit,                # nullable
        "raw_payload": None,                 # avoid "not a record"
        "group_id": group_id,                # nullable
    }

    insert_reading(row)

    return {
        "status": "inserted",
        "meter_id": meter_id,
        "group_id": group_id,
        "raw_value": float(raw_value),
        "raw_unit": raw_unit,
        "value": float(value),
        "unit": display_unit,
        "table_id": table_id(TABLE_READINGS),
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
        "table_id": table_id(TABLE_READINGS),
        "secret_env_present": bool(os.getenv("INGEST_SECRET")),
    }


@app.post("/ingest/{secret}")
async def ingest(secret: str, request: Request):
    """
    Generic ingest.
    Expected JSON (minimum):
      {
        "meter_id": "...",
        "value": 123.4,
        "unit": "mm" | "cm" | "L" | ...
        "location": "optional",
        "uplink_id": "optional"
      }
    """
    if secret != INGEST_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    meter_id = body.get("meter_id")
    if not meter_id:
        raise HTTPException(status_code=400, detail="Missing field: meter_id")

    if "value" not in body:
        raise HTTPException(status_code=400, detail="Missing field: value")

    try:
        raw_value = float(body["value"])
    except Exception:
        raise HTTPException(status_code=400, detail="Field value must be a number")

    raw_unit = body.get("unit")
    location = body.get("location")
    uplink_id = body.get("uplink_id")

    try:
        return ingest_core(
            meter_id=str(meter_id),
            raw_value=raw_value,
            raw_unit=raw_unit,
            location=location,
            raw_payload_obj=body,
            uplink_id=uplink_id,
        )
    except HTTPException:
        raise
    except Exception as e:
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
def ingest_chs(secret: str, body: Dict[str, Any] = Body(...)):
    """
    ChirpStack adapter (minimal payload supported):
      {
        "deviceInfo": { "deviceName": "nivell_gasoil_escola" },
        "object": { "distancia_mm": 350 }
      }

    Mapping:
      - deviceName in CHS_DEVICE_MAP => mapped meter_id
      - else => meter_id = deviceName
    """
    if secret != INGEST_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")

    try:
        device_name = (body.get("deviceInfo") or {}).get("deviceName")
        if not device_name:
            raise HTTPException(status_code=400, detail="Missing deviceInfo.deviceName")

        meter_id = CHS_DEVICE_MAP.get(device_name, device_name)

        distancia_mm = (body.get("object") or {}).get("distancia_mm")
        if distancia_mm is None:
            raise HTTPException(status_code=400, detail="Missing object.distancia_mm")

        raw_value = float(distancia_mm)
        raw_unit = "mm"

        # optional uplink id
        uplink_id = body.get("uplinkID") or body.get("uplink_id") or body.get("uplinkId")

        return ingest_core(
            meter_id=meter_id,
            raw_value=raw_value,
            raw_unit=raw_unit,
            location=None,
            raw_payload_obj=body,
            uplink_id=uplink_id,
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
