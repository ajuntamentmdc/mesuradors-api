# ============================================================
# mesuradors-api — main.py
#
# Version: 1.2.8
# Release date (UTC): 2026-02-22T21:30:00Z
#
# CHANGELOG (1.2.8)
# - FIX (CRITICAL): Correct conversion for scale_type="gasoil_linear"
#   - Previous bug: value_l = (level_cm / zm_sensor_cm) * litres_diposit
#     This could generate absurd values (e.g., 29,000 L for a 2,000 L tank),
#     because zm_sensor_cm is the dead-zone margin, NOT the usable height.
#   - New formula:
#       usable_h = h_sensor_cm - zm_sensor_cm
#       level_cm = clamp(h_sensor_cm - raw_cm, 0, usable_h)
#       value_l  = clamp((level_cm / usable_h) * litres_diposit, 0, litres_diposit)
#
# - IMPROVEMENT: ChirpStack adapter now gracefully ignores non-measurement events
#   (event=join, event=status, report_type=0x03, etc.) by returning 200 OK
#   instead of 400, to avoid log noise and confusion.
#
# - OBSERVABILITY: Adds structured diagnostic fields in responses and logs.
#
# CONSIGNES (kept)
# - FastAPI on Cloud Run
# - Endpoints:
#   - GET  /health
#   - POST /ingest/{secret}       (generic)
#   - POST /ingest_chs/{secret}   (ChirpStack adapter)
# - BigQuery:
#   - Read meter config from `mesuradors.meters`
#   - Write readings to `mesuradors.readings`
#   - Align to schema:
#       event_time   TIMESTAMP (REQUIRED)
#       meter_id     STRING    (REQUIRED)
#       location     STRING    (NULLABLE)
#       value        FLOAT     (REQUIRED)
#       raw          STRING    (NULLABLE)   <-- full payload JSON string
#       uplink_id    STRING    (NULLABLE)
#       unit         STRING    (NULLABLE)
#       raw_value    FLOAT     (NULLABLE)
#       raw_unit     STRING    (NULLABLE)
#       raw_payload  JSON      (NULLABLE)   <-- optional; see note below
#       group_id     STRING    (NULLABLE)
#
# NOTE ABOUT raw_payload
# - Earlier deployments saw BigQuery insert errors ("raw_payload is not a record").
# - To ensure ingestion never breaks, we ALWAYS store full payload into `raw` (STRING).
# - We only fill `raw_payload` if explicitly enabled (env RAW_PAYLOAD_MODE="json").
#   Otherwise raw_payload is set to None.
#
# REFERENCES (project context)
# - DF555 protocol formatter exposes object.distancia_mm, temperatura, inclinacio, alarms, etc.
# - Historical "working" Apps Script wrote distance (cm) to Sheets and used sheet formulas
#   for liters/%; this service computes liters directly for BigQuery.
# ============================================================

import os
import json
import traceback
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from fastapi import FastAPI, Request, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery


# -----------------------------
# LOGGING
# -----------------------------
logger = logging.getLogger("mesuradors-api")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))


# -----------------------------
# CONFIG (env + defaults)
# -----------------------------
PROJECT_ID = os.getenv("PROJECT_ID", "mesuradors-mdc")
DATASET_ID = os.getenv("DATASET_ID", "mesuradors")
TABLE_METERS = os.getenv("TABLE_METERS", "meters")
TABLE_READINGS = os.getenv("TABLE_READINGS", "readings")

INGEST_SECRET = os.getenv("INGEST_SECRET", "massanet123")

# If set to "json", we will attempt to also populate BigQuery JSON column raw_payload.
# Default is "off" (store raw JSON string in 'raw' and set raw_payload=None).
RAW_PAYLOAD_MODE = os.getenv("RAW_PAYLOAD_MODE", "off").strip().lower()  # "off" | "json"

# Optional: strict mapping for known CHS deviceName -> meter_id
CHS_DEVICE_MAP = {
    "nivell_gasoil_escola": "gasoil_escola",
}

VERSION = "1.2.8"


def table_id(table_name: str) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{table_name}"


def utc_now_iso() -> str:
    # BigQuery insert_rows_json accepts timestamp as ISO8601 string
    return datetime.now(timezone.utc).isoformat()


# -----------------------------
# APP
# -----------------------------
app = FastAPI(title="mesuradors-api", version=VERSION)

# CORS (Swagger "Try it out" + future PWA)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # restrict later (e.g. https://mesuradors.massanet.cat)
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# BigQuery client
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
            query_parameters=[bigquery.ScalarQueryParameter("meter_id", "STRING", meter_id)]
        ),
    )
    rows = list(job.result())
    if not rows:
        raise HTTPException(status_code=404, detail=f"Meter not found: {meter_id}")
    return dict(rows[0])


# -----------------------------
# CONVERSION HELPERS
# -----------------------------
def _distance_to_cm(value: float, unit: Optional[str]) -> float:
    """
    Convert a distance to centimeters.
    Expected raw units for DF555 distance are mm.
    """
    if unit is None:
        return value
    u = unit.strip().lower()
    if u == "mm":
        return value / 10.0
    if u == "cm":
        return value
    if u == "m":
        return value * 100.0
    # unknown => pass-through
    return value


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def convert_value(raw_value: float, raw_unit: Optional[str], meter: Dict[str, Any]) -> Tuple[float, Optional[str], Dict[str, Any]]:
    """
    Returns: (value, display_unit, debug)
    debug contains intermediate values for troubleshooting.
    """
    scale_type = (meter.get("scale_type") or "").strip()
    display_unit = meter.get("display_unit")
    debug: Dict[str, Any] = {
        "scale_type": scale_type or None,
        "raw_value": raw_value,
        "raw_unit": raw_unit,
    }

    # Default: pass-through
    if not scale_type:
        return float(raw_value), display_unit, debug

    # --- Correct implementation for gasoil_linear ---
    # The meters table provides:
    # - litres_diposit : total tank capacity in liters
    # - h_sensor_cm    : distance from sensor to tank bottom reference (cm)
    # - zm_sensor_cm   : dead-zone margin near bottom (cm) not usable for measurement
    #
    # Sensor provides:
    # - distancia_mm: "air height" from sensor to liquid surface (mm)
    #
    # Compute:
    #   raw_cm = distancia_mm/10
    #   usable_h = h - z
    #   level_cm = clamp(h - raw_cm, 0, usable_h)
    #   value_l = clamp((level_cm / usable_h) * litres, 0, litres)
    #
    if scale_type == "gasoil_linear":
        h = meter.get("h_sensor_cm")
        z = meter.get("zm_sensor_cm")
        litres = meter.get("litres_diposit")

        debug.update({"h_sensor_cm": h, "zm_sensor_cm": z, "litres_diposit": litres})

        if h is None or z is None or litres is None:
            # fallback, do not break ingestion
            debug["fallback_reason"] = "missing_meter_params"
            return float(raw_value), display_unit, debug

        raw_cm = _distance_to_cm(float(raw_value), raw_unit)
        usable_h = float(h) - float(z)

        debug.update({"raw_cm": raw_cm, "usable_h_cm": usable_h})

        if usable_h <= 0:
            debug["fallback_reason"] = "invalid_usable_height"
            return float(raw_value), display_unit, debug

        # level height of liquid from bottom reference (cm)
        level_cm = float(h) - raw_cm
        level_cm = _clamp(level_cm, 0.0, usable_h)

        value_l = (level_cm / usable_h) * float(litres)
        value_l = _clamp(value_l, 0.0, float(litres))

        debug.update({"level_cm": level_cm, "value_l": value_l})

        return round(float(value_l), 3), display_unit, debug

    # Unknown scale_type: pass-through
    debug["fallback_reason"] = "unknown_scale_type"
    return float(raw_value), display_unit, debug


# -----------------------------
# BIGQUERY INSERT
# -----------------------------
def insert_reading(row: Dict[str, Any]) -> None:
    errors = bq.insert_rows_json(table_id(TABLE_READINGS), [row])
    if errors:
        logger.error("BigQuery insert errors: %s", errors)
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

    value, display_unit, debug = convert_value(raw_value, raw_unit, meter)

    # Always store full payload in 'raw' as JSON string
    raw_payload_str = json.dumps(raw_payload_obj, ensure_ascii=False)

    # raw_payload handling:
    # - If RAW_PAYLOAD_MODE="json", try to store as JSON as well.
    # - Else keep None to avoid schema mismatch failures.
    raw_payload_for_bq = raw_payload_obj if RAW_PAYLOAD_MODE == "json" else None

    row = {
        "event_time": utc_now_iso(),     # TIMESTAMP (ISO string OK)
        "meter_id": meter_id,
        "location": location,           # nullable
        "value": float(value),
        "raw": raw_payload_str,         # nullable STRING -> keep full payload here
        "uplink_id": uplink_id,         # nullable
        "unit": display_unit,           # nullable
        "raw_value": float(raw_value),  # nullable
        "raw_unit": raw_unit,           # nullable
        "raw_payload": raw_payload_for_bq,
        "group_id": group_id,           # nullable
    }

    insert_reading(row)

    logger.info(
        "Inserted reading meter_id=%s value=%s unit=%s raw_value=%s raw_unit=%s",
        meter_id, value, display_unit, raw_value, raw_unit
    )

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
        "debug": debug,
        "raw_payload_mode": RAW_PAYLOAD_MODE,
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
        "table_id_readings": table_id(TABLE_READINGS),
        "table_id_meters": table_id(TABLE_METERS),
        "secret_env_present": bool(os.getenv("INGEST_SECRET")),
        "raw_payload_mode": RAW_PAYLOAD_MODE,
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
        logger.error("Unhandled exception in /ingest: %s", str(e))
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
async def ingest_chs(secret: str, request: Request, body: Dict[str, Any] = Body(...)):
    """
    ChirpStack adapter.

    Supported minimal payload (as produced by our DF555 formatter):
      {
        "deviceInfo": { "deviceName": "nivell_gasoil_escola" },
        "object": { "distancia_mm": 350, ... }
      }

    ChirpStack also calls this webhook with query param:
      ?event=up | ?event=join | ?event=status

    Strategy:
    - event=up: ingest if distancia_mm exists
    - other events: return 200 OK with status="ignored"
    """
    if secret != INGEST_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")

    event = request.query_params.get("event")  # "up", "join", "status", ...
    if event and event != "up":
        # ignore non-measurement events (avoid 400 noise)
        return {"status": "ignored", "reason": f"event={event}", "version": VERSION}

    try:
        device_name = (body.get("deviceInfo") or {}).get("deviceName")
        if not device_name:
            # If missing, we cannot map the meter reliably.
            return {"status": "ignored", "reason": "missing deviceInfo.deviceName", "version": VERSION}

        meter_id = CHS_DEVICE_MAP.get(device_name, device_name)

        obj = body.get("object") or {}
        distancia_mm = obj.get("distancia_mm")

        # Some frames may be confirm/config (report_type 0x03) => no distance
        if distancia_mm is None:
            return {"status": "ignored", "reason": "missing object.distancia_mm", "meter_id": meter_id, "version": VERSION}

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
        logger.error("Unhandled exception in /ingest_chs: %s", str(e))
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Unhandled exception in /ingest_chs",
                "message": str(e),
                "trace": traceback.format_exc(),
                "version": VERSION,
            },
        )
