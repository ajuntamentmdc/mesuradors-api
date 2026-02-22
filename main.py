# ============================================================
#  mesuradors-api — main.py
# ============================================================
#
#  Project: mesuradors-mdc
#  Dataset: mesuradors
#
#  Pipeline:
#    DF555 (LoRaWAN) → ChirpStack → HTTP Webhook → Cloud Run → BigQuery → PWA
#
# ------------------------------------------------------------
#  VERSION
# ------------------------------------------------------------
#  Version: 1.3.0
#  Date (UTC): 2026-02-22T22:30:00Z
#
# ------------------------------------------------------------
#  CHANGELOG
# ------------------------------------------------------------
#  1.3.0 (2026-02-22)
#   - FEATURE (READ API): Adds read-only endpoints for PWA:
#       GET /v1/locations
#       GET /v1/locations/{ubicacio}/estat
#       GET /v1/estat (optional: all)
#     Data source: BigQuery view `mesuradors.v_estat_scada`
#
#   - Keeps ingestion logic from 1.2.9:
#       - Correct scale_type="gasoil_linear" conversion
#       - Store battery_v / temperature_c / tilt_deg into readings table
#       - Ignore event=join/status (200 OK ignored)
#
# ------------------------------------------------------------
#  NOTES
# ------------------------------------------------------------
#  - v_estat_scada must exist in BigQuery (already created in this project).
#  - CORS is open (*) for now; restrict later to https://mesuradors.massanet.cat
# ============================================================

import os
import json
import logging
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

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

# BigQuery view used by the PWA endpoints
VIEW_ESTAT_SCADA = os.getenv("VIEW_ESTAT_SCADA", "v_estat_scada")

INGEST_SECRET = os.getenv("INGEST_SECRET", "massanet123")

# If set to "json", we also attempt to store dict payload in BigQuery JSON column raw_payload.
# Default "off": always store JSON payload into `raw` (STRING), raw_payload = None.
RAW_PAYLOAD_MODE = os.getenv("RAW_PAYLOAD_MODE", "off").strip().lower()  # "off" | "json"

# Optional: strict mapping for known CHS deviceName -> meter_id
CHS_DEVICE_MAP = {
    "nivell_gasoil_escola": "gasoil_escola",
}

VERSION = "1.3.0"


def table_id(name: str) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{name}"


def view_id(name: str) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{name}"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


# -----------------------------
# APP
# -----------------------------
app = FastAPI(title="mesuradors-api", version=VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # later: restrict to https://mesuradors.massanet.cat
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
# ChirpStack helpers (DF555 decoded object)
# -----------------------------
def extract_df555_object(body: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(body.get("object"), dict):
        return body["object"]

    uplink = body.get("uplink")
    if isinstance(uplink, dict) and isinstance(uplink.get("object"), dict):
        return uplink["object"]

    event = body.get("event")
    if isinstance(event, dict) and isinstance(event.get("object"), dict):
        return event["object"]

    return {}


def extract_device_name(body: Dict[str, Any]) -> Optional[str]:
    di = body.get("deviceInfo")
    if isinstance(di, dict):
        dn = di.get("deviceName")
        if isinstance(dn, str) and dn.strip():
            return dn.strip()

    dn2 = body.get("deviceName")
    if isinstance(dn2, str) and dn2.strip():
        return dn2.strip()

    return None


def extract_uplink_id(body: Dict[str, Any]) -> Optional[str]:
    for k in ("uplinkID", "uplinkId", "uplink_id"):
        v = body.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


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


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def convert_value(raw_value: float, raw_unit: Optional[str], meter: Dict[str, Any]) -> Tuple[float, Optional[str]]:
    scale_type = meter.get("scale_type")
    display_unit = meter.get("display_unit")

    if not scale_type:
        return float(raw_value), display_unit

    # This is the configured scale_type in mesuradors.meters (e.g., gasoil_linear).
    if scale_type == "gasoil_linear":
        h = meter.get("h_sensor_cm")
        z = meter.get("zm_sensor_cm")
        litres = meter.get("litres_diposit")

        if h is None or z is None or litres is None:
            return float(raw_value), display_unit

        raw_cm = _distance_to_cm(float(raw_value), raw_unit)

        usable_h = float(h) - float(z)
        if usable_h <= 0:
            return float(raw_value), display_unit

        level_cm = float(h) - raw_cm
        level_cm = _clamp(level_cm, 0.0, usable_h)

        value_l = (level_cm / usable_h) * float(litres)
        value_l = _clamp(value_l, 0.0, float(litres))

        return round(float(value_l), 3), display_unit

    return float(raw_value), display_unit


# -----------------------------
# BIGQUERY INSERT
# -----------------------------
def insert_reading(row: Dict[str, Any]) -> None:
    errors = bq.insert_rows_json(table_id(TABLE_READINGS), [row])
    if errors:
        logger.error("BigQuery insert errors: %s", errors)
        raise HTTPException(status_code=500, detail={"bq_errors": errors})


def ingest_core(
    meter_id: str,
    raw_value: float,
    raw_unit: Optional[str],
    location: Optional[str],
    raw_payload_obj: Dict[str, Any],
    uplink_id: Optional[str] = None,
    battery_v: Optional[float] = None,
    temperature_c: Optional[float] = None,
    tilt_deg: Optional[float] = None,
) -> Dict[str, Any]:

    meter = get_meter_config(meter_id)
    group_id = meter.get("group_id")

    value, display_unit = convert_value(raw_value, raw_unit, meter)

    raw_payload_str = json.dumps(raw_payload_obj, ensure_ascii=False)
    raw_payload_for_bq = raw_payload_obj if RAW_PAYLOAD_MODE == "json" else None

    row = {
        "event_time": utc_now_iso(),
        "meter_id": meter_id,
        "location": location,
        "value": float(value),
        "raw": raw_payload_str,
        "uplink_id": uplink_id,
        "unit": display_unit,
        "raw_value": float(raw_value),
        "raw_unit": raw_unit,
        "raw_payload": raw_payload_for_bq,
        "group_id": group_id,
        "battery_v": battery_v,
        "temperature_c": temperature_c,
        "tilt_deg": tilt_deg,
    }

    insert_reading(row)

    logger.info(
        "Inserted reading meter_id=%s raw=%s%s value=%s%s bat=%s temp=%s tilt=%s v=%s",
        meter_id,
        raw_value,
        f" {raw_unit}" if raw_unit else "",
        value,
        f" {display_unit}" if display_unit else "",
        battery_v,
        temperature_c,
        tilt_deg,
        VERSION,
    )

    return {
        "status": "inserted",
        "meter_id": meter_id,
        "group_id": group_id,
        "raw_value": float(raw_value),
        "raw_unit": raw_unit,
        "value": float(value),
        "unit": display_unit,
        "battery_v": battery_v,
        "temperature_c": temperature_c,
        "tilt_deg": tilt_deg,
        "table_id": table_id(TABLE_READINGS),
        "version": VERSION,
    }


# -----------------------------
# READ API (PWA)
# -----------------------------
def bq_rows_to_dicts(rows) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        # Make TIMESTAMP JSON-friendly (ISO)
        ts = d.get("ultima_lectura")
        if ts is not None:
            try:
                d["ultima_lectura"] = ts.isoformat()
            except Exception:
                pass
        out.append(d)
    return out


@app.get("/v1/locations")
def list_locations():
    q = f"""
    SELECT DISTINCT ubicacio
    FROM `{view_id(VIEW_ESTAT_SCADA)}`
    WHERE ubicacio IS NOT NULL
    ORDER BY ubicacio
    """
    rows = bq.query(q).result()
    return {"locations": [dict(r)["ubicacio"] for r in rows], "version": VERSION}


@app.get("/v1/locations/{ubicacio}/estat")
def estat_by_location(ubicacio: str):
    q = f"""
    SELECT
      ubicacio, sensor, rang, v_act, unit, pct, estat, ultima_lectura
    FROM `{view_id(VIEW_ESTAT_SCADA)}`
    WHERE ubicacio = @ubicacio
    ORDER BY sensor
    """
    job = bq.query(
        q,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("ubicacio", "STRING", ubicacio)]
        ),
    )
    rows = list(job.result())
    return {"ubicacio": ubicacio, "rows": bq_rows_to_dicts(rows), "version": VERSION}


@app.get("/v1/estat")
def estat_all():
    q = f"""
    SELECT
      ubicacio, sensor, rang, v_act, unit, pct, estat, ultima_lectura
    FROM `{view_id(VIEW_ESTAT_SCADA)}`
    ORDER BY ubicacio, sensor
    """
    rows = bq.query(q).result()
    return {"rows": bq_rows_to_dicts(rows), "version": VERSION}


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
        "view_estat_scada": VIEW_ESTAT_SCADA,
        "id_readings": table_id(TABLE_READINGS),
        "id_meters": table_id(TABLE_METERS),
        "id_view_estat_scada": view_id(VIEW_ESTAT_SCADA),
        "secret_env_present": bool(os.getenv("INGEST_SECRET")),
        "raw_payload_mode": RAW_PAYLOAD_MODE,
    }


@app.post("/ingest/{secret}")
async def ingest(secret: str, request: Request):
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

    battery_v = _safe_float(body.get("battery_v"))
    temperature_c = _safe_float(body.get("temperature_c"))
    tilt_deg = _safe_float(body.get("tilt_deg"))

    try:
        return ingest_core(
            meter_id=str(meter_id),
            raw_value=raw_value,
            raw_unit=raw_unit,
            location=location,
            raw_payload_obj=body,
            uplink_id=uplink_id,
            battery_v=battery_v,
            temperature_c=temperature_c,
            tilt_deg=tilt_deg,
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
    if secret != INGEST_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")

    event = request.query_params.get("event")  # up / join / status / ...
    if event and event != "up":
        return {"status": "ignored", "reason": f"event={event}", "version": VERSION}

    try:
        device_name = extract_device_name(body)
        if not device_name:
            return {"status": "ignored", "reason": "missing deviceInfo.deviceName", "version": VERSION}

        meter_id = CHS_DEVICE_MAP.get(device_name, device_name)

        obj = extract_df555_object(body)

        distancia_mm = obj.get("distancia_mm")
        if distancia_mm is None:
            return {"status": "ignored", "reason": "missing object.distancia_mm", "meter_id": meter_id, "version": VERSION}

        battery_v = _safe_float(obj.get("bateria_V"))
        temperature_c = _safe_float(obj.get("temperatura_C"))
        tilt_deg = _safe_float(obj.get("inclinacio_deg"))

        raw_value = float(distancia_mm)
        raw_unit = "mm"
        uplink_id = extract_uplink_id(body)

        return ingest_core(
            meter_id=meter_id,
            raw_value=raw_value,
            raw_unit=raw_unit,
            location=None,
            raw_payload_obj=body,
            uplink_id=uplink_id,
            battery_v=battery_v,
            temperature_c=temperature_c,
            tilt_deg=tilt_deg,
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
