# ============================================================
#  mesuradors-api — main.py
# ============================================================
#
#  Project:  mesuradors-mdc
#  Dataset:  mesuradors
#  Service:  Cloud Run (FastAPI)
#
#  Pipeline:
#    Sensor LoRaWAN → ChirpStack → HTTP Webhook → Cloud Run → BigQuery → API → PWA
#
# ------------------------------------------------------------
#  VERSION
# ------------------------------------------------------------
#  Version: 1.4.0
#  Date (UTC): 2026-03-02T00:00:00Z
#
# ------------------------------------------------------------
#  CHANGELOG
# ------------------------------------------------------------
#  1.4.0 (2026-03-02)
#   - FEATURE (HISTORY API): Adds time-series endpoint for charts:
#       GET /v1/meters/{meter_id}/series?window=6h|12h|24h|48h|7d|30d
#     Returns bucketed points (latest reading per bucket) to keep payloads small.
#   - UX SUPPORT: Keeps response schema stable and predictable for the PWA chart.
#   - No breaking changes to ingestion or existing read-only endpoints.
#
#  1.3.1 (2026-02-28)
#   - FIX (INGEST): Accepts DF555 decoded field names from ChirpStack `object`:
#       - temperature_c: accepts `temperatura_C` OR `temperatura`
#       - tilt_deg: accepts `inclinacio_deg` OR `inclinacio_graus`
#       - battery_v: accepts `bateria_V` OR (`voltatge`/`voltage`/`vbat`) when present; otherwise NULL
#     This fixes NULL telemetry fields in BigQuery while keeping schema unchanged.
#   - DIAG: Logs DF555 object keys when optional telemetry is missing.
#
# ------------------------------------------------------------
#  NOTES
# ------------------------------------------------------------
#  - BigQuery objects expected:
#      * Table:  {PROJECT_ID}.{DATASET_ID}.{TABLE_READINGS}
#      * Table:  {PROJECT_ID}.{DATASET_ID}.{TABLE_METERS}
#      * View:   {PROJECT_ID}.{DATASET_ID}.{VIEW_ESTAT_SCADA}
#  - CORS is currently open (*) for simplicity; consider restricting later to:
#      https://mesuradors.massanet.cat
# ============================================================

import os
import json
import logging
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List, Literal

from fastapi import FastAPI, Request, HTTPException, Body, Query
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

VERSION = "1.4.0"


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
    """Convert incoming raw_value/raw_unit to the configured display unit/value.

    Current supported scale_type:
      - gasoil_linear: distance -> litres based on tank geometry parameters stored in mesuradors.meters
    """
    scale_type = meter.get("scale_type")
    display_unit = meter.get("display_unit")

    if not scale_type:
        return float(raw_value), display_unit

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
        "table_readings": table_id(TABLE_READINGS),
    }


@app.post("/ingest_chs/{secret}")
async def ingest_chs(secret: str, request: Request, body: Dict[str, Any] = Body(...)):
    """ChirpStack webhook ingestion.

    Expected decoded payload for the DF555 level sensor:
      body.object.distancia_mm (required)
      body.object.bateria_V / temperatura_C / inclinacio_deg (optional)
    """
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
            return {
                "status": "ignored",
                "reason": "missing object.distancia_mm",
                "meter_id": meter_id,
                "version": VERSION,
            }

        # Telemetry fields (optional). DF555 decoders/formatters vary by key name.
        battery_v = _safe_float(
            obj.get("bateria_V")
            or obj.get("voltatge")
            or obj.get("voltage")
            or obj.get("vbat")
        )

        temperature_c = _safe_float(obj.get("temperatura_C"))
        if temperature_c is None:
            temperature_c = _safe_float(obj.get("temperatura"))

        tilt_deg = _safe_float(obj.get("inclinacio_deg"))
        if tilt_deg is None:
            tilt_deg = _safe_float(obj.get("inclinacio_graus"))

        # Helpful diagnostics (won't break ingestion)
        if (battery_v is None) or (temperature_c is None) or (tilt_deg is None):
            try:
                logger.info(
                    "DF555 optional telemetry missing (bat=%s temp=%s tilt=%s). object keys=%s",
                    battery_v,
                    temperature_c,
                    tilt_deg,
                    sorted(list(obj.keys())) if isinstance(obj, dict) else None,
                )
            except Exception:
                pass

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


# -----------------------------
# READ API (PWA)
# -----------------------------
def bq_rows_to_dicts(rows) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
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
    rows = list(bq.query(q).result())
    return {"rows": bq_rows_to_dicts(rows), "version": VERSION}


# -----------------------------
# HISTORY API (CHARTS)
# -----------------------------
WindowKey = Literal["6h", "12h", "24h", "48h", "7d", "30d"]


def _window_to_config(window: str) -> Tuple[int, int]:
    """Return (lookback_seconds, bucket_seconds) for supported windows."""
    w = (window or "").strip().lower()

    mapping: Dict[str, Tuple[int, int]] = {
        "6h": (6 * 3600, 5 * 60),                 # 5 min buckets
        "12h": (12 * 3600, 10 * 60),              # 10 min buckets
        "24h": (24 * 3600, 15 * 60),              # 15 min buckets
        "48h": (48 * 3600, 30 * 60),              # 30 min buckets
        "7d": (7 * 24 * 3600, 2 * 60 * 60),       # 2 h buckets
        "30d": (30 * 24 * 3600, 6 * 60 * 60),     # 6 h buckets
    }

    if w not in mapping:
        raise HTTPException(
            status_code=400,
            detail="Invalid window. Use one of: 6h, 12h, 24h, 48h, 7d, 30d",
        )

    return mapping[w]


def _to_iso(ts: Any) -> str:
    try:
        return ts.isoformat()
    except Exception:
        return str(ts)


@app.get("/v1/meters/{meter_id}/series")
def meter_series(
    meter_id: str,
    window: WindowKey = Query("24h", description="6h|12h|24h|48h|7d|30d"),
):
    """Return bucketed time-series for a meter.

    Response schema (stable):
      {
        "meter_id": "...",
        "window": "24h",
        "unit": "L",
        "points": [{"t": "ISO8601", "v": 123.4}, ...],
        "count": 123,
        "bucket_seconds": 900,
        "version": "1.4.0"
      }
    """
    lookback_seconds, bucket_seconds = _window_to_config(window)

    # Pick last reading per bucket to limit point count.
    q = f"""
    WITH base AS (
      SELECT
        event_time,
        value,
        unit,
        TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(event_time), @bucket_seconds) * @bucket_seconds) AS bucket_ts
      FROM `{table_id(TABLE_READINGS)}`
      WHERE meter_id = @meter_id
        AND event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_seconds SECOND)
        AND value IS NOT NULL
    ),
    pick AS (
      SELECT
        bucket_ts,
        event_time,
        value,
        unit,
        ROW_NUMBER() OVER (PARTITION BY bucket_ts ORDER BY event_time DESC) AS rn
      FROM base
    )
    SELECT bucket_ts, event_time, value, unit
    FROM pick
    WHERE rn = 1
    ORDER BY bucket_ts ASC
    """

    job = bq.query(
        q,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("meter_id", "STRING", meter_id),
                bigquery.ScalarQueryParameter("bucket_seconds", "INT64", bucket_seconds),
                bigquery.ScalarQueryParameter("lookback_seconds", "INT64", lookback_seconds),
            ]
        ),
    )

    rows = list(job.result())

    points: List[Dict[str, Any]] = []
    unit: Optional[str] = None

    for r in rows:
        d = dict(r)
        unit = unit or d.get("unit")
        points.append({
            "t": d.get("event_time").isoformat() if d.get("event_time") else None,
            "v": float(d.get("value")),
        })

    return {
        "meter_id": meter_id,
        "window": window,
        "unit": unit,
        "points": points,
        "count": len(points),
        "bucket_seconds": bucket_seconds,
        "version": VERSION,
    }
