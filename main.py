# ============================================================
# mesuradors-api
# Version: 1.2.6 20/02/26 - 15:32
# Fix: datetime JSON serialization for BigQuery
# ============================================================

import os
import traceback
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from fastapi import FastAPI, Request, HTTPException, Body
from google.cloud import bigquery


PROJECT_ID = os.getenv("PROJECT_ID", "mesuradors-mdc")
DATASET_ID = os.getenv("DATASET_ID", "mesuradors")

TABLE_READINGS = os.getenv("TABLE_READINGS", "readings")
TABLE_METERS = os.getenv("TABLE_METERS", "meters")

INGEST_SECRET = os.getenv("INGEST_SECRET", "massanet123")

VERSION = "1.2.6"

app = FastAPI()
bq = bigquery.Client(project=PROJECT_ID)


def _utc_now():
    return datetime.now(timezone.utc)


def table_id(name):
    return f"{PROJECT_ID}.{DATASET_ID}.{name}"


# ------------------------------------------------
# Meter config
# ------------------------------------------------

def get_meter(meter_id):

    query = f"""
    SELECT *
    FROM `{table_id(TABLE_METERS)}`
    WHERE meter_id = @meter_id
    LIMIT 1
    """

    job = bq.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "meter_id",
                    "STRING",
                    meter_id,
                )
            ]
        ),
    )

    rows = list(job.result())

    if not rows:
        raise HTTPException(404, f"Meter not found: {meter_id}")

    return dict(rows[0])


# ------------------------------------------------
# Conversion
# ------------------------------------------------

def convert_value(raw_value, raw_unit, meter):

    scale = meter.get("scale_type")

    if scale == "gasoil_linear":

        h = meter["h_sensor_cm"]
        z = meter["zm_sensor_cm"]
        litres = meter["litres_diposit"]

        raw_cm = raw_value / 10

        level_cm = h - raw_cm

        value = level_cm / z * litres

        return round(value, 3)

    return raw_value


# ------------------------------------------------
# Insert
# ------------------------------------------------

def ingest_core(
    meter_id,
    raw_value,
    raw_unit,
    location,
    raw_payload,
):

    meter = get_meter(meter_id)

    value = convert_value(
        raw_value,
        raw_unit,
        meter,
    )

    row = {

        "event_time": _utc_now().isoformat(),

        "meter_id": meter_id,

        "location": location,

        "value": value,

        "raw": None,

        "uplink_id": None,

        "unit": meter.get("display_unit"),

        "raw_value": raw_value,

        "raw_unit": raw_unit,

        "raw_payload": raw_payload,

        "group_id": meter.get("group_id"),

    }

    errors = bq.insert_rows_json(
        table_id(TABLE_READINGS),
        [row],
    )

    if errors:

        raise HTTPException(
            500,
            {
                "bq_errors": errors
            },
        )

    return {

        "status": "inserted",

        "meter_id": meter_id,

        "value": value,

        "unit": meter.get("display_unit"),

        "version": VERSION,

    }


# ------------------------------------------------
# Health
# ------------------------------------------------

@app.get("/health")
def health():

    return {

        "ok": True,

        "version": VERSION,

        "project": PROJECT_ID,

        "dataset": DATASET_ID,

    }


# ------------------------------------------------
# Chirpstack
# ------------------------------------------------

@app.post("/ingest_chs/{secret}")
def ingest_chs(
    secret: str,
    body: Dict[str, Any] = Body(...),
):

    if secret != INGEST_SECRET:

        raise HTTPException(403, "Invalid secret")

    device_name = body["deviceInfo"]["deviceName"]

    meter_id = (

        "gasoil_escola"

        if device_name == "nivell_gasoil_escola"

        else device_name

    )

    raw_value = body["object"]["distancia_mm"]

    return ingest_core(

        meter_id,

        raw_value,

        "mm",

        None,

        body,

    )
