# ============================================================
# mesuradors-api
# main.py
#
# Version: 1.1.0
# Date: 2026-02-22
#
# SCALA READY VERSION
# - Uses meters table
# - Supports unit conversion
# - Fully scalable architecture
# ============================================================

import os
from datetime import datetime, timezone

from fastapi import FastAPI, Request, HTTPException
from google.cloud import bigquery

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------

PROJECT_ID = os.getenv("PROJECT_ID", "mesuradors-mdc")
DATASET_ID = os.getenv("DATASET_ID", "mesuradors")

TABLE_READINGS = "readings"
TABLE_METERS = "meters"

VERSION = "1.1.0"

# ------------------------------------------------------------
# INIT
# ------------------------------------------------------------

app = FastAPI()

bq = bigquery.Client(project=PROJECT_ID)

# ------------------------------------------------------------
# SCALE FUNCTIONS
# ------------------------------------------------------------

def convert_value(raw_value, meter):

    scale = meter.get("scale_type")

    if scale == "gasoil_linear":

        h = meter.get("h_sensor_cm")
        z = meter.get("zm_sensor_cm")
        litres = meter.get("litres_diposit")

        if None in (h, z, litres):
            return raw_value

        level = h - raw_value

        value = (level / z) * litres

        return round(value, 3)

    return raw_value


# ------------------------------------------------------------
# GET METER CONFIG
# ------------------------------------------------------------

def get_meter(meter_id):

    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_METERS}`
    WHERE meter_id = @meter_id
    """

    job = bq.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "meter_id",
                    "STRING",
                    meter_id
                )
            ]
        )
    )

    rows = list(job.result())

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Meter not found: {meter_id}"
        )

    return dict(rows[0])


# ------------------------------------------------------------
# INSERT
# ------------------------------------------------------------

def insert_reading(data):

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_READINGS}"

    errors = bq.insert_rows_json(
        table_id,
        [data]
    )

    if errors:
        raise HTTPException(
            status_code=500,
            detail=errors
        )


# ------------------------------------------------------------
# ROUTE
# ------------------------------------------------------------

@app.post("/ingest/{secret}")

async def ingest(secret: str, request: Request):

    if secret != "massanet123":

        raise HTTPException(
            status_code=403,
            detail="Invalid secret"
        )

    body = await request.json()

    meter_id = body["meter_id"]

    raw_value = float(body["value"])

    location = body.get("location")

    # get meter config

    meter = get_meter(meter_id)

    # convert

    value = convert_value(raw_value, meter)

    unit = meter.get("display_unit")

    now = datetime.now(timezone.utc)

    row = {

        "event_time": now.isoformat(),

        "meter_id": meter_id,

        "location": location,

        "value": value,

        "raw": str(raw_value),

        "unit": unit

    }

    insert_reading(row)

    return {

        "status": "inserted",

        "meter_id": meter_id,

        "value": value,

        "unit": unit,

        "timestamp": now.isoformat(),

        "version": VERSION

    }
