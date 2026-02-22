# ============================================================
# mesuradors-api
# Version: 1.0.0
# Build: 2026-02-22
# Author: Ajuntament de Maçanet de Cabrenys
#
# Changelog:
#
# 1.0.0 (2026-02-22)
# - Primera versió operativa
# - Ingesta via POST /ingest/{secret}
# - Inserció a BigQuery
# - Timestamp automàtic
# - Endpoint /version
#
# ============================================================

import os
import datetime
import logging

from fastapi import FastAPI, HTTPException, Request
from google.cloud import bigquery


# ============================================================
# CONFIGURACIÓ
# ============================================================

VERSION = "1.0.0"
BUILD_DATE = "2026-02-22"

PROJECT_ID = "mesuradors-mdc"
DATASET_ID = "mesuradors"
TABLE_ID = "readings"

INGEST_SECRET = os.environ.get("INGEST_SECRET", "massanet123")

TABLE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger("mesuradors-api")


# ============================================================
# BIGQUERY CLIENT
# ============================================================

bq = bigquery.Client()


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(
    title="Mesuradors API",
    version=VERSION
)


# ============================================================
# HEALTH CHECK
# ============================================================

@app.get("/")
def health():

    return {
        "status": "ok",
        "service": "mesuradors-api",
        "version": VERSION,
        "build": BUILD_DATE
    }


# ============================================================
# VERSION INFO
# ============================================================

@app.get("/version")
def version():

    return {
        "service": "mesuradors-api",
        "version": VERSION,
        "build": BUILD_DATE,
        "project": PROJECT_ID,
        "dataset": DATASET_ID,
        "table": TABLE_ID
    }


# ============================================================
# INGEST ENDPOINT
# ============================================================

@app.post("/ingest/{secret}")
async def ingest(secret: str, request: Request):

    if secret != INGEST_SECRET:

        logger.warning("Intent amb secret incorrecte")

        raise HTTPException(
            status_code=403,
            detail="forbidden"
        )


    body = await request.json()


    meter_id = body.get("meter_id")

    location = body.get("location")

    value = body.get("value")


    if meter_id is None or location is None or value is None:

        raise HTTPException(
            status_code=400,
            detail="missing fields"
        )


    timestamp = datetime.datetime.utcnow().isoformat()


    row = {

        "meter_id": meter_id,

        "location": location,

        "value": value,

        "timestamp": timestamp

    }


    try:

        errors = bq.insert_rows_json(TABLE, [row])


        if errors:

            logger.error(f"BigQuery errors: {errors}")

            raise HTTPException(
                status_code=500,
                detail="bigquery insert error"
            )


        logger.info(f"Insert OK: {meter_id} {value}")


        return {

            "status": "inserted",

            "table_id": f"{DATASET_ID}.{TABLE_ID}",

            "timestamp": timestamp,

            "version": VERSION

        }


    except Exception as e:

        logger.exception("Internal error")

        raise HTTPException(
            status_code=500,
            detail="internal error"
        )



# ============================================================
# END
# ============================================================
