import os
from fastapi import FastAPI, Request, HTTPException
from google.cloud import bigquery
from datetime import datetime

app = FastAPI()
client = bigquery.Client()

PROJECT = "mesuradors-mdc"
DATASET = "mesuradors"
TABLE = "readings"
SECRET = os.environ.get("INGEST_SECRET", "secret")

@app.get("/")
def root():
    return {"status": "ok"}

@app.post("/ingest/{secret}")
async def ingest(secret: str, request: Request):
    if secret != SECRET:
        raise HTTPException(status_code=403, detail="forbidden")

    data = await request.json()

    row = {
        "event_time": data.get("event_time", datetime.utcnow().isoformat()),
        "meter_id": data.get("meter_id"),
        "location": data.get("location"),
        "value": float(data.get("value")),
        "raw": str(data),
        "uplink_id": data.get("uplink_id"),
    }

    table_id = f"{PROJECT}.{DATASET}.{TABLE}"
    errors = client.insert_rows_json(table_id, [row])

    if errors:
        raise HTTPException(status_code=500, detail=str(errors))

    return {"status": "inserted"}
