import os
from datetime import datetime
from fastapi import FastAPI, Request, HTTPException
from google.cloud import bigquery

app = FastAPI()
client = bigquery.Client()

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

    try:
        data = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"invalid json: {e}")

    meter_id = data.get("meter_id")
    if not meter_id:
        raise HTTPException(status_code=400, detail="missing meter_id")

    if data.get("value") is None:
        raise HTTPException(status_code=400, detail="missing value")

    try:
        value = float(data.get("value"))
    except Exception:
        raise HTTPException(status_code=400, detail="value must be a number")

    row = {
        "event_time": data.get("event_time", datetime.utcnow().isoformat()),
        "meter_id": meter_id,
        "location": data.get("location"),
        "value": value,
        "raw": str(data),
        "uplink_id": data.get("uplink_id"),
    }

    # IMPORTANT: no posem PROJECT. Usa el projecte per defecte del servei.
    table_id = f"{DATASET}.{TABLE}"

    try:
        errors = client.insert_rows_json(table_id, [row])
        if errors:
            raise HTTPException(status_code=500, detail={"bq_insert_errors": errors, "table_id": table_id})
    except HTTPException:
        raise
    except Exception as e:
        # retornem l'error complet per diagnosticar (temporal)
        raise HTTPException(status_code=500, detail={"exception": str(e), "table_id": table_id})

    return {"status": "inserted", "table_id": table_id}
