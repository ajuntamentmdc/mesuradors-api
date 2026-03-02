# ============================================================
#  mesuradors-api — Dockerfile
# ============================================================
#  Version: 1.4.0
#  Date (UTC): 2026-03-02T00:00:00Z
#
#  Build:
#    docker build -t mesuradors-api .
#  Run (local):
#    docker run -p 8080:8080 -e PORT=8080 mesuradors-api
# ============================================================

FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy app
COPY main.py .

# Cloud Run provides $PORT
CMD exec uvicorn main:app --host 0.0.0.0 --port $PORT
