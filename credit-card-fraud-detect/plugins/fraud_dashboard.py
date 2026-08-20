"""Airflow plugin exposing a fraud detection dashboard at ``/fraud-dashboard``.

Adds a FastAPI application to the Airflow API server that serves:

* ``GET  /fraud-dashboard/``                 - the HTML dashboard
* ``GET  /fraud-dashboard/api/summary``      - KPI counters
* ``GET  /fraud-dashboard/api/transactions`` - recent transactions
* ``GET  /fraud-dashboard/api/flagged``      - flagged transactions + reasons
* ``GET  /fraud-dashboard/api/transaction/{id}`` - single transaction details
* ``POST /fraud-dashboard/api/decision/{id}`` - record a human decision

Also registers an external view so the dashboard is one click away from
the Airflow navbar.
"""

from __future__ import annotations

from pathlib import Path

from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from include.fraud_utils import (
    fetch_flagged,
    fetch_recent,
    fetch_summary,
    fetch_transaction,
    init_db,
    update_decision,
)


app = FastAPI(title="Fraud Detection Dashboard", docs_url="/api/docs")


# Make sure the SQLite schema exists when the API server boots.
try:
    init_db()
except Exception as exc:  # pragma: no cover
    # Don't crash the API server just because the DB isn't reachable yet.
    print(f"[fraud_dashboard] init_db failed: {exc}")


_DASHBOARD_HTML = (Path(__file__).parent / "fraud_dashboard.html").read_text()


class DecisionIn(BaseModel):
    decision: str
    notes: str | None = None


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(_DASHBOARD_HTML)


@app.get("/api/summary")
def api_summary() -> JSONResponse:
    return JSONResponse(fetch_summary())


@app.get("/api/transactions")
def api_transactions(limit: int = 50) -> JSONResponse:
    return JSONResponse(fetch_recent(limit=limit))


@app.get("/api/flagged")
def api_flagged(limit: int = 100) -> JSONResponse:
    return JSONResponse(fetch_flagged(limit=limit))


@app.get("/api/transaction/{tx_id}")
def api_transaction(tx_id: str) -> JSONResponse:
    tx = fetch_transaction(tx_id)
    if not tx:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return JSONResponse(tx)


@app.post("/api/decision/{tx_id}")
def api_decision(tx_id: str, body: DecisionIn) -> JSONResponse:
    try:
        ok = update_decision(tx_id, body.decision, body.notes)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not ok:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return JSONResponse({"ok": True, "id": tx_id, "decision": body.decision})


class FraudDashboardPlugin(AirflowPlugin):
    """Registers the fraud dashboard FastAPI app + nav link."""

    name = "fraud_dashboard"

    fastapi_apps = [
        {
            "app": app,
            "url_prefix": "/fraud-dashboard",
            "name": "Fraud Dashboard",
        }
    ]

    external_views = [
        {
            "name": "Fraud Dashboard",
            "href": "/fraud-dashboard/",
            "destination": "nav",
            "icon": "https://raw.githubusercontent.com/twitter/twemoji/master/assets/svg/1f6a8.svg",
            "category": "browse",
        }
    ]
