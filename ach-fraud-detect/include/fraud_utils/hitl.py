"""Official Airflow 3.x HITL REST API client."""

from __future__ import annotations

import logging
import os
from typing import Any, Mapping

import requests

from .db import fetch_hitl_reference


logger = logging.getLogger(__name__)
DEFAULT_AIRFLOW_API_URL = "http://127.0.0.1:8080/api/v2"


class HITLResponseError(RuntimeError):
    """Raised when Airflow rejects a HITL REST response."""

    def __init__(self, message: str, status_code: int | None = None, body: str = ""):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


def respond_to_hitl(
    tx_id: str,
    decision: str,
    notes: str | None = None,
    forwarded_headers: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Submit a decision using Airflow's public v2 HITL endpoint.

    Authentication is inherited from the dashboard request. The dashboard is
    mounted in Airflow's API server, so forwarding the browser session cookie
    or bearer token avoids introducing credentials or a second auth system.
    """
    reference = fetch_hitl_reference(tx_id)
    if not reference or reference.get("hitl_map_index") is None:
        raise HITLResponseError(f"No Airflow HITL task is registered for {tx_id}")

    base_url = os.environ.get("AIRFLOW_API_URL", DEFAULT_AIRFLOW_API_URL).rstrip("/")
    url = (
        f"{base_url}/dags/{reference['hitl_dag_id']}"
        f"/dagRuns/{reference['hitl_run_id']}"
        f"/taskInstances/{reference['hitl_task_id']}"
        f"/{reference['hitl_map_index']}/hitlDetails"
    )
    headers = {
        key: value
        for key, value in (forwarded_headers or {}).items()
        if key.lower() in {"authorization", "cookie", "x-csrftoken", "x-requested-with"}
    }
    payload: dict[str, Any] = {"chosen_options": [decision]}
    if notes:
        payload["params_input"] = {"notes": notes}

    try:
        response = requests.patch(url, json=payload, headers=headers, timeout=10)
    except requests.RequestException as exc:
        logger.exception("Airflow HITL PATCH failed before receiving a response: %s", url)
        raise HITLResponseError("Could not reach the Airflow HITL API") from exc

    body = response.text
    if not response.ok:
        logger.error(
            "Airflow HITL PATCH failed: status=%s body=%s url=%s",
            response.status_code,
            body,
            url,
        )
        raise HITLResponseError(
            f"Airflow rejected the HITL response ({response.status_code})",
            status_code=response.status_code,
            body=body,
        )

    logger.info(
        "Airflow HITL PATCH accepted: status=%s url=%s body=%s",
        response.status_code,
        url,
        body,
    )
    try:
        return response.json()
    except ValueError as exc:
        raise HITLResponseError(
            "Airflow returned a non-JSON HITL response",
            response.status_code,
            body,
        ) from exc
