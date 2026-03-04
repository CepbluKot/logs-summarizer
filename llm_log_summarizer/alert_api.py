from __future__ import annotations

import json
from typing import Any, Callable, Dict, Optional
from urllib import error, request


def make_workflow_alert_generator(
    *,
    api_url: str = "http://localhost:8000/run-workflow",
    workflow_template: str = "clickhouse_text_to_sql",
    webhook_url: str = "",
    timeout_sec: float = 30.0,
) -> Callable[[str, str], Optional[str]]:
    """
    Build an AlertGenerator-compatible callable.

    Contract:
    alert_generator(summary: str, outlier_timestamp: str) -> Optional[str]
    """

    def _alert_generator(summary: str, outlier_timestamp: str) -> Optional[str]:
        _ = outlier_timestamp
        response_payload = run_workflow(
            task=summary,
            api_url=api_url,
            workflow_template=workflow_template,
            webhook_url=webhook_url,
            timeout_sec=timeout_sec,
        )
        return _extract_alert_id(response_payload)

    return _alert_generator


def run_workflow(
    *,
    task: str,
    api_url: str = "http://localhost:8000/run-workflow",
    workflow_template: str = "clickhouse_text_to_sql",
    webhook_url: str = "",
    timeout_sec: float = 30.0,
) -> Dict[str, Any]:
    """
    POST /run-workflow payload:
    {
      "workflow_template": "...",
      "task": "...",
      "webhook_url": ""
    }
    """
    body = {
        "workflow_template": workflow_template,
        "task": task,
        "webhook_url": webhook_url,
    }
    data = json.dumps(body).encode("utf-8")
    req = request.Request(
        url=api_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8").strip()
            if not raw:
                return {}
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
                return {"result": parsed}
            except json.JSONDecodeError:
                return {"raw_response": raw}
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Alert API HTTPError {exc.code}: {raw}"
        ) from exc
    except error.URLError as exc:
        raise RuntimeError(f"Alert API URLError: {exc.reason}") from exc


def _extract_alert_id(payload: Dict[str, Any]) -> Optional[str]:
    for key in ("run_id", "workflow_run_id", "task_id", "id", "job_id"):
        value = payload.get(key)
        if value is not None:
            return str(value)
    if payload:
        return json.dumps(payload, ensure_ascii=False)
    return None

