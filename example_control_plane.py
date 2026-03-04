from __future__ import annotations

from datetime import datetime
from typing import Dict, List

from llm_log_summarizer import (
    ControlPlaneConfig,
    LogAlertControlPlane,
    OutlierEvent,
    SummarizerConfig,
    has_required_env,
    make_llm_call,
    make_workflow_alert_generator,
)


LOG_ROWS: List[Dict[str, str]] = [
    {
        "ts": "2026-03-01T09:25:00Z",
        "service": "payments",
        "level": "INFO",
        "log": "Healthcheck ok",
    },
    {
        "ts": "2026-03-01T09:35:00Z",
        "service": "payments",
        "level": "ERROR",
        "log": "Timeout while calling card-gateway request_id=ab01",
    },
    {
        "ts": "2026-03-01T09:40:00Z",
        "service": "payments",
        "level": "ERROR",
        "log": "Timeout while calling card-gateway request_id=ab02",
    },
    {
        "ts": "2026-03-01T09:50:00Z",
        "service": "payments",
        "level": "ERROR",
        "log": "Timeout while calling card-gateway request_id=ab03",
    },
    {
        "ts": "2026-03-01T10:01:00Z",
        "service": "payments",
        "level": "ERROR",
        "log": "Payment processing failed code=502",
    },
    {
        "ts": "2026-03-01T10:10:00Z",
        "service": "auth",
        "level": "WARN",
        "log": "JWT parse failed token_id=z123",
    },
]


def db_fetcher_factory(
    *,
    window_start: str,
    window_end: str,
    outlier: OutlierEvent,
):
    """
    Returns db_fetch(start_date, limit, offset) for one outlier window.
    In production this factory should wire real SQL call with:
    window_start <= ts < window_end.
    """
    start_dt = _parse_iso(window_start)
    end_dt = _parse_iso(window_end)
    filtered = [
        row
        for row in LOG_ROWS
        if start_dt <= _parse_iso(row["ts"]) < end_dt
    ]

    def db_fetch(start_date: str, limit: int, offset: int) -> str:
        _ = start_date  # window is already bound in closure.
        chunk = filtered[offset : offset + limit]
        if not chunk:
            return ""

        headers = list(chunk[0].keys())
        lines = [
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join("---" for _ in headers) + " |",
        ]
        for row in chunk:
            cells = [str(row.get(h, "")).replace("|", "\\|") for h in headers]
            lines.append("| " + " | ".join(cells) + " |")
        return "\n".join(lines)

    return db_fetch


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


if __name__ == "__main__":
    if not has_required_env():
        raise RuntimeError(
            "OPENAI_API_BASE_DB and OPENAI_API_KEY_DB are required."
        )

    outliers = [
        OutlierEvent(timestamp="2026-03-01T10:00:00Z", outlier_id="o-1", score=0.91),
        OutlierEvent(timestamp="2026-03-01T10:15:00Z", outlier_id="o-2", score=0.88),
    ]

    control_plane = LogAlertControlPlane(
        llm_call=make_llm_call(),
        db_fetcher_factory=db_fetcher_factory,
        alert_generator=make_workflow_alert_generator(
            api_url="http://localhost:8000/run-workflow",
            workflow_template="clickhouse_text_to_sql",
            webhook_url="",
        ),
        config=ControlPlaneConfig(
            format_alert_summary=False,
            summarizer_config=SummarizerConfig(
                page_limit=2,
                max_leaf_rows=3,
                reduce_fan_in=3,
                log_column="log",
            )
        ),
    )

    run_results = control_plane.run_for_outliers(outliers)
    print("=== RUN RESULTS ===")
    for item in run_results:
        print(
            {
                "outlier_id": item.outlier.outlier_id,
                "outlier_ts": item.outlier.timestamp,
                "window_start": item.window_start,
                "window_end": item.window_end,
                "alert_id": item.alert_id,
                "rows": item.summary_stats.total_rows if item.summary_stats else None,
                "error": item.error,
            }
        )
