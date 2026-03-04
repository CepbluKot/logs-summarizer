from __future__ import annotations

from typing import Dict, List

from llm_log_summarizer import (
    LogSummarizer,
    SummarizerConfig,
    has_required_env,
    make_llm_call,
)


MOCK_ROWS: List[Dict[str, str]] = [
    {
        "ts": "2026-03-01T10:00:00Z",
        "service": "payments",
        "level": "ERROR",
        "log": "Timeout while calling card-gateway request_id=ab12 user_id=1001",
    },
    {
        "ts": "2026-03-01T10:00:02Z",
        "service": "payments",
        "level": "ERROR",
        "log": "Timeout while calling card-gateway request_id=ab13 user_id=1002",
    },
    {
        "ts": "2026-03-01T10:01:00Z",
        "service": "auth",
        "level": "WARN",
        "log": "JWT parse failed token_id=ff11",
    },
    {
        "ts": "2026-03-01T10:02:00Z",
        "service": "payments",
        "level": "INFO",
        "log": "Processed payment order_id=501 amount=1000",
    },
    {
        "ts": "2026-03-01T10:03:00Z",
        "service": "payments",
        "level": "ERROR",
        "log": "Timeout while calling card-gateway request_id=ab14 user_id=1003",
    },
]


def db_fetch(start_date: str, limit: int, offset: int) -> str:
    """
    Replace with your real DB call.
    Contract:
    - accepts start_date, limit, offset
    - returns markdown table string
    """
    chunk = MOCK_ROWS[offset : offset + limit]
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


if __name__ == "__main__":
    if not has_required_env():
        raise RuntimeError(
            "OPENAI_API_BASE_DB and OPENAI_API_KEY_DB are required."
        )

    config = SummarizerConfig(
        page_limit=2,
        max_leaf_rows=3,
        max_leaf_signatures=10,
        reduce_fan_in=3,
    )
    llm_call = make_llm_call()
    summarizer = LogSummarizer(llm_call=llm_call, db_fetch=db_fetch, config=config)
    result = summarizer.summarize(start_date="2026-03-01T00:00:00Z")

    print("=== FINAL SUMMARY ===")
    print(result.summary)
    print()
    print("=== STATS ===")
    print(result.stats)
