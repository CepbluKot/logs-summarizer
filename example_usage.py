from __future__ import annotations

from typing import Any, Dict, List, Sequence

from llm_log_summarizer import (
    PeriodLogSummarizer,
    SummarizerConfig,
    has_required_env,
    make_llm_call,
)


def db_fetch_page(
    *,
    columns: Sequence[str],
    period_start: str,
    period_end: str,
    limit: int,
    offset: int,
) -> List[Dict[str, Any]]:
    """
    Interface #1 implementation template.

    You should implement SQL like:
    SELECT <columns>
    FROM logs
    WHERE ts >= :period_start AND ts < :period_end
    ORDER BY ts, id
    LIMIT :limit OFFSET :offset
    """
    _ = (columns, period_start, period_end, limit, offset)
    raise NotImplementedError("Implement DB access here.")


if __name__ == "__main__":
    if not has_required_env():
        raise RuntimeError("OPENAI_API_BASE_DB and OPENAI_API_KEY_DB are required.")

    summarizer = PeriodLogSummarizer(
        db_fetch_page=db_fetch_page,  # Interface #1
        llm_call=make_llm_call(),     # Interface #2
        config=SummarizerConfig(
            page_limit=1000,
            llm_chunk_rows=200,
            reduce_group_size=8,
        ),
    )

    result = summarizer.summarize_period(      # Interface #3
        period_start="2026-03-01T00:00:00Z",
        period_end="2026-03-02T00:00:00Z",
        columns=["ts", "service", "level", "log"],
    )

    print("=== SUMMARY ===")
    print(result.summary)
    print("=== STATS ===")
    print(
        {
            "pages_fetched": result.pages_fetched,
            "rows_processed": result.rows_processed,
            "llm_calls": result.llm_calls,
            "chunk_summaries": result.chunk_summaries,
            "reduce_rounds": result.reduce_rounds,
        }
    )
