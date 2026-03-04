from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Protocol, Sequence


class DBPageFetcher(Protocol):
    """
    Interface #1:
    Fetch one page of logs for a fixed time period and selected columns.

    Notes for SQL side:
    - Use stable ordering: ORDER BY ts, id
    - Filter by period: ts >= period_start AND ts < period_end
    - Apply incoming limit/offset
    """

    def __call__(
        self,
        *,
        columns: Sequence[str],
        period_start: str,
        period_end: str,
        limit: int,
        offset: int,
    ) -> List[Dict[str, Any]]:
        ...


class LLMTextCaller(Protocol):
    """
    Interface #2:
    Raw text-in / text-out LLM call.
    """

    def __call__(self, prompt: str) -> str:
        ...


@dataclass
class SummarizerConfig:
    page_limit: int = 1000
    llm_chunk_rows: int = 200
    reduce_group_size: int = 8
    max_reduce_rounds: int = 12
    max_cell_chars: int = 500
    max_summary_chars: int = 10_000


@dataclass
class SummarizationResult:
    summary: str
    pages_fetched: int
    rows_processed: int
    llm_calls: int


class PeriodLogSummarizer:
    """
    Interface #3 (entrypoint):
    summarize_period(...)
    """

    def __init__(
        self,
        *,
        db_fetch_page: DBPageFetcher,
        llm_call: LLMTextCaller,
        config: SummarizerConfig | None = None,
    ) -> None:
        self.db_fetch_page = db_fetch_page
        self.llm_call = llm_call
        self.config = config or SummarizerConfig()

    def summarize_period(
        self,
        *,
        period_start: str,
        period_end: str,
        columns: Sequence[str],
    ) -> SummarizationResult:
        self._validate_iso_datetime(period_start)
        self._validate_iso_datetime(period_end)
        if not columns:
            raise ValueError("columns must not be empty")

        offset = 0
        pages_fetched = 0
        rows_processed = 0
        llm_calls = 0
        chunk_summaries: List[str] = []

        while True:
            page = self.db_fetch_page(
                columns=columns,
                period_start=period_start,
                period_end=period_end,
                limit=self.config.page_limit,
                offset=offset,
            )
            if not page:
                break

            pages_fetched += 1
            rows_processed += len(page)
            offset += len(page)

            for i in range(0, len(page), self.config.llm_chunk_rows):
                rows_chunk = page[i : i + self.config.llm_chunk_rows]
                prompt = self._build_chunk_prompt(
                    period_start=period_start,
                    period_end=period_end,
                    columns=columns,
                    rows=rows_chunk,
                )
                chunk_summary = self.llm_call(prompt).strip()
                if chunk_summary:
                    chunk_summaries.append(self._truncate(chunk_summary, self.config.max_summary_chars))
                llm_calls += 1

            if len(page) < self.config.page_limit:
                break

        if not chunk_summaries:
            return SummarizationResult(
                summary="Нет логов за указанный период.",
                pages_fetched=pages_fetched,
                rows_processed=rows_processed,
                llm_calls=llm_calls,
            )

        final_summary, reduce_calls = self._reduce_summaries(
            chunk_summaries=chunk_summaries,
            period_start=period_start,
            period_end=period_end,
        )
        llm_calls += reduce_calls

        return SummarizationResult(
            summary=final_summary,
            pages_fetched=pages_fetched,
            rows_processed=rows_processed,
            llm_calls=llm_calls,
        )

    def _reduce_summaries(
        self,
        *,
        chunk_summaries: List[str],
        period_start: str,
        period_end: str,
    ) -> tuple[str, int]:
        if len(chunk_summaries) == 1:
            return chunk_summaries[0], 0

        round_idx = 0
        current = chunk_summaries
        llm_calls = 0

        while len(current) > 1:
            round_idx += 1
            if round_idx > self.config.max_reduce_rounds:
                raise RuntimeError("Exceeded max reduce rounds")

            next_level: List[str] = []
            for i in range(0, len(current), self.config.reduce_group_size):
                group = current[i : i + self.config.reduce_group_size]
                prompt = self._build_reduce_prompt(
                    period_start=period_start,
                    period_end=period_end,
                    reduce_round=round_idx,
                    summaries=group,
                )
                merged = self.llm_call(prompt).strip()
                if not merged:
                    merged = "Пустой ответ LLM на этапе reduce."
                next_level.append(self._truncate(merged, self.config.max_summary_chars))
                llm_calls += 1
            current = next_level

        return current[0], llm_calls

    def _build_chunk_prompt(
        self,
        *,
        period_start: str,
        period_end: str,
        columns: Sequence[str],
        rows: List[Dict[str, Any]],
    ) -> str:
        lines = [
            "Ты анализируешь логи и делаешь краткое техническое summary.",
            "Верни обычный текст (не JSON), 5-12 пунктов.",
            "Пиши только по фактам из входных строк.",
            "",
            f"Период: [{period_start}, {period_end})",
            f"Строк в этом куске: {len(rows)}",
            f"Колонки: {', '.join(columns)}",
            "",
            "Логи:",
        ]
        for idx, row in enumerate(rows, start=1):
            rendered_parts: List[str] = []
            for col in columns:
                value = row.get(col, "")
                text = self._truncate(str(value), self.config.max_cell_chars)
                rendered_parts.append(f"{col}={text}")
            lines.append(f"{idx}. " + " | ".join(rendered_parts))
        return "\n".join(lines)

    def _build_reduce_prompt(
        self,
        *,
        period_start: str,
        period_end: str,
        reduce_round: int,
        summaries: List[str],
    ) -> str:
        lines = [
            "Ты объединяешь несколько частичных summary логов в одно итоговое summary.",
            "Верни обычный текст (не JSON), 7-15 пунктов.",
            "Сохрани важные ошибки, повторяющиеся паттерны и возможные причины.",
            "",
            f"Период: [{period_start}, {period_end})",
            f"Reduce round: {reduce_round}",
            "",
            "Частичные summary:",
        ]
        for idx, text in enumerate(summaries, start=1):
            lines.append(f"[SUMMARY {idx}]")
            lines.append(text)
            lines.append("")
        return "\n".join(lines).strip()

    @staticmethod
    def _validate_iso_datetime(value: str) -> None:
        datetime.fromisoformat(value.replace("Z", "+00:00"))

    @staticmethod
    def _truncate(value: str, max_chars: int) -> str:
        if len(value) <= max_chars:
            return value
        return value[: max_chars - 3] + "..."

