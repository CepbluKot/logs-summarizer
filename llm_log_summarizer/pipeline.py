from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from .interfaces import DBMarkdownFetcher, LLMCaller
from .markdown_table import parse_markdown_table
from .summary_contract import (
    compact_for_reduce,
    ensure_coverage,
    format_summary_markdown,
    merge_coverages,
    parse_summary_payload,
    to_json_string,
)


DEFAULT_LOG_COLUMN_CANDIDATES = [
    "log",
    "logs",
    "message",
    "msg",
    "text",
    "event",
    "raw_log",
]


@dataclass
class SummarizerConfig:
    page_limit: int = 1000
    max_pages: Optional[int] = None
    log_column: Optional[str] = None
    max_leaf_rows: int = 5000
    max_leaf_signatures: int = 250
    max_examples_per_signature: int = 2
    max_signature_text_len: int = 220
    max_example_text_len: int = 220
    max_signatures_in_leaf_prompt: int = 150
    reduce_fan_in: int = 8
    max_reduce_rounds: int = 12
    keep_metadata_columns: int = 4
    language_hint: str = "ru"


@dataclass
class SummaryStats:
    start_date: str
    detected_log_column: str
    total_rows: int
    total_pages: int
    leaf_summaries: int
    reduce_rounds: int


@dataclass
class SummaryResult:
    summary: str
    stats: SummaryStats
    payload: Dict[str, Any]


@dataclass
class SignatureBucket:
    signature: str
    count: int = 0
    examples: List[str] = field(default_factory=list)
    metadata_examples: List[str] = field(default_factory=list)

    def add(
        self,
        message: str,
        metadata_excerpt: str,
        *,
        max_examples_per_signature: int,
    ) -> None:
        self.count += 1

        if message and message not in self.examples:
            if len(self.examples) < max_examples_per_signature:
                self.examples.append(message)

        if metadata_excerpt and metadata_excerpt not in self.metadata_examples:
            if len(self.metadata_examples) < max_examples_per_signature:
                self.metadata_examples.append(metadata_excerpt)


class LogSummarizer:
    """
    Streaming log summarizer with hierarchical map-reduce over LLM calls.

    Interfaces expected from caller:
    - llm_call(prompt: str) -> str
    - db_fetch(start_date: str, limit: int, offset: int) -> markdown_table_str
    """

    def __init__(
        self,
        llm_call: LLMCaller,
        db_fetch: DBMarkdownFetcher,
        config: Optional[SummarizerConfig] = None,
    ) -> None:
        self.llm_call = llm_call
        self.db_fetch = db_fetch
        self.config = config or SummarizerConfig()

    def summarize(self, start_date: str) -> SummaryResult:
        self._validate_start_date(start_date)

        leaf_summaries: List[Dict[str, Any]] = []
        current_buckets: Dict[str, SignatureBucket] = {}
        current_rows = 0
        total_rows = 0
        total_pages = 0
        log_column: Optional[str] = self.config.log_column

        for page_rows in self._iterate_pages(start_date):
            total_pages += 1
            if not page_rows:
                continue

            if log_column is None:
                log_column = self._detect_log_column(page_rows[0].keys())
                if log_column is None:
                    raise ValueError(
                        "Cannot detect log column in markdown table. "
                        "Pass config.log_column explicitly."
                    )

            for row in page_rows:
                raw_message = str(row.get(log_column, "")).strip()
                if not raw_message:
                    continue

                signature = self._normalize_log_for_signature(raw_message)
                metadata_excerpt = self._metadata_excerpt(row, log_column=log_column)

                bucket = current_buckets.get(signature)
                if bucket is None:
                    bucket = SignatureBucket(signature=signature)
                    current_buckets[signature] = bucket

                bucket.add(
                    raw_message,
                    metadata_excerpt,
                    max_examples_per_signature=self.config.max_examples_per_signature,
                )

                current_rows += 1
                total_rows += 1

                if self._should_flush_leaf(current_buckets, current_rows):
                    leaf_summaries.append(
                        self._summarize_leaf(
                            start_date=start_date,
                            buckets=current_buckets.values(),
                            rows_in_leaf=current_rows,
                        )
                    )
                    current_buckets = {}
                    current_rows = 0

        if current_buckets:
            leaf_summaries.append(
                self._summarize_leaf(
                    start_date=start_date,
                    buckets=current_buckets.values(),
                    rows_in_leaf=current_rows,
                )
            )

        if log_column is None:
            log_column = self.config.log_column or "log"

        final_payload, reduce_rounds = self._reduce_summaries(start_date, leaf_summaries)
        final_summary = format_summary_markdown(final_payload)

        stats = SummaryStats(
            start_date=start_date,
            detected_log_column=log_column,
            total_rows=total_rows,
            total_pages=total_pages,
            leaf_summaries=len(leaf_summaries),
            reduce_rounds=reduce_rounds,
        )
        return SummaryResult(summary=final_summary, stats=stats, payload=final_payload)

    def _iterate_pages(self, start_date: str) -> Iterable[List[Dict[str, str]]]:
        offset = 0
        fetched_pages = 0

        while True:
            if self.config.max_pages is not None and fetched_pages >= self.config.max_pages:
                return

            markdown_table = self.db_fetch(
                start_date,
                self.config.page_limit,
                offset,
            )
            rows = parse_markdown_table(markdown_table)
            if not rows:
                return

            yield rows

            fetched_pages += 1
            offset += self.config.page_limit

            if len(rows) < self.config.page_limit:
                return

    def _should_flush_leaf(self, buckets: Dict[str, SignatureBucket], rows_in_leaf: int) -> bool:
        return (
            rows_in_leaf >= self.config.max_leaf_rows
            or len(buckets) >= self.config.max_leaf_signatures
        )

    def _summarize_leaf(
        self,
        *,
        start_date: str,
        buckets: Iterable[SignatureBucket],
        rows_in_leaf: int,
    ) -> Dict[str, Any]:
        ordered = sorted(buckets, key=lambda b: b.count, reverse=True)
        capped = ordered[: self.config.max_signatures_in_leaf_prompt]
        prompt = self._build_leaf_prompt(start_date=start_date, rows_in_leaf=rows_in_leaf, buckets=capped)
        raw_output = self.llm_call(prompt).strip()
        if not raw_output:
            raise RuntimeError("LLM returned empty output for leaf summary.")
        payload = parse_summary_payload(raw_output, stage="leaf")
        ensure_coverage(
            payload,
            input_rows=rows_in_leaf,
            input_signatures=len(ordered),
            top_signatures_considered=len(capped),
        )
        return payload

    def _reduce_summaries(
        self, start_date: str, leaf_summaries: List[Dict[str, Any]]
    ) -> tuple[Dict[str, Any], int]:
        if not leaf_summaries:
            empty_payload = parse_summary_payload("Нет данных за выбранный период.", stage="final")
            ensure_coverage(
                empty_payload,
                input_rows=0,
                input_signatures=0,
                top_signatures_considered=0,
            )
            return empty_payload, 0

        if len(leaf_summaries) == 1:
            single_payload = dict(leaf_summaries[0])
            single_payload["stage"] = "final"
            return single_payload, 0

        round_idx = 0
        current = leaf_summaries

        while len(current) > 1:
            round_idx += 1
            if round_idx > self.config.max_reduce_rounds:
                raise RuntimeError(
                    "Exceeded max reduce rounds. Lower reduce_fan_in or inspect input scale."
                )

            next_level: List[Dict[str, Any]] = []
            for i in range(0, len(current), self.config.reduce_fan_in):
                group = current[i : i + self.config.reduce_fan_in]
                prompt = self._build_reduce_prompt(
                    start_date=start_date,
                    group=group,
                    round_idx=round_idx,
                )
                raw_output = self.llm_call(prompt).strip()
                if not raw_output:
                    raise RuntimeError(
                        f"LLM returned empty output in reduce round {round_idx}."
                    )
                reduced_payload = parse_summary_payload(raw_output, stage="reduce")
                merged = merge_coverages(group)
                ensure_coverage(
                    reduced_payload,
                    input_rows=merged["input_rows"],
                    input_signatures=merged["input_signatures"],
                    top_signatures_considered=merged["top_signatures_considered"],
                )
                next_level.append(reduced_payload)
            current = next_level

        final_payload = dict(current[0])
        final_payload["stage"] = "final"
        full_coverage = merge_coverages(leaf_summaries)
        ensure_coverage(
            final_payload,
            input_rows=full_coverage["input_rows"],
            input_signatures=full_coverage["input_signatures"],
            top_signatures_considered=full_coverage["top_signatures_considered"],
        )
        return final_payload, round_idx

    def _build_leaf_prompt(
        self,
        *,
        start_date: str,
        rows_in_leaf: int,
        buckets: List[SignatureBucket],
    ) -> str:
        schema_hint = (
            '{'
            '"schema_version":"log-summary.v1",'
            '"stage":"leaf",'
            '"summary":"...",'
            '"incidents":[{"title":"...","severity":"low|medium|high|critical","count":0,'
            '"patterns":["..."],"evidence":["..."],"hypothesis":"...","actions":["..."]}],'
            '"key_signals":["..."],'
            '"risks":["..."],'
            '"coverage":{"input_rows":0,"input_signatures":0,"top_signatures_considered":0}'
            '}'
        )
        header = [
            "Ты SRE/Observability аналитик.",
            f"Язык ответа: {self.config.language_hint}.",
            "Твоя задача: сделать краткое, точное summary батча логов в JSON.",
            "Не выдумывай факты. Если причина неочевидна, помечай как 'гипотеза'.",
            "Верни ТОЛЬКО JSON, без markdown и без пояснений.",
            "Схема JSON:",
            schema_hint,
            "",
            f"Период начинается с: {start_date}",
            f"Объем батча (строк): {rows_in_leaf}",
            f"Уникальных сигнатур в батче: {len(buckets)}",
            f"Максимум сигнатур в анализе: {self.config.max_signatures_in_leaf_prompt}",
            "Заполни coverage числами из входа.",
            "",
            "Сигнатуры (по убыванию частоты):",
        ]

        lines: List[str] = []
        for idx, bucket in enumerate(buckets, start=1):
            signature = _truncate(bucket.signature, self.config.max_signature_text_len)
            ex = "; ".join(
                _truncate(v, self.config.max_example_text_len) for v in bucket.examples
            )
            meta = "; ".join(
                _truncate(v, self.config.max_example_text_len) for v in bucket.metadata_examples
            )
            lines.append(
                f"{idx}. count={bucket.count} | signature={signature} | examples={ex or '-'} | meta={meta or '-'}"
            )

        return "\n".join(header + lines)

    def _build_reduce_prompt(
        self,
        *,
        start_date: str,
        group: List[Dict[str, Any]],
        round_idx: int,
    ) -> str:
        schema_hint = (
            '{'
            '"schema_version":"log-summary.v1",'
            '"stage":"reduce",'
            '"summary":"...",'
            '"incidents":[{"title":"...","severity":"low|medium|high|critical","count":0,'
            '"patterns":["..."],"evidence":["..."],"hypothesis":"...","actions":["..."]}],'
            '"key_signals":["..."],'
            '"risks":["..."],'
            '"coverage":{"input_rows":0,"input_signatures":0,"top_signatures_considered":0}'
            '}'
        )
        parts = [
            "Ты объединяешь несколько промежуточных summary логов в одно более высокоуровневое summary.",
            f"Язык ответа: {self.config.language_hint}.",
            "Не теряй критичные инциденты и явные регрессии.",
            "Если есть противоречия между summary, явно укажи это.",
            "Верни ТОЛЬКО JSON, без markdown и без пояснений.",
            "Схема JSON:",
            schema_hint,
            "",
            f"Период начинается с: {start_date}",
            f"Reduce round: {round_idx}",
            f"Количество входных summary: {len(group)}",
            "Coverage посчитай суммой coverage по входам.",
            "",
            "Входные summary (JSON):",
        ]
        for i, summary in enumerate(group, start=1):
            parts.append("")
            parts.append(f"[SUMMARY {i}]")
            parts.append(to_json_string(compact_for_reduce(summary)))

        return "\n".join(parts)

    def _detect_log_column(self, headers: Iterable[str]) -> Optional[str]:
        header_list = [str(h) for h in headers]
        if self.config.log_column and self.config.log_column in header_list:
            return self.config.log_column

        lower_to_original = {h.strip().lower(): h for h in header_list}
        for candidate in DEFAULT_LOG_COLUMN_CANDIDATES:
            if candidate in lower_to_original:
                return lower_to_original[candidate]

        return None

    def _metadata_excerpt(self, row: Dict[str, str], *, log_column: str) -> str:
        items = []
        for key, value in row.items():
            if key == log_column:
                continue
            if value is None or str(value).strip() == "":
                continue
            items.append(f"{key}={str(value).strip()}")
            if len(items) >= self.config.keep_metadata_columns:
                break
        return ", ".join(items)

    def _normalize_log_for_signature(self, raw_log: str) -> str:
        text = raw_log
        text = re.sub(
            r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b",
            "<UUID>",
            text,
        )
        text = re.sub(r"\b\d{1,3}(?:\.\d{1,3}){3}\b", "<IP>", text)
        text = re.sub(r"\b0x[0-9a-fA-F]+\b", "<HEX>", text)
        text = re.sub(r"\b\d+\b", "<NUM>", text)
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            text = "<EMPTY>"
        return text

    def _validate_start_date(self, start_date: str) -> None:
        try:
            datetime.fromisoformat(start_date.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(
                f"start_date must be ISO-like format, got: {start_date!r}"
            ) from exc


def _truncate(value: str, max_len: int) -> str:
    if len(value) <= max_len:
        return value
    return value[: max_len - 3] + "..."
