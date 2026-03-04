from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Protocol, runtime_checkable

from .interfaces import DBMarkdownFetcher, LLMCaller
from .pipeline import LogSummarizer, SummarizerConfig, SummaryStats
from .summary_contract import format_summary_for_alert


@dataclass(frozen=True)
class OutlierEvent:
    """
    Outlier produced by anomaly detection.

    timestamp is the anomaly point in ISO-like format (e.g. 2026-03-01T10:00:00Z).
    """

    timestamp: str
    outlier_id: Optional[str] = None
    score: Optional[float] = None
    metadata: Dict[str, str] = field(default_factory=dict)


@runtime_checkable
class WindowDBFetcherFactory(Protocol):
    """
    Build a db_fetch callback bound to a specific time window for one outlier.

    Returned callback must have this contract:
    db_fetch(start_date, limit, offset) -> markdown table.
    """

    def __call__(
        self,
        *,
        window_start: str,
        window_end: str,
        outlier: OutlierEvent,
    ) -> DBMarkdownFetcher:
        ...


@runtime_checkable
class AlertGenerator(Protocol):
    """
    Existing alert pipeline adapter.

    Must accept summary text and outlier timestamp.
    Can return optional alert id / message id.
    """

    def __call__(self, summary: str, outlier_timestamp: str) -> Optional[str]:
        ...


@dataclass
class ControlPlaneConfig:
    lookback: timedelta = timedelta(minutes=30)
    summarizer_config: SummarizerConfig = field(default_factory=SummarizerConfig)
    continue_on_error: bool = True
    format_alert_summary: bool = False


@dataclass
class OutlierRunResult:
    outlier: OutlierEvent
    window_start: str
    window_end: str
    alert_id: Optional[str]
    summary_stats: Optional[SummaryStats]
    alert_summary: Optional[str]
    error: Optional[str] = None


class LogAlertControlPlane:
    """
    Control plane:
    outlier -> fetch logs in lookback window -> summarize -> call alert generator.
    """

    def __init__(
        self,
        *,
        llm_call: LLMCaller,
        db_fetcher_factory: WindowDBFetcherFactory,
        alert_generator: AlertGenerator,
        config: Optional[ControlPlaneConfig] = None,
    ) -> None:
        self.llm_call = llm_call
        self.db_fetcher_factory = db_fetcher_factory
        self.alert_generator = alert_generator
        self.config = config or ControlPlaneConfig()

    def run_for_outliers(self, outliers: Iterable[OutlierEvent]) -> List[OutlierRunResult]:
        results: List[OutlierRunResult] = []

        for outlier in outliers:
            try:
                result = self._process_single_outlier(outlier)
            except Exception as exc:  # noqa: BLE001
                if not self.config.continue_on_error:
                    raise
                result = OutlierRunResult(
                    outlier=outlier,
                    window_start="",
                    window_end="",
                    alert_id=None,
                    summary_stats=None,
                    alert_summary=None,
                    error=str(exc),
                )
            results.append(result)

        return results

    def _process_single_outlier(self, outlier: OutlierEvent) -> OutlierRunResult:
        outlier_dt = _parse_iso_datetime(outlier.timestamp)
        window_end_dt = outlier_dt
        window_start_dt = window_end_dt - self.config.lookback

        window_start = _format_iso_datetime(window_start_dt)
        window_end = _format_iso_datetime(window_end_dt)

        db_fetch = self.db_fetcher_factory(
            window_start=window_start,
            window_end=window_end,
            outlier=outlier,
        )
        summarizer = LogSummarizer(
            llm_call=self.llm_call,
            db_fetch=db_fetch,
            config=self.config.summarizer_config,
        )

        summary_result = summarizer.summarize(start_date=window_start)
        alert_summary = summary_result.summary
        if self.config.format_alert_summary:
            alert_summary = format_summary_for_alert(
                payload=summary_result.payload,
                summary_markdown=summary_result.summary,
                outlier_timestamp=outlier.timestamp,
                window_start=window_start,
                window_end=window_end,
                outlier_id=outlier.outlier_id or "",
                outlier_score=str(outlier.score) if outlier.score is not None else "",
            )

        alert_id = self.alert_generator(alert_summary, outlier.timestamp)

        return OutlierRunResult(
            outlier=outlier,
            window_start=window_start,
            window_end=window_end,
            alert_id=alert_id,
            summary_stats=summary_result.stats,
            alert_summary=alert_summary,
        )


def _parse_iso_datetime(value: str) -> datetime:
    normalized = value.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _format_iso_datetime(value: datetime) -> str:
    iso = value.astimezone(timezone.utc).isoformat()
    return iso.replace("+00:00", "Z")
