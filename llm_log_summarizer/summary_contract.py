from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List


SUMMARY_SCHEMA_VERSION = "log-summary.v1"
ALLOWED_SEVERITIES = {"low", "medium", "high", "critical"}
MAX_INCIDENTS = 8
MAX_SIGNALS = 8
MAX_RISKS = 8


def parse_summary_payload(raw_output: str, *, stage: str) -> Dict[str, Any]:
    """
    Parse LLM output into normalized summary payload.
    If parsing fails, keeps text in `summary` field.
    """
    payload = _extract_json_payload(raw_output)
    if not isinstance(payload, dict):
        payload = {"summary": _clean_fallback_text(raw_output)}

    normalized = _normalize_payload(payload, stage=stage)
    if not normalized["summary"]:
        normalized["summary"] = _clean_fallback_text(raw_output)
    return normalized


def ensure_coverage(
    payload: Dict[str, Any],
    *,
    input_rows: int,
    input_signatures: int,
    top_signatures_considered: int,
) -> Dict[str, Any]:
    payload["coverage"] = {
        "input_rows": _safe_int(input_rows, default=0),
        "input_signatures": _safe_int(input_signatures, default=0),
        "top_signatures_considered": _safe_int(top_signatures_considered, default=0),
    }
    return payload


def merge_coverages(payloads: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    rows = 0
    signatures = 0
    top_considered = 0
    for payload in payloads:
        coverage = payload.get("coverage", {})
        if not isinstance(coverage, dict):
            continue
        rows += _safe_int(coverage.get("input_rows"), default=0)
        signatures += _safe_int(coverage.get("input_signatures"), default=0)
        top_considered += _safe_int(coverage.get("top_signatures_considered"), default=0)
    return {
        "input_rows": rows,
        "input_signatures": signatures,
        "top_signatures_considered": top_considered,
    }


def compact_for_reduce(payload: Dict[str, Any]) -> Dict[str, Any]:
    incidents = payload.get("incidents", [])
    if not isinstance(incidents, list):
        incidents = []
    compact_incidents: List[Dict[str, Any]] = []
    for incident in incidents[:5]:
        if not isinstance(incident, dict):
            continue
        compact_incidents.append(
            {
                "title": str(incident.get("title", "")),
                "severity": str(incident.get("severity", "")),
                "count": _safe_int(incident.get("count"), default=0),
            }
        )

    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "stage": str(payload.get("stage", "")),
        "summary": str(payload.get("summary", "")),
        "incidents": compact_incidents,
        "key_signals": _normalize_str_list(payload.get("key_signals"), limit=5),
        "risks": _normalize_str_list(payload.get("risks"), limit=5),
        "coverage": payload.get("coverage", {}),
    }


def format_summary_markdown(payload: Dict[str, Any]) -> str:
    summary = str(payload.get("summary", "")).strip() or "Нет summary."
    incidents = payload.get("incidents", [])
    if not isinstance(incidents, list):
        incidents = []
    key_signals = _normalize_str_list(payload.get("key_signals"), limit=MAX_SIGNALS)
    risks = _normalize_str_list(payload.get("risks"), limit=MAX_RISKS)
    coverage = payload.get("coverage", {})
    if not isinstance(coverage, dict):
        coverage = {}

    lines: List[str] = []
    lines.append("## TL;DR")
    lines.append(summary)
    lines.append("")
    lines.append("## Инциденты")
    if not incidents:
        lines.append("- Значимых инцидентов не выделено.")
    else:
        for idx, incident in enumerate(incidents[:MAX_INCIDENTS], start=1):
            if not isinstance(incident, dict):
                continue
            title = str(incident.get("title", "")).strip() or f"Инцидент {idx}"
            severity = str(incident.get("severity", "medium")).lower()
            count = _safe_int(incident.get("count"), default=0)
            patterns = _normalize_str_list(incident.get("patterns"), limit=3)
            evidence = _normalize_str_list(incident.get("evidence"), limit=2)
            hypothesis = str(incident.get("hypothesis", "")).strip()
            actions = _normalize_str_list(incident.get("actions"), limit=3)

            lines.append(f"{idx}. [{severity}] {title} (count={count})")
            if patterns:
                lines.append(f"   patterns: {', '.join(patterns)}")
            if evidence:
                lines.append(f"   evidence: {', '.join(evidence)}")
            if hypothesis:
                lines.append(f"   hypothesis: {hypothesis}")
            if actions:
                lines.append(f"   actions: {', '.join(actions)}")

    lines.append("")
    lines.append("## Сигналы")
    if key_signals:
        for signal in key_signals:
            lines.append(f"- {signal}")
    else:
        lines.append("- Нет выделенных сигналов.")

    lines.append("")
    lines.append("## Риски")
    if risks:
        for risk in risks:
            lines.append(f"- {risk}")
    else:
        lines.append("- Нет явных рисков.")

    lines.append("")
    lines.append("## Coverage")
    lines.append(f"- input_rows: {_safe_int(coverage.get('input_rows'), default=0)}")
    lines.append(
        f"- input_signatures: {_safe_int(coverage.get('input_signatures'), default=0)}"
    )
    lines.append(
        "- top_signatures_considered: "
        f"{_safe_int(coverage.get('top_signatures_considered'), default=0)}"
    )

    return "\n".join(lines)


def format_summary_for_alert(
    *,
    payload: Dict[str, Any],
    summary_markdown: str,
    outlier_timestamp: str,
    window_start: str,
    window_end: str,
    outlier_id: str = "",
    outlier_score: str = "",
) -> str:
    incidents = payload.get("incidents", [])
    top_incident_line = "не выделен"
    if isinstance(incidents, list) and incidents:
        first = incidents[0]
        if isinstance(first, dict):
            title = str(first.get("title", "")).strip() or "без названия"
            severity = str(first.get("severity", "medium")).lower()
            count = _safe_int(first.get("count"), default=0)
            top_incident_line = f"[{severity}] {title} (count={count})"

    lines = [
        f"outlier_timestamp: {outlier_timestamp}",
        f"window_start: {window_start}",
        f"window_end: {window_end}",
        f"outlier_id: {outlier_id or '-'}",
        f"outlier_score: {outlier_score or '-'}",
        f"top_incident: {top_incident_line}",
        "",
        summary_markdown,
    ]
    return "\n".join(lines)


def to_json_string(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _normalize_payload(payload: Dict[str, Any], *, stage: str) -> Dict[str, Any]:
    summary = str(payload.get("summary", "")).strip()
    incidents_raw = payload.get("incidents")
    if not isinstance(incidents_raw, list):
        incidents_raw = []

    incidents: List[Dict[str, Any]] = []
    for item in incidents_raw[:MAX_INCIDENTS]:
        if not isinstance(item, dict):
            continue
        severity = str(item.get("severity", "medium")).lower()
        if severity not in ALLOWED_SEVERITIES:
            severity = "medium"
        incidents.append(
            {
                "title": str(item.get("title", "")).strip(),
                "severity": severity,
                "count": _safe_int(item.get("count"), default=0),
                "patterns": _normalize_str_list(item.get("patterns"), limit=4),
                "evidence": _normalize_str_list(item.get("evidence"), limit=3),
                "hypothesis": str(item.get("hypothesis", "")).strip(),
                "actions": _normalize_str_list(item.get("actions"), limit=4),
            }
        )

    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "stage": stage,
        "summary": summary,
        "incidents": incidents,
        "key_signals": _normalize_str_list(payload.get("key_signals"), limit=MAX_SIGNALS),
        "risks": _normalize_str_list(payload.get("risks"), limit=MAX_RISKS),
        "coverage": _normalize_coverage(payload.get("coverage")),
    }


def _normalize_coverage(value: Any) -> Dict[str, int]:
    if not isinstance(value, dict):
        value = {}
    return {
        "input_rows": _safe_int(value.get("input_rows"), default=0),
        "input_signatures": _safe_int(value.get("input_signatures"), default=0),
        "top_signatures_considered": _safe_int(
            value.get("top_signatures_considered"), default=0
        ),
    }


def _normalize_str_list(value: Any, *, limit: int) -> List[str]:
    if not isinstance(value, list):
        return []
    items: List[str] = []
    for raw in value:
        text = str(raw).strip()
        if not text:
            continue
        if text not in items:
            items.append(text)
        if len(items) >= limit:
            break
    return items


def _extract_json_payload(raw_output: str) -> Any:
    text = raw_output.strip()
    if not text:
        return None

    fenced = re.search(r"```(?:json)?\s*(\{[\s\S]*\})\s*```", text, flags=re.IGNORECASE)
    if fenced:
        text = fenced.group(1).strip()

    try:
        return json.loads(text)
    except Exception:  # noqa: BLE001
        pass

    left = text.find("{")
    right = text.rfind("}")
    if left >= 0 and right > left:
        candidate = text[left : right + 1]
        try:
            return json.loads(candidate)
        except Exception:  # noqa: BLE001
            return None

    return None


def _clean_fallback_text(raw_output: str) -> str:
    text = raw_output.strip()
    if not text:
        return "LLM вернула пустой ответ."
    if len(text) > 1200:
        return text[:1197] + "..."
    return text


def _safe_int(value: Any, *, default: int) -> int:
    try:
        parsed = int(value)
    except Exception:  # noqa: BLE001
        return default
    if parsed < 0:
        return 0
    return parsed
