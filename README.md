# LLM Log Summarizer

Streaming log summarization for very large datasets with hierarchical map-reduce.

## Interfaces you provide

1. `llm_call(prompt: str) -> str`
- Input: plain prompt string.
- Output: plain model output string.

2. `db_fetch(start_date: str, limit: int, offset: int) -> str`
- Input: start date + pagination params.
- Output: markdown table string.

Example markdown expected from DB callback:

```md
| ts | service | level | log |
| --- | --- | --- | --- |
| 2026-03-01T10:00:00Z | payments | ERROR | Timeout while calling card-gateway request_id=ab12 |
```

## Integrated LLM adapter (your code)

Built-in adapter module:

- `create_llm_client()`
- `communicate_with_llm(message: str, system_prompt: str = "") -> str`
- `make_llm_call(system_prompt: str = "") -> Callable[[str], str]`

File: `llm_log_summarizer/llm_adapter.py`

Runtime requirements:

- `retry_openai_model` (with `RetryOpenAIServerModel`)
- `smolagents` (for `ChatMessage`, `MessageRole`)
- env vars: `OPENAI_API_BASE_DB`, `OPENAI_API_KEY_DB`

Enable real LLM in examples:

```bash
export OPENAI_API_BASE_DB=...
export OPENAI_API_KEY_DB=...
python3 example_usage.py
```

## LLM prompt/output contract

All summarizer stages use one strict output format:

```json
{
  "schema_version": "log-summary.v1",
  "stage": "leaf|reduce|final",
  "summary": "short text",
  "incidents": [
    {
      "title": "incident name",
      "severity": "low|medium|high|critical",
      "count": 123,
      "patterns": ["..."],
      "evidence": ["..."],
      "hypothesis": "...",
      "actions": ["..."]
    }
  ],
  "key_signals": ["..."],
  "risks": ["..."],
  "coverage": {
    "input_rows": 0,
    "input_signatures": 0,
    "top_signatures_considered": 0
  }
}
```

Important:

- LLM is asked to return JSON only.
- Parser normalizes and validates fields.
- If LLM returns non-JSON text, fallback parser keeps text in `summary` and pipeline continues.

Contract implementation: `llm_log_summarizer/summary_contract.py`

## Prompt strategy

1. Leaf prompt:
- Input: normalized log signatures + counts + examples + metadata.
- Output: JSON `stage=leaf`.

2. Reduce prompt:
- Input: compact JSON summaries from previous layer.
- Output: JSON `stage=reduce`.

3. Final output:
- Last reduce node is converted to markdown for human-readable alert body.
- Structured payload is still available as `SummaryResult.payload`.

Prompt builders are in `llm_log_summarizer/pipeline.py`:
- `_build_leaf_prompt(...)`
- `_build_reduce_prompt(...)`

## Flow formatting (control plane)

By default `LogAlertControlPlane` sends the final summary text as-is to alert generator
(`ControlPlaneConfig.format_alert_summary=False`), so `task` gets the exact final summary.

Optional mode (`format_alert_summary=True`) formats alert text as:

- outlier metadata (`timestamp`, `window_start`, `window_end`, `outlier_id`, `score`)
- top incident line
- full markdown summary

Formatter: `format_summary_for_alert(...)` in `summary_contract.py`.
This string is passed to your `alert_generator(summary, outlier_timestamp)`.

## Quick start

```python
from llm_log_summarizer import LogSummarizer, SummarizerConfig

def llm_call(prompt: str) -> str:
    ...

def db_fetch(start_date: str, limit: int, offset: int) -> str:
    ...

config = SummarizerConfig(
    page_limit=1000,
    max_leaf_rows=5000,
    max_leaf_signatures=250,
    reduce_fan_in=8,
)

summarizer = LogSummarizer(
    llm_call=llm_call,
    db_fetch=db_fetch,
    config=config,
)
result = summarizer.summarize(start_date="2026-03-01T00:00:00Z")

print(result.summary)
print(result.stats)
print(result.payload)
```

## Why this avoids context overflow

- Logs are fetched page-by-page from DB (`limit`/`offset`).
- Each page contributes to small signature buckets.
- Buckets are summarized into leaf summaries.
- Leaf summaries are recursively reduced (`reduce_fan_in`) until one final summary remains.

So the model never receives all raw logs at once.

## Demo

Run:

```bash
python example_usage.py
```

## Control Plane (outlier -> summary -> alert)

If you already have outlier timestamps and an alert pipeline, use control plane:

```python
from llm_log_summarizer import (
    ControlPlaneConfig,
    LogAlertControlPlane,
    OutlierEvent,
    SummarizerConfig,
)
```

Required integrations:

1. `llm_call(prompt: str) -> str`
2. `db_fetcher_factory(window_start: str, window_end: str, outlier: OutlierEvent) -> db_fetch`
3. `alert_generator(summary: str, outlier_timestamp: str) -> Optional[str]`

For alert generation+send via API use built-in adapter:

```python
from llm_log_summarizer import make_workflow_alert_generator

alert_generator = make_workflow_alert_generator(
    api_url="http://localhost:8000/run-workflow",
    workflow_template="clickhouse_text_to_sql",
    webhook_url="",
)
```

It sends:

```json
{
  "workflow_template": "clickhouse_text_to_sql",
  "task": "<final summary>",
  "webhook_url": ""
}
```

The produced `db_fetch` must still follow:

`db_fetch(start_date: str, limit: int, offset: int) -> markdown table`

Run demo:

```bash
python3 example_control_plane.py
```
