from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class LLMCaller(Protocol):
    """A callable interface for a plain-text LLM invocation."""

    def __call__(self, prompt: str) -> str:
        """Return model output for the given prompt."""


@runtime_checkable
class DBMarkdownFetcher(Protocol):
    """A callable interface for paginated markdown-table fetching."""

    def __call__(self, start_date: str, limit: int, offset: int) -> str:
        """Return a markdown table for [start_date, +inf) with limit/offset pagination."""

