from __future__ import annotations

import os
from typing import Callable


def create_llm_client():
    """
    Creates and returns a client for communicating with LLM.
    """
    try:
        from retry_openai_model import RetryOpenAIServerModel
    except ImportError as exc:
        raise ImportError(
            "retry_openai_model is required. Install package with RetryOpenAIServerModel."
        ) from exc

    return RetryOpenAIServerModel(
        model_id="PNX.QWEN3 235b a22b instruct",
        api_base=os.getenv("OPENAI_API_BASE_DB"),
        api_key=os.getenv("OPENAI_API_KEY_DB"),
        max_retries=3,
    )


def communicate_with_llm(message: str, system_prompt: str = "") -> str:
    """
    Sends a message to LLM and returns the response.
    """
    try:
        from smolagents import ChatMessage, MessageRole
    except ImportError as exc:
        raise ImportError("smolagents is required for ChatMessage and MessageRole.") from exc

    try:
        model = create_llm_client()
        messages = []
        if system_prompt:
            messages.append(ChatMessage(role=MessageRole.SYSTEM, content=system_prompt))
        messages.append(ChatMessage(role=MessageRole.USER, content=message))

        response = model(messages)

        if hasattr(response, "content"):
            return response.content
        if hasattr(response, "choices") and response.choices:
            return response.choices[0].message.content
        return str(response)
    except Exception as exc:  # noqa: BLE001
        return f"Error communicating with LLM: {str(exc)}"


def make_llm_call(system_prompt: str = "") -> Callable[[str], str]:
    """
    Adapter for summarizer interface:
    llm_call(prompt: str) -> str
    """

    def _llm_call(prompt: str) -> str:
        return communicate_with_llm(message=prompt, system_prompt=system_prompt)

    return _llm_call


def has_required_env() -> bool:
    """
    Quick check if api base and key exist in env.
    """
    return bool(os.getenv("OPENAI_API_BASE_DB")) and bool(os.getenv("OPENAI_API_KEY_DB"))
