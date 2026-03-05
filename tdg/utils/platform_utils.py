"""
Platform-agnostic connection and LLM utilities for T - TDD Generator

Replaces direct snowflake_utils usage. All functions maintain the same
call signatures so callers need minimal changes.
"""

import streamlit as st
from config.settings import DEFAULT_MAX_TOKENS, LLM_PLATFORM_AVAILABLE, get_llm_models
from utils.llm_provider import get_llm_provider, LLMProvider

# Module-level cached provider
_provider: LLMProvider = None


def _get_provider() -> LLMProvider:
    global _provider
    if _provider is None:
        _provider = get_llm_provider()
    return _provider


# ==========================================================================
# PUBLIC API — used by streamlit_app.py and other modules
# ==========================================================================

def get_connection():
    """Get platform connection. Returns (connection, error)."""
    return _get_provider().get_connection()


def call_llm(model_name: str, prompt: str, max_tokens: int = DEFAULT_MAX_TOKENS):
    """Call LLM. Returns (response_text, error)."""
    return _get_provider().complete(model_name, prompt, max_tokens)


def get_available_llms() -> list:
    """Get available models for the active platform."""
    return _get_provider().get_available_models()


def execute_sql(sql: str, fetch_results: bool = True):
    """Execute SQL. Returns (results, error)."""
    return _get_provider().execute_sql(sql, fetch_results)


def test_connection() -> tuple:
    """Test connectivity. Returns (success, message)."""
    return _get_provider().test_connection()


def get_platform_name() -> str:
    """Display name of the active platform."""
    return _get_provider().platform_name


def is_platform_available() -> bool:
    """True if any LLM platform SDK is installed."""
    return LLM_PLATFORM_AVAILABLE
