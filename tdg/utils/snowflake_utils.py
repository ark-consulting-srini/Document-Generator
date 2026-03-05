"""
Backward-compatibility shim for snowflake_utils.

All logic now lives in utils/platform_utils.py and utils/llm_provider.py.
This file re-exports the old names so existing imports keep working.
"""

from utils.platform_utils import (
    get_connection as get_snowflake_connection,
    call_llm as _call_llm,
    get_available_llms,
    execute_sql,
    test_connection,
    get_platform_name,
    is_platform_available,
)
from config.settings import DEFAULT_MAX_TOKENS


def call_snowflake_llm(conn, model_name: str, prompt: str, max_tokens: int = DEFAULT_MAX_TOKENS):
    """Legacy wrapper — 'conn' is accepted but ignored (provider manages its own connection)."""
    return _call_llm(model_name, prompt, max_tokens)
