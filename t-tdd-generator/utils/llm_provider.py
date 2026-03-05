"""
LLM Provider Abstraction Layer
================================
Unified interface for LLM calls across platforms:
- Snowflake Cortex (Streamlit in Snowflake / local connector)
- Databricks Foundation Model APIs (Databricks Apps / local SDK)

Usage:
    from utils.llm_provider import get_llm_provider

    provider = get_llm_provider()               # auto-detect platform
    provider = get_llm_provider("databricks")    # force platform

    response, error = provider.complete("model-name", "prompt text")
    conn, error     = provider.get_connection()
    models          = provider.get_available_models()
"""

import os
import json
import logging
from abc import ABC, abstractmethod

import streamlit as st

logger = logging.getLogger(__name__)


# =============================================================================
# ABSTRACT BASE
# =============================================================================
class LLMProvider(ABC):
    """Abstract base class for all LLM providers."""

    @abstractmethod
    def get_connection(self):
        """Returns (connection_object, error_string_or_None)."""

    @abstractmethod
    def complete(self, model_name: str, prompt: str, max_tokens: int = 4000):
        """Returns (response_text, error_string_or_None)."""

    @abstractmethod
    def get_available_models(self) -> list:
        """Returns list of {'name': ..., 'description': ...} dicts."""

    @abstractmethod
    def execute_sql(self, sql: str, fetch_results: bool = True):
        """Returns (results, error_string_or_None)."""

    @abstractmethod
    def test_connection(self) -> tuple:
        """Returns (success_bool, message_string)."""

    @property
    @abstractmethod
    def platform_name(self) -> str:
        """Display name for UI labels."""

    @property
    @abstractmethod
    def is_available(self) -> bool:
        """True if the SDK/connector can be imported."""

    @property
    @abstractmethod
    def is_native(self) -> bool:
        """True if running natively inside the platform."""


# =============================================================================
# SNOWFLAKE CORTEX PROVIDER
# =============================================================================
class SnowflakeCortexProvider(LLMProvider):
    """LLM provider using Snowflake Cortex COMPLETE() function."""

    def __init__(self):
        self._conn = None
        self._conn_error = None
        self._conn_checked = False

        # Detect Snowflake environment
        self._running_in_sis = False
        self._snowflake_available = False

        try:
            from snowflake.snowpark.context import get_active_session  # noqa: F401
            self._running_in_sis = True
            self._snowflake_available = True
        except ImportError:
            pass

        if not self._running_in_sis:
            try:
                import snowflake.connector  # noqa: F401
                self._snowflake_available = True
            except ImportError:
                pass

    # -- properties ----------------------------------------------------------
    @property
    def platform_name(self) -> str:
        return "Snowflake Cortex"

    @property
    def is_available(self) -> bool:
        return self._snowflake_available

    @property
    def is_native(self) -> bool:
        return self._running_in_sis

    # -- connection ----------------------------------------------------------
    def get_connection(self):
        if self._conn_checked:
            return self._conn, self._conn_error
        self._conn_checked = True

        # Method 1: Streamlit in Snowflake (SiS)
        if self._running_in_sis:
            try:
                from snowflake.snowpark.context import get_active_session
                self._conn = get_active_session()
                return self._conn, None
            except Exception as e:
                self._conn_error = f"SiS session error: {e}"
                return None, self._conn_error

        # Method 2: st.connection (Streamlit native Snowflake connection)
        try:
            self._conn = st.connection("snowflake")
            return self._conn, None
        except Exception:
            pass

        # Method 3: snowflake.connector with secrets.toml
        if not self._snowflake_available:
            self._conn_error = "Snowflake connector not installed"
            return None, self._conn_error

        try:
            import snowflake.connector

            if "snowflake" in st.secrets:
                sf = st.secrets["snowflake"]
                params = {
                    "account": sf.get("account"),
                    "user": sf.get("user"),
                    "warehouse": sf.get("warehouse"),
                    "database": sf.get("database"),
                    "schema": sf.get("schema"),
                }
                if sf.get("role"):
                    params["role"] = sf["role"]

                auth = sf.get("authenticator", "")
                if sf.get("token"):
                    params["token"] = sf["token"]
                    params["authenticator"] = "oauth"
                elif sf.get("password"):
                    params["password"] = sf["password"]
                elif auth:
                    params["authenticator"] = auth

                self._conn = snowflake.connector.connect(**params)
                return self._conn, None
        except Exception as e:
            self._conn_error = str(e)
            return None, self._conn_error

        self._conn_error = "No Snowflake configuration found"
        return None, self._conn_error

    # -- LLM call ------------------------------------------------------------
    def complete(self, model_name: str, prompt: str, max_tokens: int = 4000):
        conn, error = self.get_connection()
        if not conn:
            return None, f"No Snowflake connection: {error}"

        try:
            escaped = prompt.replace("'", "''")
            query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                '{model_name}',
                '{escaped}'
            ) as response
            """

            if self._running_in_sis:
                result = conn.sql(query).collect()
                if result and len(result) > 0:
                    return result[0]["RESPONSE"], None
            elif hasattr(conn, "cursor"):
                cursor = conn.cursor()
                cursor.execute(query)
                result = cursor.fetchone()
                if result:
                    return result[0], None
            elif hasattr(conn, "session"):
                result = conn.session.sql(query).collect()
                if result and len(result) > 0:
                    return result[0]["RESPONSE"], None
            else:
                result = conn.sql(query).collect()
                if result and len(result) > 0:
                    return result[0][0], None

            return None, "No response from LLM"
        except Exception as e:
            return None, str(e)

    # -- SQL -----------------------------------------------------------------
    def execute_sql(self, sql: str, fetch_results: bool = True):
        conn, error = self.get_connection()
        if not conn:
            return None, f"No Snowflake connection: {error}"

        try:
            if self._running_in_sis:
                result = conn.sql(sql).collect()
                return (result if fetch_results else True), None
            elif hasattr(conn, "cursor"):
                cursor = conn.cursor()
                cursor.execute(sql)
                return (cursor.fetchall() if fetch_results else True), None
            elif hasattr(conn, "session"):
                result = conn.session.sql(sql).collect()
                return (result if fetch_results else True), None
            else:
                result = conn.sql(sql).collect()
                return (result if fetch_results else True), None
        except Exception as e:
            return None, str(e)

    def test_connection(self) -> tuple:
        result, error = self.execute_sql("SELECT CURRENT_TIMESTAMP()")
        if error:
            return False, f"Connection test failed: {error}"
        return True, "Snowflake connection successful"

    def get_available_models(self) -> list:
        return _load_models_from_config()


# =============================================================================
# DATABRICKS PROVIDER
# =============================================================================
class DatabricksProvider(LLMProvider):
    """LLM provider using Databricks Foundation Model APIs / Model Serving."""

    def __init__(self):
        self._client = None
        self._sql_conn = None
        self._conn_error = None
        self._conn_checked = False

        # Detect Databricks environment
        self._running_in_databricks = False
        self._databricks_available = False

        try:
            from databricks.sdk import WorkspaceClient  # noqa: F401
            self._databricks_available = True
            if os.environ.get("DATABRICKS_RUNTIME_VERSION") or os.environ.get("DATABRICKS_HOST"):
                self._running_in_databricks = True
        except ImportError:
            pass

        if not self._databricks_available:
            try:
                from databricks import sql as _  # noqa: F401
                self._databricks_available = True
            except ImportError:
                pass

    # -- properties ----------------------------------------------------------
    @property
    def platform_name(self) -> str:
        return "Databricks Foundation Models"

    @property
    def is_available(self) -> bool:
        return self._databricks_available

    @property
    def is_native(self) -> bool:
        return self._running_in_databricks

    # -- connection ----------------------------------------------------------
    def get_connection(self):
        if self._conn_checked:
            return self._client, self._conn_error
        self._conn_checked = True

        # Method 1: Running inside Databricks (Apps / Notebook)
        if self._running_in_databricks:
            try:
                from databricks.sdk import WorkspaceClient
                self._client = WorkspaceClient()
                return self._client, None
            except Exception as e:
                self._conn_error = f"Databricks SDK error: {e}"
                return None, self._conn_error

        # Method 2: Local dev with secrets.toml
        if "databricks" in st.secrets:
            try:
                db = st.secrets["databricks"]
                host = db.get("host", "")
                token = db.get("token", "")

                if not host or not token:
                    self._conn_error = "Databricks host and token required in secrets.toml"
                    return None, self._conn_error

                full_host = f"https://{host}" if not host.startswith("http") else host

                # Prefer WorkspaceClient SDK
                try:
                    from databricks.sdk import WorkspaceClient
                    self._client = WorkspaceClient(host=full_host, token=token)
                    return self._client, None
                except ImportError:
                    pass

                # Fallback to REST API config dict
                self._client = {
                    "host": full_host,
                    "token": token,
                    "http_path": db.get("http_path", ""),
                }
                return self._client, None

            except Exception as e:
                self._conn_error = str(e)
                return None, self._conn_error

        self._conn_error = "No Databricks configuration found in secrets.toml"
        return None, self._conn_error

    def _get_sql_connection(self):
        """Get a Databricks SQL connection (separate from WorkspaceClient)."""
        if self._sql_conn:
            return self._sql_conn, None

        if "databricks" in st.secrets:
            try:
                from databricks import sql as databricks_sql
                db = st.secrets["databricks"]
                self._sql_conn = databricks_sql.connect(
                    server_hostname=db.get("host", ""),
                    http_path=db.get("http_path", ""),
                    access_token=db.get("token", ""),
                )
                return self._sql_conn, None
            except ImportError:
                return None, "databricks-sql-connector not installed"
            except Exception as e:
                return None, str(e)

        return None, "No Databricks SQL config found"

    # -- LLM call ------------------------------------------------------------
    def complete(self, model_name: str, prompt: str, max_tokens: int = 4000):
        client, error = self.get_connection()
        if not client:
            return None, f"No Databricks connection: {error}"

        response, err = self._call_model(client, model_name, prompt, max_tokens)
        if response is not None:
            return response, None

        # Graceful fallback: if an external model (e.g. claude-sonnet-4-6) is not
        # available, fall back to the first Databricks-native model.
        is_external = not model_name.startswith("databricks-")
        not_found = err and ("RESOURCE_DOES_NOT_EXIST" in err or "404" in err or "not found" in str(err).lower())

        if is_external and not_found:
            logger.warning("External model '%s' not found – falling back to native model.", model_name)
            try:
                st.warning(
                    f"Model '{model_name}' not found on Databricks. "
                    "Set up an External Model Serving endpoint to use it. "
                    "Falling back to a native Databricks model."
                )
            except Exception:
                pass

            fallback_models = _default_databricks_models()
            native = [m['name'] for m in fallback_models if m['name'].startswith("databricks-")]
            if native:
                fb_response, fb_err = self._call_model(client, native[0], prompt, max_tokens)
                if fb_response is not None:
                    return fb_response, None
                return None, f"Fallback model '{native[0]}' also failed: {fb_err}"

        return None, err

    def _call_model(self, client, model_name: str, prompt: str, max_tokens: int):
        """Low-level model call via SDK or REST. Returns (response, error)."""
        # --- SDK path (WorkspaceClient) ---
        if hasattr(client, "serving_endpoints"):
            try:
                from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
                messages = [ChatMessage(role=ChatMessageRole.USER, content=prompt)]
                response = client.serving_endpoints.query(
                    name=model_name,
                    messages=messages,
                    max_tokens=max_tokens,
                )
                # Response may be object or dict depending on SDK version
                if hasattr(response, 'choices'):
                    return response.choices[0].message.content, None
                elif isinstance(response, dict) and "choices" in response:
                    return response["choices"][0]["message"]["content"], None
                else:
                    return None, f"Unexpected SDK response type: {type(response)}"
            except ImportError:
                # ChatMessage/ChatMessageRole not available in this SDK version,
                # fall through to REST API
                logger.warning("ChatMessage not available in SDK, falling through to REST.")
            except Exception as e:
                sdk_error = str(e)
                if "RESOURCE_DOES_NOT_EXIST" not in sdk_error:
                    return None, sdk_error
                # Fall through to REST API

        # --- REST API path ---
        try:
            import requests

            if isinstance(client, dict):
                host, token = client["host"], client["token"]
            elif hasattr(client, "config"):
                host = client.config.host
                token = getattr(client.config, 'token', None)
                if not token:
                    # In Databricks Apps, use OAuth via SDK headers
                    try:
                        headers_factory = client.config.authenticate
                        auth_headers = headers_factory()
                    except Exception:
                        auth_headers = None
            else:
                return None, "Cannot determine Databricks host/token for REST call"

            url = f"{host}/serving-endpoints/{model_name}/invocations"

            # Build auth headers
            if isinstance(client, dict) or token:
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                }
            elif auth_headers:
                headers = {**auth_headers, "Content-Type": "application/json"}
            else:
                return None, "Cannot authenticate for REST call"

            payload = {
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
            }

            resp = requests.post(url, headers=headers, json=payload, timeout=120)
            resp.raise_for_status()
            data = resp.json()

            if "choices" in data and data["choices"]:
                return data["choices"][0]["message"]["content"], None

            return None, f"Unexpected response format: {list(data.keys())}"
        except Exception as e:
            return None, str(e)

    # -- SQL -----------------------------------------------------------------
    def execute_sql(self, sql: str, fetch_results: bool = True):
        conn, error = self._get_sql_connection()
        if not conn:
            return None, f"No Databricks SQL connection: {error}"

        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            return (cursor.fetchall() if fetch_results else True), None
        except Exception as e:
            return None, str(e)

    def test_connection(self) -> tuple:
        client, error = self.get_connection()
        if not client:
            return False, f"Connection failed: {error}"

        try:
            if hasattr(client, "serving_endpoints"):
                endpoints = list(client.serving_endpoints.list())
                return True, f"Databricks connected ({len(endpoints)} serving endpoints)"
            elif isinstance(client, dict):
                return True, "Databricks REST config loaded"
        except Exception as e:
            return False, f"Connection test failed: {e}"

        return True, "Databricks connection initialized"

    def get_available_models(self) -> list:
        models = _load_models_from_config()
        if models:
            return models

        # Try listing serving endpoints dynamically
        client, _ = self.get_connection()
        if client and hasattr(client, "serving_endpoints"):
            try:
                endpoints = list(client.serving_endpoints.list())
                dynamic = [
                    {"name": ep.name, "description": f"{ep.name} (Serving)"}
                    for ep in endpoints
                    if ep.state and ep.state.ready == "READY"
                ]
                if dynamic:
                    return dynamic
            except Exception:
                pass

        return _default_databricks_models()


# =============================================================================
# CLAUDE (ANTHROPIC API) PROVIDER
# =============================================================================
class ClaudeProvider(LLMProvider):
    """LLM provider using the Anthropic Claude API directly."""

    DEFAULT_MODELS = [
        {"name": "claude-sonnet-4-6",        "description": "Claude Sonnet 4.6 — fast, balanced (Recommended)"},
        {"name": "claude-opus-4-6",           "description": "Claude Opus 4.6 — highest quality reasoning"},
        {"name": "claude-haiku-4-5-20251001", "description": "Claude Haiku 4.5 — fastest, lowest cost"},
    ]

    def __init__(self):
        self._client = None
        self._conn_error = None
        self._conn_checked = False
        self._claude_available = False

        try:
            import anthropic  # noqa: F401
            self._claude_available = True
        except ImportError:
            pass

    # -- properties ----------------------------------------------------------
    @property
    def platform_name(self) -> str:
        return "Anthropic Claude"

    @property
    def is_available(self) -> bool:
        return self._claude_available

    @property
    def is_native(self) -> bool:
        return False  # always external API

    # -- connection ----------------------------------------------------------
    def get_connection(self):
        if self._conn_checked:
            return self._client, self._conn_error
        self._conn_checked = True

        if not self._claude_available:
            self._conn_error = "anthropic package not installed. Run: pip install anthropic"
            return None, self._conn_error

        try:
            import anthropic

            # Read API key from secrets.toml [claude] or [anthropic] section
            api_key = None
            try:
                if "claude" in st.secrets:
                    api_key = st.secrets["claude"].get("api_key")
                if not api_key and "anthropic" in st.secrets:
                    api_key = st.secrets["anthropic"].get("api_key")
            except Exception:
                pass

            # Fallback: ANTHROPIC_API_KEY environment variable
            if not api_key:
                api_key = os.environ.get("ANTHROPIC_API_KEY")

            if not api_key:
                self._conn_error = (
                    "No Claude API key found. Add to .streamlit/secrets.toml:\n"
                    "[claude]\napi_key = \"sk-ant-...\""
                )
                return None, self._conn_error

            self._client = anthropic.Anthropic(api_key=api_key)
            return self._client, None

        except Exception as e:
            self._conn_error = str(e)
            return None, self._conn_error

    # -- LLM call ------------------------------------------------------------
    def complete(self, model_name: str, prompt: str, max_tokens: int = 4000):
        client, error = self.get_connection()
        if not client:
            return None, f"Claude connection error: {error}"

        try:
            response = client.messages.create(
                model=model_name,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text if response.content else ""
            return text, None
        except Exception as e:
            return None, str(e)

    # -- SQL -----------------------------------------------------------------
    def execute_sql(self, sql: str, fetch_results: bool = True):
        return None, "SQL execution is not supported by the Claude provider."

    def test_connection(self) -> tuple:
        client, error = self.get_connection()
        if not client:
            return False, f"Claude connection failed: {error}"
        try:
            # Cheap test call
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=10,
                messages=[{"role": "user", "content": "Hi"}],
            )
            return True, f"Claude API connected (model: {response.model})"
        except Exception as e:
            return False, f"Claude API test failed: {e}"

    def get_available_models(self) -> list:
        models = _load_models_from_config()
        return models if models else self.DEFAULT_MODELS


# =============================================================================
# HELPERS
# =============================================================================
def _load_models_from_config() -> list:
    """Load LLM models from config file (shared across providers)."""
    from config.settings import get_llm_models
    return get_llm_models()


def _default_databricks_models() -> list:
    from config.settings import DEFAULT_DATABRICKS_LLM_MODELS
    return DEFAULT_DATABRICKS_LLM_MODELS


# =============================================================================
# PLATFORM DETECTION & FACTORY
# =============================================================================
def detect_platform() -> str:
    """
    Auto-detect the current platform.
    Returns: 'snowflake', 'databricks', or 'unknown'
    """
    # Databricks env vars (highest priority)
    if os.environ.get("DATABRICKS_RUNTIME_VERSION") or os.environ.get("DATABRICKS_HOST"):
        return "databricks"

    # Snowflake SiS
    try:
        from snowflake.snowpark.context import get_active_session  # noqa: F401
        return "snowflake"
    except ImportError:
        pass

    # secrets.toml hints
    try:
        if "databricks" in st.secrets:
            return "databricks"
        if "snowflake" in st.secrets:
            return "snowflake"
        if "claude" in st.secrets or "anthropic" in st.secrets:
            return "claude"
    except Exception:
        pass

    # ANTHROPIC_API_KEY env var
    if os.environ.get("ANTHROPIC_API_KEY"):
        return "claude"

    return "unknown"


def get_llm_provider(platform: str = None) -> LLMProvider:
    """
    Factory: returns the appropriate LLM provider.
    Auto-detects if platform is None.
    """
    if platform is None:
        platform = detect_platform()

    if platform == "databricks":
        return DatabricksProvider()
    elif platform == "snowflake":
        return SnowflakeCortexProvider()
    elif platform == "claude":
        return ClaudeProvider()
    else:
        # Try whichever SDK is installed and configured
        claude = ClaudeProvider()
        if claude.is_available:
            return claude
        sf = SnowflakeCortexProvider()
        if sf.is_available:
            return sf
        db = DatabricksProvider()
        if db.is_available:
            return db
        return claude  # default fallback (will show a clear install message)
