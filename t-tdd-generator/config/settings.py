"""
Application settings and configuration for T - TDD Generator
Platform-agnostic: supports both Snowflake and Databricks.
"""

import os
import json

# =============================================================================
# APPLICATION SETTINGS
# =============================================================================
APP_TITLE = "T - TDD Generator"
APP_ICON = "📊"
PAGE_LAYOUT = "wide"
APP_VERSION = "2.1.0"

# =============================================================================
# FEATURE FLAGS
# =============================================================================
ENABLE_SQL_GENERATOR = True
ENABLE_DATA_MODEL = True
ENABLE_BRD_GENERATION = True
ENABLE_STTM_UPDATE = True

# =============================================================================
# DEFAULT VALUES
# =============================================================================
DEFAULT_MAX_TOKENS = 4000
DEFAULT_BRD_MODE = "auto"
TOKEN_THRESHOLD_FOR_OPTIMIZED = 15000

# =============================================================================
# PLATFORM DETECTION — SDK availability flags
# The active provider is chosen at runtime by llm_provider.detect_platform().
# =============================================================================

# -- Snowflake ---------------------------------------------------------------
SNOWFLAKE_AVAILABLE = False
RUNNING_IN_SIS = False

try:
    from snowflake.snowpark.context import get_active_session  # noqa: F401
    RUNNING_IN_SIS = True
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    pass

if not RUNNING_IN_SIS:
    try:
        import snowflake.connector  # noqa: F401
        SNOWFLAKE_AVAILABLE = True
    except ImportError:
        pass

# -- Databricks --------------------------------------------------------------
DATABRICKS_AVAILABLE = False
RUNNING_IN_DATABRICKS = False

try:
    from databricks.sdk import WorkspaceClient  # noqa: F401
    DATABRICKS_AVAILABLE = True
    if os.environ.get("DATABRICKS_RUNTIME_VERSION") or os.environ.get("DATABRICKS_HOST"):
        RUNNING_IN_DATABRICKS = True
except ImportError:
    pass

if not DATABRICKS_AVAILABLE:
    try:
        from databricks import sql as _db_sql  # noqa: F401
        DATABRICKS_AVAILABLE = True
    except ImportError:
        pass

# -- Anthropic Claude --------------------------------------------------------
CLAUDE_AVAILABLE = False
try:
    import anthropic  # noqa: F401
    CLAUDE_AVAILABLE = True
except ImportError:
    pass

# -- Unified flag: True if ANY platform is available -------------------------
LLM_PLATFORM_AVAILABLE = SNOWFLAKE_AVAILABLE or DATABRICKS_AVAILABLE or CLAUDE_AVAILABLE

# =============================================================================
# GRAPHVIZ
# =============================================================================
GRAPHVIZ_AVAILABLE = False
try:
    import graphviz  # noqa: F401
    GRAPHVIZ_AVAILABLE = True
except ImportError:
    pass

# =============================================================================
# LLM MODELS — Defaults per platform
# =============================================================================
# Snowflake Cortex model names — must match SNOWFLAKE.CORTEX.COMPLETE() catalog
DEFAULT_LLM_MODELS = [
    {"name": "claude-sonnet-4-6", "description": "Claude Sonnet 4.6 - Latest reasoning (Primary)"},
    {"name": "claude-sonnet-4-5", "description": "Claude Sonnet 4.5 - Enhanced reasoning (Secondary)"},
    {"name": "claude-opus-4-6", "description": "Claude Opus 4.6 - Highest quality reasoning (Consolidation)"},
    {"name": "claude-4-sonnet", "description": "Claude 4 Sonnet - Advanced reasoning"},
    {"name": "claude-4-opus", "description": "Claude 4 Opus - High quality reasoning"},
    {"name": "claude-3-7-sonnet", "description": "Claude 3.7 Sonnet - Good reasoning"},
    {"name": "llama3.3-70b", "description": "Llama 3.3 70B - Good balance"},
    {"name": "mistral-large2", "description": "Mistral Large 2 - High quality"},
    {"name": "deepseek-r1", "description": "DeepSeek R1 - Strong reasoning"},
    {"name": "llama3.1-405b", "description": "Llama 3.1 405B - Largest open model"},
]

# Databricks model names — must match Model Serving endpoint names
# Pay-per-token Foundation Model APIs (available out of the box):
#   databricks-claude-*, databricks-meta-llama-*, databricks-dbrx-*
# External Model endpoints (customer-configured):
#   databricks-openai-gpt-*, or custom names
# The app also auto-discovers all READY serving endpoints at runtime.
DEFAULT_DATABRICKS_LLM_MODELS = [
    # -- Claude (Anthropic) --
    {"name": "databricks-claude-sonnet-4-6", "description": "Claude Sonnet 4.6 - Advanced reasoning (Primary)"},
    {"name": "databricks-claude-opus-4-6", "description": "Claude Opus 4.6 - Highest quality reasoning"},
    {"name": "databricks-claude-sonnet-4-5", "description": "Claude Sonnet 4.5 - Strong reasoning"},
    # -- OpenAI --
    {"name": "databricks-openai-gpt-4o", "description": "GPT-4o - Fast, high quality"},
    {"name": "databricks-openai-gpt-4o-mini", "description": "GPT-4o Mini - Fast, cost effective"},
    {"name": "databricks-openai-o3", "description": "OpenAI o3 - Advanced reasoning"},
    {"name": "databricks-openai-o3-mini", "description": "OpenAI o3 Mini - Fast reasoning"},
    # -- Meta Llama --
    {"name": "databricks-meta-llama-3-3-70b-instruct", "description": "Llama 3.3 70B - Excellent open model (Secondary)"},
    {"name": "databricks-meta-llama-3-1-405b-instruct", "description": "Llama 3.1 405B - Largest open model (Consolidation)"},
    {"name": "databricks-meta-llama-3-1-8b-instruct", "description": "Llama 3.1 8B - Fast, good for simple tasks"},
]


DEFAULT_CLAUDE_LLM_MODELS = [
    {"name": "claude-sonnet-4-6",        "description": "Claude Sonnet 4.6 — fast, balanced (Primary)"},
    {"name": "claude-opus-4-6",           "description": "Claude Opus 4.6 — highest quality (Secondary)"},
    {"name": "claude-haiku-4-5-20251001", "description": "Claude Haiku 4.5 — fastest, lowest cost (Consolidation)"},
]


def _get_platform_default_models() -> list:
    """Return the default model list based on available platform."""
    if RUNNING_IN_DATABRICKS or (DATABRICKS_AVAILABLE and not SNOWFLAKE_AVAILABLE and not CLAUDE_AVAILABLE):
        return DEFAULT_DATABRICKS_LLM_MODELS
    if CLAUDE_AVAILABLE and not SNOWFLAKE_AVAILABLE and not RUNNING_IN_DATABRICKS:
        return DEFAULT_CLAUDE_LLM_MODELS
    return DEFAULT_LLM_MODELS


def get_default_model(role: str = 'primary') -> str:
    """
    Return the platform-appropriate default model name for a given role.

    Args:
        role: 'primary', 'secondary', or 'consolidation'
    """
    models = _get_platform_default_models()
    role_index = {'primary': 0, 'secondary': 1, 'consolidation': 2}.get(role, 0)
    if role_index < len(models):
        return models[role_index]['name']
    return models[0]['name']


def get_llm_models(config_path: str = None) -> list:
    """
    Load LLM models from JSON config file, or return platform defaults.
    Supports: ["model1", ...] or [{"name": "model1", "description": "..."}, ...]
    """
    search_paths = [
        config_path,
        "llm_models.json",
        "config/llm_models.json",
        os.path.join(os.path.dirname(__file__), "llm_models.json"),
    ]

    for path in search_paths:
        if path and os.path.exists(path):
            try:
                with open(path, "r") as f:
                    loaded = json.load(f)
                    if loaded and isinstance(loaded, list):
                        if isinstance(loaded[0], str):
                            return [{"name": m, "description": m} for m in loaded]
                        elif isinstance(loaded[0], dict):
                            return loaded
            except Exception:
                pass

    return _get_platform_default_models()


# =============================================================================
# DATATYPE MAPPINGS (Informatica → Snowflake)
# =============================================================================
INFORMATICA_TO_SNOWFLAKE_TYPES = {
    'string': 'VARCHAR', 'varchar': 'VARCHAR', 'varchar2': 'VARCHAR',
    'char': 'CHAR', 'nstring': 'VARCHAR', 'nvarchar': 'VARCHAR',
    'nvarchar2': 'VARCHAR', 'text': 'TEXT',
    'integer': 'INTEGER', 'int': 'INTEGER', 'smallint': 'SMALLINT',
    'bigint': 'BIGINT', 'decimal': 'DECIMAL', 'number': 'NUMBER',
    'numeric': 'NUMERIC', 'float': 'FLOAT', 'double': 'DOUBLE', 'real': 'REAL',
    'date': 'DATE', 'datetime': 'TIMESTAMP_NTZ', 'timestamp': 'TIMESTAMP_NTZ',
    'time': 'TIME', 'binary': 'BINARY', 'varbinary': 'VARBINARY',
    'blob': 'BINARY', 'clob': 'VARCHAR(16777216)', 'nclob': 'VARCHAR(16777216)',
    'boolean': 'BOOLEAN', 'bit': 'BOOLEAN',
}

# =============================================================================
# EXPRESSION CONVERSION (Informatica → Snowflake SQL)
# =============================================================================
INFORMATICA_TO_SNOWFLAKE_FUNCTIONS = {
    'IIF': 'IFF', 'DECODE': 'DECODE',
    'LTRIM': 'LTRIM', 'RTRIM': 'RTRIM', 'SUBSTR': 'SUBSTR',
    'UPPER': 'UPPER', 'LOWER': 'LOWER', 'LENGTH': 'LENGTH',
    'LPAD': 'LPAD', 'RPAD': 'RPAD', 'REPLACE': 'REPLACE',
    'REPLACESTR': 'REPLACE', 'REPLACECHR': 'TRANSLATE',
    'INITCAP': 'INITCAP', 'REG_EXTRACT': 'REGEXP_SUBSTR',
    'REG_REPLACE': 'REGEXP_REPLACE', 'REG_MATCH': 'REGEXP_LIKE',
    'ISNULL': 'IS NULL', 'NVL': 'NVL', 'NVL2': 'NVL2',
    'SYSDATE': 'CURRENT_TIMESTAMP()', 'SYSTIMESTAMP': 'CURRENT_TIMESTAMP()',
    'TO_DATE': 'TO_DATE', 'TO_CHAR': 'TO_CHAR',
    'ADD_TO_DATE': 'DATEADD', 'DATE_DIFF': 'DATEDIFF',
    'TRUNC': 'DATE_TRUNC', 'GET_DATE_PART': 'DATE_PART', 'LAST_DAY': 'LAST_DAY',
    'ROUND': 'ROUND', 'ABS': 'ABS', 'CEIL': 'CEIL', 'FLOOR': 'FLOOR',
    'MOD': 'MOD', 'POWER': 'POWER', 'SQRT': 'SQRT',
    'TO_DECIMAL': 'TO_DECIMAL', 'TO_INTEGER': 'TO_NUMBER', 'TO_FLOAT': 'TO_DOUBLE',
    'SUM': 'SUM', 'AVG': 'AVG', 'MIN': 'MIN', 'MAX': 'MAX', 'COUNT': 'COUNT',
    'TO_BIGINT': 'TO_NUMBER',
    'LOOKUP': ':LKP', 'ERROR': 'NULL', 'ABORT': 'NULL',
}

DATE_FORMAT_MAPPINGS = {
    'YYYY': 'YYYY', 'YY': 'YY', 'MM': 'MM', 'MON': 'MON',
    'MONTH': 'MMMM', 'DD': 'DD', 'DY': 'DY', 'DAY': 'DAY',
    'HH': 'HH', 'HH12': 'HH12', 'HH24': 'HH24',
    'MI': 'MI', 'SS': 'SS', 'MS': 'FF3', 'US': 'FF6', 'NS': 'FF9',
}

# =============================================================================
# SQL TEMPLATES
# =============================================================================
SQL_TEMPLATES = {
    'create_table': '''CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
{columns}
){table_options};''',

    'insert_simple': '''INSERT INTO {target_schema}.{target_table} (
{target_columns}
)
SELECT
{source_expressions}
FROM {source_schema}.{source_table}{joins}{where_clause};''',

    'merge': '''MERGE INTO {target_schema}.{target_table} AS tgt
USING (
    SELECT
{source_expressions}
    FROM {source_schema}.{source_table}{joins}{where_clause}
) AS src
ON {merge_keys}
WHEN MATCHED THEN UPDATE SET
{update_columns}
WHEN NOT MATCHED THEN INSERT (
{insert_columns}
) VALUES (
{insert_values}
);''',

    'stored_procedure': '''CREATE OR REPLACE PROCEDURE {schema}.{procedure_name}()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_row_count INTEGER;
    v_error_msg VARCHAR;
BEGIN
    -- Log start
    INSERT INTO {schema}.ETL_LOG (PROCEDURE_NAME, STATUS, START_TIME)
    VALUES ('{procedure_name}', 'RUNNING', CURRENT_TIMESTAMP());

    -- Main ETL logic
{etl_logic}

    -- Get row count
    v_row_count := SQLROWCOUNT;

    -- Log success
    UPDATE {schema}.ETL_LOG
    SET STATUS = 'SUCCESS',
        END_TIME = CURRENT_TIMESTAMP(),
        ROWS_PROCESSED = v_row_count
    WHERE PROCEDURE_NAME = '{procedure_name}'
      AND STATUS = 'RUNNING';

    RETURN 'Success: ' || v_row_count || ' rows processed';

EXCEPTION
    WHEN OTHER THEN
        v_error_msg := SQLERRM;
        UPDATE {schema}.ETL_LOG
        SET STATUS = 'FAILED',
            END_TIME = CURRENT_TIMESTAMP(),
            ERROR_MESSAGE = v_error_msg
        WHERE PROCEDURE_NAME = '{procedure_name}'
          AND STATUS = 'RUNNING';
        RAISE;
END;
$$;''',
}

# =============================================================================
# DATABRICKS DATATYPE MAPPINGS (Informatica → Databricks Spark SQL)
# =============================================================================
INFORMATICA_TO_DATABRICKS_TYPES = {
    'string': 'STRING', 'varchar': 'STRING', 'varchar2': 'STRING',
    'char': 'STRING', 'nstring': 'STRING', 'nvarchar': 'STRING',
    'nvarchar2': 'STRING', 'text': 'STRING',
    'integer': 'INT', 'int': 'INT', 'smallint': 'SMALLINT',
    'bigint': 'BIGINT', 'decimal': 'DECIMAL', 'number': 'DECIMAL',
    'numeric': 'DECIMAL', 'float': 'FLOAT', 'double': 'DOUBLE', 'real': 'FLOAT',
    'date': 'DATE', 'datetime': 'TIMESTAMP', 'timestamp': 'TIMESTAMP',
    'time': 'STRING', 'binary': 'BINARY', 'varbinary': 'BINARY',
    'blob': 'BINARY', 'clob': 'STRING', 'nclob': 'STRING',
    'boolean': 'BOOLEAN', 'bit': 'BOOLEAN',
}

# =============================================================================
# DATABRICKS EXPRESSION CONVERSION (Informatica → Databricks Spark SQL)
# =============================================================================
INFORMATICA_TO_DATABRICKS_FUNCTIONS = {
    'IIF': 'IF', 'DECODE': 'DECODE',
    'LTRIM': 'LTRIM', 'RTRIM': 'RTRIM', 'SUBSTR': 'SUBSTR',
    'UPPER': 'UPPER', 'LOWER': 'LOWER', 'LENGTH': 'LENGTH',
    'LPAD': 'LPAD', 'RPAD': 'RPAD', 'REPLACE': 'REPLACE',
    'REPLACESTR': 'REPLACE', 'REPLACECHR': 'TRANSLATE',
    'INITCAP': 'INITCAP', 'REG_EXTRACT': 'REGEXP_EXTRACT',
    'REG_REPLACE': 'REGEXP_REPLACE', 'REG_MATCH': 'RLIKE',
    'ISNULL': 'IS NULL', 'NVL': 'COALESCE', 'NVL2': 'NVL2',
    'SYSDATE': 'CURRENT_TIMESTAMP()', 'SYSTIMESTAMP': 'CURRENT_TIMESTAMP()',
    'TO_DATE': 'TO_DATE', 'TO_CHAR': 'DATE_FORMAT',
    'ADD_TO_DATE': 'DATE_ADD', 'DATE_DIFF': 'DATEDIFF',
    'TRUNC': 'DATE_TRUNC', 'GET_DATE_PART': 'EXTRACT', 'LAST_DAY': 'LAST_DAY',
    'ROUND': 'ROUND', 'ABS': 'ABS', 'CEIL': 'CEIL', 'FLOOR': 'FLOOR',
    'MOD': 'MOD', 'POWER': 'POWER', 'SQRT': 'SQRT',
    'TO_DECIMAL': 'CAST_DECIMAL', 'TO_INTEGER': 'CAST_INT', 'TO_FLOAT': 'CAST_DOUBLE',
    'SUM': 'SUM', 'AVG': 'AVG', 'MIN': 'MIN', 'MAX': 'MAX', 'COUNT': 'COUNT',
    'TO_BIGINT': 'CAST_BIGINT',
    'LOOKUP': ':LKP', 'ERROR': 'NULL', 'ABORT': 'NULL',
}

# =============================================================================
# DATABRICKS SQL TEMPLATES
# =============================================================================
DATABRICKS_SQL_TEMPLATES = {
    'create_table': '''CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
{columns}
)
USING DELTA{table_options};''',

    'insert_simple': '''INSERT INTO {target_schema}.{target_table} (
{target_columns}
)
SELECT
{source_expressions}
FROM {source_schema}.{source_table}{joins}{where_clause};''',

    'merge': '''MERGE INTO {target_schema}.{target_table} AS tgt
USING (
    SELECT
{source_expressions}
    FROM {source_schema}.{source_table}{joins}{where_clause}
) AS src
ON {merge_keys}
WHEN MATCHED THEN UPDATE SET
{update_columns}
WHEN NOT MATCHED THEN INSERT (
{insert_columns}
) VALUES (
{insert_values}
);''',

    'notebook': '''# Databricks notebook source
# MAGIC %md
# MAGIC # ETL: {procedure_name}
# MAGIC Load data into **{target_table}**

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ETL Logic

# COMMAND ----------

def run_etl():
    """Execute the ETL load for {target_table}."""
    start_time = datetime.now()

    try:
        # Log start
        spark.sql("""
            INSERT INTO {schema}.ETL_LOG (PROCEDURE_NAME, STATUS, START_TIME)
            VALUES ('{procedure_name}', 'RUNNING', current_timestamp())
        """)

        # Main ETL logic
        spark.sql("""
{etl_logic}
        """)

        # Log success
        spark.sql("""
            UPDATE {schema}.ETL_LOG
            SET STATUS = 'SUCCESS',
                END_TIME = current_timestamp()
            WHERE PROCEDURE_NAME = '{procedure_name}'
              AND STATUS = 'RUNNING'
        """)

        print(f"ETL {procedure_name} completed successfully")

    except Exception as e:
        # Log failure
        spark.sql(f"""
            UPDATE {schema}.ETL_LOG
            SET STATUS = 'FAILED',
                END_TIME = current_timestamp(),
                ERROR_MESSAGE = '{{str(e)[:500]}}'
            WHERE PROCEDURE_NAME = '{procedure_name}'
              AND STATUS = 'RUNNING'
        """)
        raise

run_etl()
''',
}


# =============================================================================
# PLATFORM-AWARE HELPERS
# =============================================================================

def get_type_mappings(target_platform: str = 'snowflake') -> dict:
    """Return the Informatica-to-target type mapping dict."""
    if target_platform == 'databricks':
        return INFORMATICA_TO_DATABRICKS_TYPES
    return INFORMATICA_TO_SNOWFLAKE_TYPES


def get_function_mappings(target_platform: str = 'snowflake') -> dict:
    """Return the Informatica-to-target function mapping dict."""
    if target_platform == 'databricks':
        return INFORMATICA_TO_DATABRICKS_FUNCTIONS
    return INFORMATICA_TO_SNOWFLAKE_FUNCTIONS


def get_sql_templates(target_platform: str = 'snowflake') -> dict:
    """Return the SQL templates for the target platform."""
    if target_platform == 'databricks':
        return DATABRICKS_SQL_TEMPLATES
    return SQL_TEMPLATES
