"""
Application settings and configuration for Technical Document Generator (TDG)
Supports: PostgreSQL, Microsoft SQL (T-SQL), Databricks SQL, Databricks Python Notebooks.
LLM providers: Anthropic Claude, Databricks Foundation Models.
"""

import os
import json

# =============================================================================
# APPLICATION SETTINGS
# =============================================================================
APP_TITLE = "Technical Document Generator"
APP_SHORT_NAME = "TDG"
APP_ICON = "📄"
PAGE_LAYOUT = "wide"
APP_VERSION = "3.0.0"

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
# PLATFORM DETECTION — LLM SDK availability flags
# =============================================================================

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

# -- Unified flag: True if ANY LLM platform is available --------------------
LLM_PLATFORM_AVAILABLE = DATABRICKS_AVAILABLE or CLAUDE_AVAILABLE

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
DEFAULT_DATABRICKS_LLM_MODELS = [
    {"name": "databricks-claude-sonnet-4-6",          "description": "Claude Sonnet 4.6 — advanced reasoning (Primary)"},
    {"name": "databricks-claude-opus-4-6",             "description": "Claude Opus 4.6 — highest quality"},
    {"name": "databricks-claude-sonnet-4-5",           "description": "Claude Sonnet 4.5 — strong reasoning"},
    {"name": "databricks-openai-gpt-4o",               "description": "GPT-4o — fast, high quality"},
    {"name": "databricks-openai-gpt-4o-mini",          "description": "GPT-4o Mini — fast, cost effective"},
    {"name": "databricks-meta-llama-3-3-70b-instruct", "description": "Llama 3.3 70B — excellent open model (Secondary)"},
    {"name": "databricks-meta-llama-3-1-405b-instruct","description": "Llama 3.1 405B — largest open model (Consolidation)"},
]

DEFAULT_CLAUDE_LLM_MODELS = [
    {"name": "claude-sonnet-4-6",        "description": "Claude Sonnet 4.6 — fast, balanced (Primary)"},
    {"name": "claude-opus-4-6",           "description": "Claude Opus 4.6 — highest quality (Secondary)"},
    {"name": "claude-haiku-4-5-20251001", "description": "Claude Haiku 4.5 — fastest, lowest cost (Consolidation)"},
]


def _get_platform_default_models() -> list:
    """Return the default model list based on available LLM platform."""
    if RUNNING_IN_DATABRICKS or (DATABRICKS_AVAILABLE and not CLAUDE_AVAILABLE):
        return DEFAULT_DATABRICKS_LLM_MODELS
    if CLAUDE_AVAILABLE:
        return DEFAULT_CLAUDE_LLM_MODELS
    return DEFAULT_CLAUDE_LLM_MODELS  # fallback


def get_default_model(role: str = 'primary') -> str:
    """Return the platform-appropriate default model name for a given role."""
    models = _get_platform_default_models()
    role_index = {'primary': 0, 'secondary': 1, 'consolidation': 2}.get(role, 0)
    if role_index < len(models):
        return models[role_index]['name']
    return models[0]['name']


def get_llm_models(config_path: str = None) -> list:
    """Load LLM models from JSON config file, or return platform defaults."""
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
# DATATYPE MAPPINGS (Informatica → Target Platform)
# =============================================================================

INFORMATICA_TO_POSTGRESQL_TYPES = {
    'string': 'VARCHAR', 'varchar': 'VARCHAR', 'varchar2': 'VARCHAR',
    'char': 'CHAR', 'nstring': 'VARCHAR', 'nvarchar': 'VARCHAR',
    'nvarchar2': 'VARCHAR', 'text': 'TEXT',
    'integer': 'INTEGER', 'int': 'INTEGER', 'smallint': 'SMALLINT',
    'bigint': 'BIGINT', 'decimal': 'NUMERIC', 'number': 'NUMERIC',
    'numeric': 'NUMERIC', 'float': 'DOUBLE PRECISION', 'double': 'DOUBLE PRECISION', 'real': 'REAL',
    'date': 'DATE', 'datetime': 'TIMESTAMP', 'timestamp': 'TIMESTAMP',
    'time': 'TIME', 'binary': 'BYTEA', 'varbinary': 'BYTEA',
    'blob': 'BYTEA', 'clob': 'TEXT', 'nclob': 'TEXT',
    'boolean': 'BOOLEAN', 'bit': 'BOOLEAN',
}

INFORMATICA_TO_MSSQL_TYPES = {
    'string': 'VARCHAR', 'varchar': 'VARCHAR', 'varchar2': 'VARCHAR(MAX)',
    'char': 'CHAR', 'nstring': 'NVARCHAR', 'nvarchar': 'NVARCHAR',
    'nvarchar2': 'NVARCHAR(MAX)', 'text': 'NVARCHAR(MAX)',
    'integer': 'INT', 'int': 'INT', 'smallint': 'SMALLINT',
    'bigint': 'BIGINT', 'decimal': 'DECIMAL', 'number': 'DECIMAL',
    'numeric': 'NUMERIC', 'float': 'FLOAT', 'double': 'FLOAT', 'real': 'REAL',
    'date': 'DATE', 'datetime': 'DATETIME2', 'timestamp': 'DATETIME2',
    'time': 'TIME', 'binary': 'VARBINARY', 'varbinary': 'VARBINARY(MAX)',
    'blob': 'VARBINARY(MAX)', 'clob': 'NVARCHAR(MAX)', 'nclob': 'NVARCHAR(MAX)',
    'boolean': 'BIT', 'bit': 'BIT',
}

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
# EXPRESSION / FUNCTION MAPPINGS (Informatica → Target Platform)
# =============================================================================

INFORMATICA_TO_POSTGRESQL_FUNCTIONS = {
    'IIF': 'CASE WHEN', 'DECODE': 'CASE WHEN',
    'LTRIM': 'LTRIM', 'RTRIM': 'RTRIM', 'SUBSTR': 'SUBSTRING',
    'UPPER': 'UPPER', 'LOWER': 'LOWER', 'LENGTH': 'LENGTH',
    'LPAD': 'LPAD', 'RPAD': 'RPAD', 'REPLACE': 'REPLACE',
    'REPLACESTR': 'REPLACE', 'REPLACECHR': 'TRANSLATE',
    'INITCAP': 'INITCAP', 'REG_EXTRACT': 'REGEXP_MATCH',
    'REG_REPLACE': 'REGEXP_REPLACE', 'REG_MATCH': 'REGEXP_MATCH',
    'ISNULL': 'IS NULL', 'NVL': 'COALESCE', 'NVL2': 'COALESCE',
    'SYSDATE': 'CURRENT_TIMESTAMP', 'SYSTIMESTAMP': 'CURRENT_TIMESTAMP',
    'TO_DATE': 'TO_DATE', 'TO_CHAR': 'TO_CHAR',
    'ADD_TO_DATE': 'INTERVAL', 'DATE_DIFF': 'DATE_PART',
    'TRUNC': 'DATE_TRUNC', 'GET_DATE_PART': 'EXTRACT', 'LAST_DAY': 'DATE_TRUNC',
    'ROUND': 'ROUND', 'ABS': 'ABS', 'CEIL': 'CEIL', 'FLOOR': 'FLOOR',
    'MOD': 'MOD', 'POWER': 'POWER', 'SQRT': 'SQRT',
    'TO_DECIMAL': 'CAST', 'TO_INTEGER': 'CAST', 'TO_FLOAT': 'CAST',
    'SUM': 'SUM', 'AVG': 'AVG', 'MIN': 'MIN', 'MAX': 'MAX', 'COUNT': 'COUNT',
    'TO_BIGINT': 'CAST',
    'LOOKUP': ':LKP', 'ERROR': 'NULL', 'ABORT': 'NULL',
}

INFORMATICA_TO_MSSQL_FUNCTIONS = {
    'IIF': 'IIF', 'DECODE': 'CASE WHEN',
    'LTRIM': 'LTRIM', 'RTRIM': 'RTRIM', 'SUBSTR': 'SUBSTRING',
    'UPPER': 'UPPER', 'LOWER': 'LOWER', 'LENGTH': 'LEN',
    'LPAD': 'FORMAT', 'RPAD': 'FORMAT', 'REPLACE': 'REPLACE',
    'REPLACESTR': 'REPLACE', 'REPLACECHR': 'TRANSLATE',
    'INITCAP': 'UPPER',  # T-SQL has no INITCAP
    'REG_EXTRACT': 'PATINDEX', 'REG_REPLACE': 'REPLACE', 'REG_MATCH': 'PATINDEX',
    'ISNULL': 'ISNULL', 'NVL': 'ISNULL', 'NVL2': 'IIF',
    'SYSDATE': 'GETDATE()', 'SYSTIMESTAMP': 'SYSDATETIME()',
    'TO_DATE': 'CONVERT', 'TO_CHAR': 'FORMAT',
    'ADD_TO_DATE': 'DATEADD', 'DATE_DIFF': 'DATEDIFF',
    'TRUNC': 'DATETRUNC', 'GET_DATE_PART': 'DATEPART', 'LAST_DAY': 'EOMONTH',
    'ROUND': 'ROUND', 'ABS': 'ABS', 'CEIL': 'CEILING', 'FLOOR': 'FLOOR',
    'MOD': 'CAST(x AS INT) % y', 'POWER': 'POWER', 'SQRT': 'SQRT',
    'TO_DECIMAL': 'CAST', 'TO_INTEGER': 'CAST', 'TO_FLOAT': 'CAST',
    'SUM': 'SUM', 'AVG': 'AVG', 'MIN': 'MIN', 'MAX': 'MAX', 'COUNT': 'COUNT',
    'TO_BIGINT': 'CAST',
    'LOOKUP': ':LKP', 'ERROR': 'NULL', 'ABORT': 'NULL',
}

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
    'TO_DECIMAL': 'CAST', 'TO_INTEGER': 'CAST', 'TO_FLOAT': 'CAST',
    'SUM': 'SUM', 'AVG': 'AVG', 'MIN': 'MIN', 'MAX': 'MAX', 'COUNT': 'COUNT',
    'TO_BIGINT': 'CAST',
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

POSTGRESQL_SQL_TEMPLATES = {
    'create_table': '''CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
{columns}
){table_options};''',

    'insert_simple': '''INSERT INTO {target_schema}.{target_table} (
{target_columns}
)
SELECT
{source_expressions}
FROM {source_schema}.{source_table}{joins}{where_clause};''',

    'merge': '''INSERT INTO {target_schema}.{target_table} (
{insert_columns}
)
SELECT
{source_expressions}
FROM {source_schema}.{source_table}{joins}{where_clause}
ON CONFLICT ({merge_key_cols}) DO UPDATE SET
{update_columns};''',

    'stored_procedure': '''CREATE OR REPLACE PROCEDURE {schema}.{procedure_name}()
LANGUAGE plpgsql
AS $$
DECLARE
    v_row_count INTEGER;
BEGIN
    -- Log start
    INSERT INTO {schema}.etl_log (procedure_name, status, start_time)
    VALUES ('{procedure_name}', 'RUNNING', NOW());

    -- Main ETL logic
{etl_logic}

    GET DIAGNOSTICS v_row_count = ROW_COUNT;

    -- Log success
    UPDATE {schema}.etl_log
    SET status = 'SUCCESS',
        end_time = NOW(),
        rows_processed = v_row_count
    WHERE procedure_name = '{procedure_name}'
      AND status = 'RUNNING';

EXCEPTION WHEN OTHERS THEN
    UPDATE {schema}.etl_log
    SET status = 'FAILED',
        end_time = NOW(),
        error_message = SQLERRM
    WHERE procedure_name = '{procedure_name}'
      AND status = 'RUNNING';
    RAISE;
END;
$$;''',
}

MSSQL_SQL_TEMPLATES = {
    'create_table': '''IF NOT EXISTS (
    SELECT * FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'
)
CREATE TABLE [{schema}].[{table_name}] (
{columns}
){table_options};''',

    'insert_simple': '''INSERT INTO [{target_schema}].[{target_table}] (
{target_columns}
)
SELECT
{source_expressions}
FROM [{source_schema}].[{source_table}]{joins}{where_clause};''',

    'merge': '''MERGE [{target_schema}].[{target_table}] AS tgt
USING (
    SELECT
{source_expressions}
    FROM [{source_schema}].[{source_table}]{joins}{where_clause}
) AS src
ON {merge_keys}
WHEN MATCHED THEN UPDATE SET
{update_columns}
WHEN NOT MATCHED THEN INSERT (
{insert_columns}
) VALUES (
{insert_values}
);''',

    'stored_procedure': '''CREATE OR ALTER PROCEDURE [{schema}].[{procedure_name}]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @v_row_count INT;
    DECLARE @v_error_msg NVARCHAR(4000);

    BEGIN TRY
        -- Log start
        INSERT INTO [{schema}].[ETL_LOG] (PROCEDURE_NAME, STATUS, START_TIME)
        VALUES ('{procedure_name}', 'RUNNING', GETDATE());

        -- Main ETL logic
{etl_logic}

        SET @v_row_count = @@ROWCOUNT;

        -- Log success
        UPDATE [{schema}].[ETL_LOG]
        SET STATUS = 'SUCCESS',
            END_TIME = GETDATE(),
            ROWS_PROCESSED = @v_row_count
        WHERE PROCEDURE_NAME = '{procedure_name}'
          AND STATUS = 'RUNNING';

    END TRY
    BEGIN CATCH
        SET @v_error_msg = ERROR_MESSAGE();
        UPDATE [{schema}].[ETL_LOG]
        SET STATUS = 'FAILED',
            END_TIME = GETDATE(),
            ERROR_MESSAGE = @v_error_msg
        WHERE PROCEDURE_NAME = '{procedure_name}'
          AND STATUS = 'RUNNING';
        THROW;
    END CATCH;
END;''',
}

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

    'stored_procedure': '''-- Databricks SQL — no native stored procedures; use notebook or workflow task.
-- ETL logic for {procedure_name}:

{etl_logic}''',

    'notebook': '''# Databricks notebook source
# MAGIC %md
# MAGIC # ETL: {procedure_name}
# MAGIC Load data into **{target_table}**

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

def run_etl():
    """Execute the ETL load for {target_table}."""
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
            SET STATUS = 'SUCCESS', END_TIME = current_timestamp()
            WHERE PROCEDURE_NAME = '{procedure_name}' AND STATUS = 'RUNNING'
        """)

        print(f"ETL {procedure_name} completed successfully")

    except Exception as e:
        spark.sql(f"""
            UPDATE {schema}.ETL_LOG
            SET STATUS = 'FAILED',
                END_TIME = current_timestamp(),
                ERROR_MESSAGE = '{{str(e)[:500]}}'
            WHERE PROCEDURE_NAME = '{procedure_name}' AND STATUS = 'RUNNING'
        """)
        raise

run_etl()
''',
}

# =============================================================================
# PLATFORM-AWARE HELPERS
# =============================================================================

def get_type_mappings(target_platform: str = 'postgresql') -> dict:
    """Return the Informatica-to-target type mapping dict."""
    if target_platform == 'mssql':
        return INFORMATICA_TO_MSSQL_TYPES
    if target_platform in ('databricks_sql', 'databricks_python'):
        return INFORMATICA_TO_DATABRICKS_TYPES
    return INFORMATICA_TO_POSTGRESQL_TYPES  # default: postgresql


def get_function_mappings(target_platform: str = 'postgresql') -> dict:
    """Return the Informatica-to-target function mapping dict."""
    if target_platform == 'mssql':
        return INFORMATICA_TO_MSSQL_FUNCTIONS
    if target_platform in ('databricks_sql', 'databricks_python'):
        return INFORMATICA_TO_DATABRICKS_FUNCTIONS
    return INFORMATICA_TO_POSTGRESQL_FUNCTIONS  # default: postgresql


def get_sql_templates(target_platform: str = 'postgresql') -> dict:
    """Return the SQL templates for the target platform."""
    if target_platform == 'mssql':
        return MSSQL_SQL_TEMPLATES
    if target_platform in ('databricks_sql', 'databricks_python'):
        return DATABRICKS_SQL_TEMPLATES
    return POSTGRESQL_SQL_TEMPLATES  # default: postgresql
