"""
dbt Model Generator Module for T - TDD Generator

Generates dbt bronze (staging) and silver (transform) layer models
from Informatica XML lineage data. Target platform: Snowflake.
"""

import io
import re
import zipfile
from typing import Callable, Dict, List, Optional, Set, Tuple
import pandas as pd

from generators.sql_generator import (
    convert_informatica_expression,
    clean_source_column,
    find_join_keys,
    map_datatype,
)
from utils.helpers import is_special_source


# String types in Informatica that need TRIM/NULLIF in bronze
_STRING_TYPES = frozenset({
    'string', 'varchar', 'varchar2', 'char', 'nstring',
    'nvarchar', 'nvarchar2', 'text', 'clob', 'nclob',
})


# ═══════════════════════════════════════════════════════════════════════════
# Tier 1: Pattern-based resolution for UNCONNECTED columns
# ═══════════════════════════════════════════════════════════════════════════

_UNCONNECTED_PATTERNS: List[Tuple[str, str]] = [
    # Audit / housekeeping timestamps
    (r'(?i)^(INSERT|CREATE|LOAD|INGESTION|ETL|DW_INSERT|DW_CREATE|DW_LOAD)[_]?(TS|TIMESTAMP|DT|DATE|DTTM|TIME)$',
     'CURRENT_TIMESTAMP()'),
    (r'(?i)^(UPDATE|MODIFY|CHANGE|LAST_?UPDATE|DW_UPDATE|DW_MODIFY)[_]?(TS|TIMESTAMP|DT|DATE|DTTM|TIME)$',
     'CURRENT_TIMESTAMP()'),
    # Audit user columns
    (r'(?i)^(INSERT|CREATE|LOAD|DW_INSERT)[_]?(BY|USER|USR|USER_ID)$', "CURRENT_USER()"),
    (r'(?i)^(UPDATE|MODIFY|DW_UPDATE)[_]?(BY|USER|USR|USER_ID)$', "CURRENT_USER()"),
    # SCD2 effective dates
    (r'(?i)^(EFF|EFFECTIVE)[_]?(START|BEGIN)[_]?(DT|DATE|DTTM|TS)?$', 'CURRENT_DATE()'),
    (r'(?i)^(EFF|EFFECTIVE)[_]?(END|EXPIRE|EXPIRY)[_]?(DT|DATE|DTTM|TS)?$', "'9999-12-31'::DATE"),
    (r'(?i)^(VALID|REC|RECORD)[_]?(FROM|START)[_]?(DT|DATE|DTTM|TS)?$', 'CURRENT_DATE()'),
    (r'(?i)^(VALID|REC|RECORD)[_]?(TO|END|THRU)[_]?(DT|DATE|DTTM|TS)?$', "'9999-12-31'::DATE"),
    # SCD2 / active flags
    (r'(?i)^(IS[_]?CURRENT|CURRENT[_]?FLAG|IS[_]?CURRENT[_]?FLAG|CURRENT[_]?IND)$', 'TRUE'),
    (r'(?i)^(IS[_]?ACTIVE|ACTIVE[_]?FLAG|ACTIVE[_]?IND)$', 'TRUE'),
    (r'(?i)^(IS[_]?DELETED|DELETE[D]?[_]?FLAG|SOFT[_]?DELETE[_]?FLAG|DELETE[D]?[_]?IND)$', 'FALSE'),
    # Row version / counter
    (r'(?i)^(RECORD|ROW|REC)[_]?(VERSION|VER|SEQ|NUM)$', '1'),
    # ETL batch / run identifiers
    (r'(?i)^(ETL|BATCH|JOB)[_]?(ID|RUN[_]?ID|BATCH[_]?ID|NBR)$', '-1'),
    # Source system identifiers
    (r'(?i)^(SOURCE|SRC)[_]?(SYSTEM|SYS|SYSTEM[_]?NAME|SYS[_]?NAME|CD)$', "'INFORMATICA'"),
]


def _resolve_unconnected_tier1(target_col: str) -> Optional[str]:
    """
    Tier 1: Resolve an UNCONNECTED column via pattern matching.

    Returns a SQL expression string if the column matches a known pattern,
    or None if no pattern matches.
    """
    for pattern, sql_expr in _UNCONNECTED_PATTERNS:
        if re.match(pattern, target_col):
            return sql_expr
    return None


def _safe_str(value) -> str:
    """Coerce a value to string, treating NaN/None as empty string."""
    if value is None:
        return ''
    if isinstance(value, float) and pd.isna(value):
        return ''
    return str(value)


def _is_string_type(datatype: str) -> bool:
    """Check if an Informatica datatype is a string type."""
    if not datatype:
        return False
    base = re.sub(r'\([^)]*\)', '', datatype).strip().lower()
    return base in _STRING_TYPES


def _safe_yaml_value(value: str) -> str:
    """Wrap a YAML string value in quotes if it contains special characters."""
    if not value:
        return '""'
    if any(c in value for c in ':{}[]|>*&!%#`@,?\\\'"'):
        escaped = value.replace('"', '\\"')
        return f'"{escaped}"'
    if value.lower() in ('true', 'false', 'null', 'yes', 'no', 'on', 'off'):
        return f'"{value}"'
    return value


def _model_name(prefix: str, table_name: str) -> str:
    """Build a dbt model filename from a prefix and table name."""
    clean = re.sub(r'[^a-zA-Z0-9_]', '_', table_name).strip('_').lower()
    clean = re.sub(r'_+', '_', clean)
    return f"{prefix}_{clean}"


def _build_lookup_on_clause(condition: str, main_alias: str, lkp_alias: str) -> str:
    """
    Convert a Lookup_Condition string into a proper SQL ON clause.

    Handles patterns like:
    - "COL1 = IN_COL1 AND COL2 = IN_COL2"
    - "TABLE.COL = value"
    - Multi-part AND conditions
    """
    if not condition:
        return f"{main_alias}.<key> = {lkp_alias}.<key>"

    # Replace IN_<col> references with main_alias.<col>
    clause = re.sub(r'\bIN_([A-Za-z][A-Za-z0-9_]*)\b', rf'{main_alias}.\1', condition)

    # Split by AND to handle each predicate individually
    predicates = re.split(r'\s+AND\s+', clause, flags=re.IGNORECASE)
    qualified = []
    for pred in predicates:
        parts = pred.split('=', 1)
        if len(parts) == 2:
            left = parts[0].strip()
            right = parts[1].strip()
            # Qualify left side with lkp_alias if it's a bare column
            if '.' not in left and not left.startswith("'"):
                left = f"{lkp_alias}.{left}"
            # Qualify right side with main_alias if it's a bare column
            if '.' not in right and not right.startswith("'") and not right.lstrip('-').replace('.', '', 1).isdigit():
                right = f"{main_alias}.{right}"
            qualified.append(f"{left} = {right}")
        else:
            qualified.append(pred.strip())

    return '\n        AND '.join(qualified)


def _find_shared_columns_from_lineage(lineage_df, table_a: str, table_b: str) -> List[str]:
    """Find column names that appear in lineage for both source tables."""
    cols_a = set()
    cols_b = set()
    for _, row in lineage_df.iterrows():
        src = _safe_str(row.get('Source_Table_INSERT', '')).replace('Shortcut_to_', '')
        col = clean_source_column(_safe_str(row.get('Source_Column_INSERT', '')))
        if not col:
            continue
        if src == table_a:
            cols_a.add(col)
        elif src == table_b:
            cols_b.add(col)
    shared = cols_a & cols_b
    # Prefer key-like columns
    key_suffixes = ('_ID', '_KEY', '_CD', '_CODE', '_NO')
    key_cols = [c for c in shared if any(c.upper().endswith(s) for s in key_suffixes)]
    return key_cols if key_cols else list(shared)[:3]


def _qualify_join_condition(condition: str, left_alias: str, right_alias: str) -> str:
    """Add table aliases to a raw join condition string."""
    # Split by AND
    predicates = re.split(r'\s+AND\s+', condition, flags=re.IGNORECASE)
    qualified = []
    for pred in predicates:
        parts = pred.split('=', 1)
        if len(parts) == 2:
            left = parts[0].strip()
            right = parts[1].strip()
            if '.' not in left:
                left = f"{left_alias}.{left}"
            if '.' not in right:
                right = f"{right_alias}.{right}"
            qualified.append(f"{left} = {right}")
        else:
            qualified.append(pred.strip())
    return ' AND '.join(qualified)


def _infer_lookup_join(lkp_table: str, lkp_info: Dict,
                       main_source: str, sources_dict: Dict,
                       targets_dict: Dict, lineage_df,
                       main_alias: str, lkp_alias: str) -> str:
    """
    Infer a JOIN condition for a lookup table when Lookup_Condition is empty.

    Strategy:
    1. Find column names shared between main source and lookup table
    2. Use _ID/_KEY/_CD suffix matching
    3. Fall back to TODO comment with context
    """
    # Get columns from the lookup table (from targets or sources dict)
    lkp_fields = set()
    for d in (targets_dict, sources_dict):
        if lkp_table in d:
            lkp_fields = set(d[lkp_table].get('fields', {}).keys())
            break

    # Get columns from main source
    main_fields = set()
    if main_source and main_source in sources_dict:
        main_fields = set(sources_dict[main_source].get('fields', {}).keys())

    if lkp_fields and main_fields:
        shared = lkp_fields & main_fields
        key_suffixes = ('_ID', '_KEY', '_CD', '_CODE', '_NO')
        key_cols = [c for c in shared if any(c.upper().endswith(s) for s in key_suffixes)]
        if key_cols:
            conds = [f"{main_alias}.{k} = {lkp_alias}.{k}" for k in sorted(key_cols)[:3]]
            return ' AND '.join(conds)
        if shared:
            conds = [f"{main_alias}.{k} = {lkp_alias}.{k}" for k in sorted(shared)[:2]]
            return ' AND '.join(conds)

    # Use target/source column names from the lineage rows for this lookup
    src_cols = lkp_info.get('source_cols', [])
    if src_cols:
        # The source column from the lookup likely matches a column in the lookup table
        col = src_cols[0]
        return f"{main_alias}.{col} = {lkp_alias}.{col}  -- inferred from lineage"

    return f"1=1  -- TODO: specify join keys for {lkp_table}"


class DbtGenerator:
    """Generate dbt models (bronze + silver) from Informatica lineage."""

    SPECIAL_SOURCES = frozenset({
        'Hardcoded', 'Derived', 'SYSTEM', 'UNCONNECTED',
        'SEQUENCE_GENERATOR', 'Lookup/Expression', '',
    })

    def __init__(
        self,
        sources_dict: Dict,
        targets_dict: Dict,
        lineage_df: pd.DataFrame,
        mappings_dict: Dict = None,
        raw_source_name: str = 'raw',
        bronze_materialization: str = 'view',
        silver_materialization: str = 'table',
        resolve_unconnected: bool = True,
        unconnected_resolver: Optional[Callable[[List[str]], Dict[str, str]]] = None,
        bronze_prefix: str = 'brz',
        silver_prefix: str = 'slv',
        bronze_layer: str = 'bronze',
        silver_layer: str = 'silver',
        target_schema: str = '',
        dbt_tags: Optional[List[str]] = None,
    ):
        self.sources = sources_dict or {}
        self.targets = targets_dict or {}
        self.lineage = lineage_df
        self.mappings = mappings_dict or {}
        self.raw_source_name = raw_source_name
        self.bronze_mat = bronze_materialization
        self.silver_mat = silver_materialization
        self.resolve_unconnected = resolve_unconnected
        self.unconnected_resolver = unconnected_resolver
        self.bronze_prefix = bronze_prefix
        self.silver_prefix = silver_prefix
        self.bronze_layer = bronze_layer
        self.silver_layer = silver_layer
        self.target_schema = target_schema
        self.dbt_tags = dbt_tags or []

    # ------------------------------------------------------------------
    # Discovery helpers
    # ------------------------------------------------------------------

    def get_source_tables(self) -> List[str]:
        """Return unique real source table names (including lookup tables)."""
        if self.lineage.empty:
            return []

        all_tables: Set[str] = set()
        for val in self.lineage['Source_Table_INSERT'].dropna().unique():
            clean = val.replace('Shortcut_to_', '') if val else ''
            if clean and clean not in self.SPECIAL_SOURCES:
                all_tables.add(clean)
        return sorted(all_tables)

    def get_target_tables(self) -> List[str]:
        """Return unique target table names."""
        if self.lineage.empty:
            return []
        return sorted(self.lineage['Target_Table'].dropna().unique())

    def _get_primary_keys(self, table_name: str, table_dict: Dict) -> List[str]:
        """Return PK column names for a table."""
        if table_name not in table_dict:
            return []
        fields = table_dict[table_name].get('fields', {})
        return [f for f, info in fields.items()
                if 'PRIMARY' in str(info.get('keytype', '')).upper()]

    def _get_silver_unique_keys(self, target_table: str, target_lineage) -> List[str]:
        """
        Determine unique key columns for a silver incremental model.

        Resolution order:
        1. Target_Key == 'PK' columns from lineage
        2. Primary keys from targets_dict metadata
        3. NK_* (natural key) target columns from lineage
        """
        # 1. PK columns from lineage Target_Key field
        pk_cols = []
        for _, row in target_lineage.iterrows():
            if _safe_str(row.get('Target_Key', '')).upper() == 'PK':
                pk_cols.append(row['Target_Column'])
        if pk_cols:
            return pk_cols

        # 2. Primary keys from targets_dict
        pks = self._get_primary_keys(target_table, self.targets)
        if pks:
            return pks

        # 3. NK_* columns (natural keys by naming convention)
        nk_cols = []
        for _, row in target_lineage.iterrows():
            tgt_col = _safe_str(row.get('Target_Column', ''))
            if tgt_col.upper().startswith('NK_') and 'DATE' not in tgt_col.upper():
                nk_cols.append(tgt_col)
        if nk_cols:
            return nk_cols

        return []

    def _get_fields(self, table_name: str) -> Dict:
        """Get fields dict for a table from sources or targets."""
        if table_name in self.sources:
            return self.sources[table_name].get('fields', {})
        if table_name in self.targets:
            return self.targets[table_name].get('fields', {})
        return {}

    # ------------------------------------------------------------------
    # Config block helper
    # ------------------------------------------------------------------

    def _build_config(self, materialization: str, unique_keys: List[str] = None) -> str:
        """Build a dbt config block string with optional schema/tags."""
        parts = [f"materialized='{materialization}'"]
        if unique_keys:
            if len(unique_keys) == 1:
                parts.append(f"unique_key='{unique_keys[0]}'")
            else:
                pk_str = "'" + "', '".join(unique_keys) + "'"
                parts.append(f"unique_key=[{pk_str}]")
        if self.target_schema:
            parts.append(f"schema='{self.target_schema}'")
        if self.dbt_tags:
            if len(self.dbt_tags) == 1:
                parts.append(f"tags='{self.dbt_tags[0]}'")
            else:
                tag_str = "'" + "', '".join(self.dbt_tags) + "'"
                parts.append(f"tags=[{tag_str}]")
        return "{{ config(" + ", ".join(parts) + ") }}"

    # ------------------------------------------------------------------
    # Bronze layer
    # ------------------------------------------------------------------

    def generate_bronze_model(self, source_table: str) -> str:
        """Generate a bronze model SQL file for a source table."""
        fields = self._get_fields(source_table)
        pks = self._get_primary_keys(source_table, self.sources)

        lines = []

        # Config block
        if pks and self.bronze_mat == 'incremental':
            lines.append(self._build_config(self.bronze_mat, pks))
        else:
            lines.append(self._build_config(self.bronze_mat))

        lines.append("")

        # SELECT columns
        lines.append("SELECT")
        select_cols = []

        if fields:
            for col_name, col_info in fields.items():
                dtype = col_info.get('datatype', '')
                if _is_string_type(dtype):
                    select_cols.append(f"    TRIM(NULLIF({col_name}, '')) AS {col_name}")
                else:
                    select_cols.append(f"    {col_name}")
        else:
            # Fallback: derive columns from lineage rows referencing this source
            cols_seen = set()
            for _, row in self.lineage.iterrows():
                src = _safe_str(row.get('Source_Table_INSERT', '')).replace('Shortcut_to_', '')
                if src == source_table:
                    col = clean_source_column(_safe_str(row.get('Source_Column_INSERT', '')))
                    if col and col not in cols_seen:
                        cols_seen.add(col)
                        select_cols.append(f"    {col}")
            if not select_cols:
                select_cols.append("    *")

        # Metadata columns
        select_cols.append("    CURRENT_TIMESTAMP() AS _loaded_at")
        select_cols.append(f"    '{source_table}' AS _source")

        lines.append(",\n".join(select_cols))

        # FROM
        lines.append(f"FROM {{{{ source('{self.raw_source_name}', '{source_table}') }}}}")

        # Incremental filter
        if self.bronze_mat == 'incremental':
            lines.append("")
            lines.append("{% if is_incremental() %}")
            lines.append("WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})")
            lines.append("{% endif %}")

        lines.append("")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Silver layer
    # ------------------------------------------------------------------

    def generate_silver_model(self, target_table: str) -> str:
        """Generate a silver model SQL file for a target table."""
        target_lineage = self.lineage[self.lineage['Target_Table'] == target_table]
        if target_lineage.empty:
            return f"-- No lineage found for target table: {target_table}\nSELECT 1"

        # Identify source tables and lookup tables
        actual_sources: Dict[str, str] = {}   # table -> CTE alias
        lookup_tables: Dict[str, Dict] = {}   # table -> {condition, alias, source_cols, target_cols}

        for _, row in target_lineage.iterrows():
            src = _safe_str(row.get('Source_Table_INSERT', '')).replace('Shortcut_to_', '')
            if not src or src in self.SPECIAL_SOURCES:
                continue

            if src in self.sources:
                if src not in actual_sources:
                    actual_sources[src] = _model_name(self.bronze_prefix, src)
            else:
                # Lookup table — collect ALL conditions and column mappings
                cond = _safe_str(row.get('Lookup_Condition', ''))
                src_col = clean_source_column(_safe_str(row.get('Source_Column_INSERT', '')))
                tgt_col = _safe_str(row.get('Target_Column', ''))
                if src not in lookup_tables:
                    lookup_tables[src] = {
                        'condition': cond,
                        'bronze': _model_name(self.bronze_prefix, src),
                        'source_cols': [],
                        'target_cols': [],
                    }
                # Prefer the longest/most complete condition
                if cond and len(cond) > len(lookup_tables[src]['condition']):
                    lookup_tables[src]['condition'] = cond
                if src_col:
                    lookup_tables[src]['source_cols'].append(src_col)
                if tgt_col:
                    lookup_tables[src]['target_cols'].append(tgt_col)

        # Assign short CTE aliases
        src_aliases: Dict[str, str] = {}
        for i, tbl in enumerate(actual_sources.keys()):
            src_aliases[tbl] = f"src_{tbl.lower()}" if len(actual_sources) <= 3 else f"src{i+1}"
        for i, tbl in enumerate(lookup_tables.keys()):
            src_aliases[tbl] = f"lkp_{tbl.lower()}" if len(lookup_tables) <= 3 else f"lkp{i+1}"

        lines = []

        # Determine unique key for incremental models
        silver_unique_keys = self._get_silver_unique_keys(target_table, target_lineage)

        # Config
        if silver_unique_keys and self.silver_mat == 'incremental':
            lines.append(self._build_config(self.silver_mat, silver_unique_keys))
        else:
            lines.append(self._build_config(self.silver_mat))
        lines.append("")

        # CTEs
        cte_parts = []
        for tbl, brz_model in actual_sources.items():
            alias = src_aliases[tbl]
            cte_parts.append(f"{alias} AS (\n    SELECT * FROM {{{{ ref('{brz_model}') }}}}\n)")
        for tbl, info in lookup_tables.items():
            alias = src_aliases[tbl]
            cte_parts.append(f"{alias} AS (\n    SELECT * FROM {{{{ ref('{info['bronze']}') }}}}\n)")

        if cte_parts:
            lines.append("WITH " + ",\n\n".join(cte_parts))
            lines.append("")

        # Pre-resolve UNCONNECTED columns (Tier 1 pattern + optional Tier 2 LLM)
        unconnected_resolutions: Dict[str, Tuple[str, str]] = {}
        if self.resolve_unconnected:
            unconnected_rows = target_lineage[
                target_lineage['Source_Table_INSERT'].fillna('').str.strip() == 'UNCONNECTED'
            ]
        else:
            unconnected_rows = pd.DataFrame()
        if not unconnected_rows.empty:
            tier2_needed: List[str] = []
            for _, urow in unconnected_rows.iterrows():
                tcol = urow['Target_Column']
                expr = _resolve_unconnected_tier1(tcol)
                if expr:
                    unconnected_resolutions[tcol] = (expr, 'pattern')
                else:
                    tier2_needed.append(tcol)

            # Tier 2: LLM batch resolve for columns that didn't match any pattern
            if tier2_needed and self.unconnected_resolver:
                try:
                    llm_results = self.unconnected_resolver(tier2_needed)
                    for col, expr in llm_results.items():
                        if expr and expr.upper() != 'NULL':
                            unconnected_resolutions[col] = (expr, 'llm')
                except Exception:
                    pass  # Fall through to NULL default

        # SELECT columns
        lines.append("SELECT")
        select_cols = []

        main_source = list(actual_sources.keys())[0] if actual_sources else None
        main_alias = src_aliases.get(main_source, 'src') if main_source else 'src'

        for _, row in target_lineage.iterrows():
            target_col = row['Target_Column']
            src_table = _safe_str(row.get('Source_Table_INSERT', '')).replace('Shortcut_to_', '')
            src_col_raw = _safe_str(row.get('Source_Column_INSERT', ''))
            expression = _safe_str(row.get('Expression_Logic', ''))
            clean_col = clean_source_column(src_col_raw)

            if expression:
                converted = convert_informatica_expression(expression, 'snowflake')
                # Prefix bare column references with main alias if we have CTEs
                select_cols.append(f"    {converted} AS {target_col}")

            elif src_table == 'SEQUENCE_GENERATOR' or src_table == 'Lookup/Expression':
                if 'SEQUENCE' in src_col_raw.upper() or src_table == 'SEQUENCE_GENERATOR':
                    select_cols.append(f"    ROW_NUMBER() OVER (ORDER BY 1) AS {target_col}")
                elif clean_col:
                    select_cols.append(f"    {clean_col} AS {target_col}")
                else:
                    select_cols.append(f"    NULL AS {target_col}")

            elif src_table == 'Hardcoded':
                if src_col_raw and (src_col_raw.startswith("'") or src_col_raw.lstrip('-').isdigit()):
                    select_cols.append(f"    {src_col_raw} AS {target_col}")
                elif clean_col:
                    select_cols.append(f"    '{clean_col}' AS {target_col}")
                else:
                    select_cols.append(f"    NULL AS {target_col}  -- Hardcoded")

            elif src_table == 'SYSTEM':
                select_cols.append(f"    CURRENT_TIMESTAMP() AS {target_col}")

            elif src_table == 'UNCONNECTED':
                if target_col in unconnected_resolutions:
                    expr, method = unconnected_resolutions[target_col]
                    select_cols.append(f"    {expr} AS {target_col}  -- UNCONNECTED (resolved: {method})")
                else:
                    select_cols.append(f"    NULL AS {target_col}  -- UNCONNECTED")

            elif src_table == 'Derived':
                if clean_col:
                    select_cols.append(f"    {clean_col} AS {target_col}  -- Derived")
                else:
                    select_cols.append(f"    NULL AS {target_col}  -- Derived")

            elif src_table in actual_sources and clean_col:
                alias = src_aliases[src_table]
                select_cols.append(f"    {alias}.{clean_col} AS {target_col}")

            elif src_table in lookup_tables and clean_col:
                alias = src_aliases[src_table]
                select_cols.append(f"    {alias}.{clean_col} AS {target_col}")

            elif clean_col:
                select_cols.append(f"    {clean_col} AS {target_col}")
            else:
                select_cols.append(f"    NULL AS {target_col}")

        lines.append(",\n".join(select_cols))

        # FROM clause
        if main_source:
            lines.append(f"FROM {main_alias}")
        else:
            lines.append("-- No actual source tables identified")

        # Collect Source_Join_Condition from lineage (useful for both join types)
        lineage_join_conds = []
        for _, row in target_lineage.iterrows():
            val = _safe_str(row.get('Source_Join_Condition', ''))
            if val and val not in lineage_join_conds:
                lineage_join_conds.append(val)

        # JOINs between source tables
        source_list = list(actual_sources.keys())
        if len(source_list) > 1:
            join_keys = find_join_keys(self.sources, source_list)
            for tbl in source_list[1:]:
                alias = src_aliases[tbl]
                pair_key = (source_list[0], tbl)
                if pair_key in join_keys and join_keys[pair_key]:
                    key_conds = [f"{main_alias}.{k} = {alias}.{k}" for k in join_keys[pair_key]]
                    lines.append(f"JOIN {alias}")
                    lines.append(f"    ON {' AND '.join(key_conds)}")
                elif lineage_join_conds:
                    # Use Source_Join_Condition from lineage
                    jc = lineage_join_conds[0]
                    jc = _qualify_join_condition(jc, main_alias, alias)
                    lines.append(f"JOIN {alias}")
                    lines.append(f"    ON {jc}")
                else:
                    # Last resort: try to find shared column names from lineage
                    shared = _find_shared_columns_from_lineage(
                        target_lineage, source_list[0], tbl)
                    if shared:
                        key_conds = [f"{main_alias}.{k} = {alias}.{k}" for k in shared]
                        lines.append(f"JOIN {alias}")
                        lines.append(f"    ON {' AND '.join(key_conds)}")
                    else:
                        lines.append(f"JOIN {alias}")
                        lines.append(f"    ON 1=1  -- TODO: specify join keys between {source_list[0]} and {tbl}")

        if lineage_join_conds and len(source_list) <= 1:
            for jc in lineage_join_conds:
                lines.append(f"-- Source Join: {jc}")

        # Lookup LEFT JOINs
        for lkp_table, lkp_info in lookup_tables.items():
            lkp_alias = src_aliases[lkp_table]
            lkp_condition = lkp_info.get('condition', '')
            if lkp_condition:
                on_clause = _build_lookup_on_clause(
                    lkp_condition, main_alias, lkp_alias)
                lines.append(f"LEFT JOIN {lkp_alias}")
                lines.append(f"    ON {on_clause}")
            else:
                # Infer join from shared column names between source and lookup
                inferred = _infer_lookup_join(
                    lkp_table, lkp_info, main_source, self.sources,
                    self.targets, target_lineage, main_alias, lkp_alias)
                lines.append(f"LEFT JOIN {lkp_alias}")
                lines.append(f"    ON {inferred}")

        # WHERE clause from source filter
        source_filter = ''
        for _, row in target_lineage.iterrows():
            val = _safe_str(row.get('Source_Filter', ''))
            if val:
                source_filter = val
                break
        if source_filter:
            lines.append(f"WHERE {source_filter}")

        lines.append("")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Schema / sources YAML
    # ------------------------------------------------------------------

    def generate_sources_yml(self) -> str:
        """Generate the dbt sources.yml file."""
        source_tables = self.get_source_tables()
        lines = [
            "version: 2",
            "",
            "sources:",
            f"  - name: {self.raw_source_name}",
            '    description: "Raw source data from Informatica ETL"',
            "    schema: \"{{ var('raw_schema', 'RAW') }}\"",
            "    tables:",
        ]
        for tbl in source_tables:
            lines.append(f"      - name: {tbl}")
            lines.append(f"        description: {_safe_yaml_value(f'Source table: {tbl}')}")
        lines.append("")
        return "\n".join(lines)

    def generate_bronze_schema_yml(self) -> str:
        """Generate models/{bronze_layer}/schema.yml."""
        source_tables = self.get_source_tables()
        lines = [
            "version: 2",
            "",
            "models:",
        ]
        for tbl in source_tables:
            model_name = _model_name(self.bronze_prefix, tbl)
            lines.append(f"  - name: {model_name}")
            lines.append(f"    description: {_safe_yaml_value(f'{self.bronze_layer.title()} layer staging for {tbl}')}")
            lines.append("    columns:")

            fields = self._get_fields(tbl)
            if fields:
                for col_name, col_info in fields.items():
                    lines.append(f"      - name: {col_name}")
                    dtype = col_info.get('datatype', '')
                    lines.append(f"        description: {_safe_yaml_value(f'{col_name} ({dtype})')}")
                    # Tests
                    tests = []
                    keytype = str(col_info.get('keytype', '')).upper()
                    nullable = str(col_info.get('nullable', '')).upper()
                    if 'PRIMARY' in keytype:
                        tests.append('unique')
                        tests.append('not_null')
                    elif nullable in ('NOTNULL', 'NOT NULL', 'N', 'NO', '0'):
                        tests.append('not_null')
                    if tests:
                        lines.append("        tests:")
                        for t in tests:
                            lines.append(f"          - {t}")
            # Metadata columns
            lines.append("      - name: _loaded_at")
            lines.append('        description: "Timestamp when the record was loaded"')
            lines.append("      - name: _source")
            lines.append('        description: "Name of the source table"')
            lines.append("")

        return "\n".join(lines)

    def generate_silver_schema_yml(self) -> str:
        """Generate models/silver/schema.yml."""
        target_tables = self.get_target_tables()
        lines = [
            "version: 2",
            "",
            "models:",
        ]
        for tbl in target_tables:
            model_name = _model_name(self.silver_prefix, tbl)
            target_lineage = self.lineage[self.lineage['Target_Table'] == tbl]

            # Collect source info for description
            src_tables = set()
            for val in target_lineage['Source_Table_INSERT'].dropna().unique():
                clean = val.replace('Shortcut_to_', '') if val else ''
                if clean and clean not in self.SPECIAL_SOURCES:
                    src_tables.add(clean)

            desc = f"{self.silver_layer.title()} layer for {tbl}"
            if src_tables:
                desc += f" (sources: {', '.join(sorted(src_tables))})"

            lines.append(f"  - name: {model_name}")
            lines.append(f"    description: {_safe_yaml_value(desc)}")
            lines.append("    columns:")

            for _, row in target_lineage.iterrows():
                target_col = _safe_str(row['Target_Column'])
                expression = _safe_str(row.get('Expression_Logic', ''))
                src_table = _safe_str(row.get('Source_Table_INSERT', ''))
                src_col = _safe_str(row.get('Source_Column_INSERT', ''))

                # Build description
                col_desc = f"Maps from {src_table}.{src_col}" if src_table and src_col else target_col
                if expression:
                    expr_short = expression[:80] + ('...' if len(expression) > 80 else '')
                    col_desc += f" | Expr: {expr_short}"

                lines.append(f"      - name: {target_col}")
                lines.append(f"        description: {_safe_yaml_value(col_desc)}")

                # Tests
                tests = []
                target_key = _safe_str(row.get('Target_Key', ''))
                target_dtype = _safe_str(row.get('Target_Datatype', ''))

                if target_key == 'PK':
                    tests.append('unique')
                    tests.append('not_null')
                elif 'NOTNULL' in target_dtype.upper():
                    tests.append('not_null')

                # Relationship test for single-source lookup columns
                src_clean = src_table.replace('Shortcut_to_', '') if src_table else ''
                if (src_clean and src_clean not in self.SPECIAL_SOURCES
                        and src_clean not in self.sources):
                    clean_col = clean_source_column(src_col)
                    if clean_col:
                        tests.append({
                            'relationships': {
                                'to': f"ref('{_model_name(self.bronze_prefix, src_clean)}')",
                                'field': clean_col,
                            }
                        })

                if tests:
                    lines.append("        tests:")
                    for t in tests:
                        if isinstance(t, str):
                            lines.append(f"          - {t}")
                        elif isinstance(t, dict):
                            for test_name, test_config in t.items():
                                lines.append(f"          - {test_name}:")
                                for k, v in test_config.items():
                                    lines.append(f"              {k}: {v}")

            lines.append("")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def generate_all(self) -> Dict[str, str]:
        """Generate all dbt model files. Returns {relative_path: content}."""
        files: Dict[str, str] = {}

        source_tables = self.get_source_tables()
        target_tables = self.get_target_tables()

        # Bronze models
        for tbl in source_tables:
            model_name = _model_name(self.bronze_prefix, tbl)
            files[f"models/{self.bronze_layer}/{model_name}.sql"] = self.generate_bronze_model(tbl)

        # Silver models
        for tbl in target_tables:
            model_name = _model_name(self.silver_prefix, tbl)
            files[f"models/{self.silver_layer}/{model_name}.sql"] = self.generate_silver_model(tbl)

        # Schema files
        if source_tables:
            files[f"models/{self.bronze_layer}/schema.yml"] = self.generate_bronze_schema_yml()
        if target_tables:
            files[f"models/{self.silver_layer}/schema.yml"] = self.generate_silver_schema_yml()

        # Sources
        if source_tables:
            files["sources.yml"] = self.generate_sources_yml()

        return files

    def generate_zip(self) -> io.BytesIO:
        """Generate all files and pack into a ZIP archive."""
        files = self.generate_all()
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
            for path, content in sorted(files.items()):
                zf.writestr(f"dbt_models/{path}", content)
        buf.seek(0)
        return buf

    # ------------------------------------------------------------------
    # Static validation
    # ------------------------------------------------------------------

    def validate_models(self, files: Dict[str, str] = None) -> List[Dict[str, str]]:
        """
        Run static validation checks on generated dbt models.

        Returns a list of issue dicts: {file, severity, message}.
        Severity is 'error', 'warning', or 'info'.
        """
        if files is None:
            files = self.generate_all()

        issues: List[Dict[str, str]] = []

        for path, content in files.items():
            if not path.endswith('.sql'):
                continue

            fname = path.split('/')[-1]

            # Check for TODO placeholders
            if '<join_key>' in content or '<key>' in content:
                issues.append({
                    'file': fname, 'severity': 'error',
                    'message': 'Contains TODO placeholder (<join_key> or <key>) — join condition needs manual specification',
                })

            # Check for unresolved UNCONNECTED columns (NULL AS ... -- UNCONNECTED)
            null_unconnected = re.findall(r'NULL AS (\S+)\s+-- UNCONNECTED', content)
            if null_unconnected:
                issues.append({
                    'file': fname, 'severity': 'warning',
                    'message': f'{len(null_unconnected)} UNCONNECTED column(s) defaulting to NULL: {", ".join(null_unconnected[:5])}',
                })

            # Check for empty SELECT (no real columns)
            if 'SELECT 1' in content:
                issues.append({
                    'file': fname, 'severity': 'error',
                    'message': 'No lineage found — model generates SELECT 1',
                })

            # Check for missing FROM clause
            if 'SELECT' in content.upper() and 'FROM' not in content.upper() and 'SELECT 1' not in content:
                issues.append({
                    'file': fname, 'severity': 'error',
                    'message': 'SELECT without FROM clause',
                })

            # Check for "No actual source tables identified"
            if 'No actual source tables identified' in content:
                issues.append({
                    'file': fname, 'severity': 'warning',
                    'message': 'No actual source tables identified — model has no FROM clause',
                })

            # Check for NaN string in output (sanity check)
            if re.search(r'\bnan\b', content, re.IGNORECASE):
                issues.append({
                    'file': fname, 'severity': 'error',
                    'message': 'Contains "nan" value — likely NaN leak from pandas',
                })

            # Check for duplicate column names in SELECT
            col_pattern = re.findall(r'AS\s+(\S+)', content)
            if col_pattern:
                seen = set()
                dupes = set()
                for col in col_pattern:
                    col_upper = col.upper()
                    if col_upper in seen:
                        dupes.add(col)
                    seen.add(col_upper)
                if dupes:
                    issues.append({
                        'file': fname, 'severity': 'warning',
                        'message': f'Duplicate column alias(es): {", ".join(sorted(dupes))}',
                    })

        # Summary
        if not issues:
            issues.append({
                'file': '-', 'severity': 'info',
                'message': 'All models passed validation checks',
            })

        return issues
