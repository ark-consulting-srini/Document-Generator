"""
SQL Generator Module — Technical Document Generator (TDG)

Converts source file lineage to SQL for:
  PostgreSQL, Microsoft SQL (T-SQL), Databricks SQL, Databricks Python Notebooks
- DDL: CREATE TABLE statements from source/target definitions
- DML: INSERT INTO ... SELECT statements
- MERGE: Upsert statements for incremental loads
- Stored Procedures / Python Notebooks: Complete ETL with error handling
"""

import re
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field
import pandas as pd

from config.settings import (
    DATE_FORMAT_MAPPINGS,
    get_type_mappings,
    get_function_mappings,
    get_sql_templates,
)


def clean_source_column(source_col: str) -> str:
    """
    Clean up source column name by removing tracing info.

    Handles formats like:
    - "Calculation: in_ALT_PROD_LVL_1_CODE <- TABLE.COLUMN; in_XXX <- TABLE.YYY"
    - "in_COLUMN <- TABLE.COLUMN"
    - "Expression: IIF(...)"
    - "COLUMN (from TABLE)"
    - Just "COLUMN"

    Returns the clean column name suitable for SQL.
    """
    if not source_col:
        return ''

    col = source_col.strip()

    # Handle "Calculation: ..." or "Derived: ..." format
    if col.startswith('Calculation:') or col.startswith('Derived:') or col.startswith('Expression:'):
        table_col_match = re.search(r'([A-Z][A-Z0-9_]+)\.([A-Z][A-Z0-9_]+)', col)
        if table_col_match:
            return table_col_match.group(2)

        in_col_match = re.search(r'in_([A-Z][A-Z0-9_]+)', col)
        if in_col_match:
            return in_col_match.group(1)

        return ''

    # Handle "COLUMN (from TABLE)" format
    if ' (from ' in col:
        col = col.split(' (from ')[0].strip()

    # Handle "in_COLUMN <- TABLE.COLUMN" format
    if '<-' in col:
        parts = col.split('<-')
        last_part = parts[-1].strip()

        if '.' in last_part:
            col = last_part.split('.')[-1].strip()
        else:
            col = last_part

        if ';' in col:
            col = col.split(';')[0].strip()

    # Handle "in_COLUMN" prefix - remove it
    if col.lower().startswith('in_'):
        col = col[3:]

    # Remove any parenthetical info
    if '(' in col:
        col = col.split('(')[0].strip()

    return col


def find_join_keys(sources_dict: Dict, source_tables: List[str]) -> Dict[str, List[str]]:
    """
    Find potential join keys between source tables.

    Looks for:
    1. Columns with identical names across tables
    2. Columns with common key suffixes (_ID, _KEY, _CD, _CODE, _NO)
    3. Primary key columns

    Returns: Dict mapping (table1, table2) pairs to list of potential join columns
    """
    if len(source_tables) < 2:
        return {}

    table_columns = {}
    table_pk_columns = {}

    for table in source_tables:
        if table in sources_dict:
            fields = sources_dict[table].get('fields', {})
            table_columns[table] = set(fields.keys())

            pks = set()
            for field_name, field_info in fields.items():
                keytype = field_info.get('keytype', '')
                if keytype and 'PRIMARY' in str(keytype).upper():
                    pks.add(field_name)
            table_pk_columns[table] = pks

    join_keys = {}

    main_table = source_tables[0]
    main_cols = table_columns.get(main_table, set())
    main_pks = table_pk_columns.get(main_table, set())

    for other_table in source_tables[1:]:
        other_cols = table_columns.get(other_table, set())
        other_pks = table_pk_columns.get(other_table, set())

        common_cols = main_cols & other_cols

        key_suffixes = ('_ID', '_KEY', '_CD', '_CODE', '_NO', '_NUM', '_NBR')

        pk_common = common_cols & (main_pks | other_pks)
        key_cols = {c for c in common_cols if any(c.upper().endswith(s) for s in key_suffixes)}
        other_common = common_cols - pk_common - key_cols

        potential_keys = []
        potential_keys.extend(sorted(pk_common))
        potential_keys.extend(sorted(key_cols - pk_common))

        if len(potential_keys) < 2:
            potential_keys.extend(sorted(other_common)[:3])

        if potential_keys:
            join_keys[(main_table, other_table)] = potential_keys[:3]

    return join_keys


@dataclass
class ColumnDefinition:
    """Represents a database column definition"""
    name: str
    datatype: str
    precision: Optional[str] = None
    scale: Optional[str] = None
    nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    default_value: Optional[str] = None
    comment: Optional[str] = None

    def to_ddl(self, target_platform: str = 'postgresql') -> str:
        """Convert to DDL column definition for the given platform."""
        mapped_type = map_datatype(self.datatype, self.precision, self.scale, target_platform)

        parts = [self.name, mapped_type]

        if not self.nullable:
            parts.append('NOT NULL')

        if self.default_value:
            parts.append(f"DEFAULT {self.default_value}")

        if self.comment:
            safe_comment = self.comment.replace("'", "''")
            parts.append(f"COMMENT '{safe_comment}'")

        return ' '.join(parts)

    # Backward-compat alias
    def to_snowflake_ddl(self) -> str:
        return self.to_ddl('postgresql')


@dataclass
class TableDefinition:
    """Represents a database table definition"""
    name: str
    schema: Optional[str] = None
    columns: List[ColumnDefinition] = field(default_factory=list)
    primary_keys: List[str] = field(default_factory=list)
    comment: Optional[str] = None

    def to_ddl(self, target_platform: str = 'postgresql', if_not_exists: bool = True) -> str:
        """Generate CREATE TABLE statement for the given platform."""
        qualified_name = f"{self.schema}.{self.name}" if self.schema else self.name

        col_defs = [f"    {col.to_ddl(target_platform)}" for col in self.columns]

        if self.primary_keys:
            pk_cols = ', '.join(self.primary_keys)
            col_defs.append(f"    PRIMARY KEY ({pk_cols})")

        columns_sql = ',\n'.join(col_defs)

        exists_clause = "IF NOT EXISTS " if if_not_exists else ""

        if target_platform in ('databricks_sql', 'databricks_python'):
            sql = f"CREATE TABLE {exists_clause}{qualified_name} (\n{columns_sql}\n)\nUSING DELTA"
            if self.comment:
                sql += f"\nCOMMENT '{self.comment.replace(chr(39), chr(39)*2)}'"
        elif target_platform == 'mssql':
            # MSSQL uses bracket quoting and a different existence check
            schema_part, table_part = (qualified_name.split('.', 1) + [''])[:2]
            sql = (
                f"IF NOT EXISTS (\n"
                f"    SELECT * FROM INFORMATION_SCHEMA.TABLES\n"
                f"    WHERE TABLE_SCHEMA = '{schema_part}' AND TABLE_NAME = '{table_part or schema_part}'\n"
                f")\nCREATE TABLE [{schema_part}].[{table_part or schema_part}] (\n{columns_sql}\n)"
            )
        else:
            # PostgreSQL and default
            sql = f"CREATE TABLE {exists_clause}{qualified_name} (\n{columns_sql}\n)"
            if self.comment:
                sql += f"\nCOMMENT '{self.comment.replace(chr(39), chr(39)*2)}'"

        return sql + ";"

    # Backward-compat alias
    def to_snowflake_ddl(self, if_not_exists: bool = True) -> str:
        return self.to_ddl('postgresql', if_not_exists)


class SQLGenerator:
    """
    Main SQL Generator class for converting lineage data to SQL.
    Supports: PostgreSQL, Microsoft SQL (T-SQL), Databricks SQL, Databricks Python Notebooks.
    """

    def __init__(self, sources_dict: Dict, targets_dict: Dict,
                 lineage_df: pd.DataFrame, mappings_dict: Dict = None,
                 target_platform: str = 'postgresql'):
        self.sources = sources_dict
        self.targets = targets_dict
        self.lineage = lineage_df
        self.mappings = mappings_dict or {}
        self.target_platform = target_platform

    def generate_source_ddl(self, table_name: str = None,
                           schema: str = 'SOURCE') -> str:
        tables = [table_name] if table_name else list(self.sources.keys())
        ddl_statements = []

        for tbl in tables:
            if tbl not in self.sources:
                continue
            table_def = self._build_table_definition(
                tbl, self.sources[tbl].get('fields', {}), schema
            )
            ddl_statements.append(table_def.to_ddl(self.target_platform))

        return '\n\n'.join(ddl_statements)

    def generate_target_ddl(self, table_name: str = None,
                           schema: str = 'TARGET') -> str:
        tables = [table_name] if table_name else list(self.targets.keys())
        ddl_statements = []

        for tbl in tables:
            if tbl not in self.targets:
                continue
            table_def = self._build_table_definition(
                tbl, self.targets[tbl].get('fields', {}), schema
            )
            ddl_statements.append(table_def.to_ddl(self.target_platform))

        return '\n\n'.join(ddl_statements)

    def generate_insert_sql(self, target_table: str,
                           source_schema: str = 'SOURCE',
                           target_schema: str = 'TARGET',
                           include_comments: bool = True) -> str:
        """Generate INSERT INTO ... SELECT statement from lineage."""
        target_lineage = self.lineage[self.lineage['Target_Table'] == target_table]

        if target_lineage.empty:
            return f"-- No lineage found for target table: {target_table}"

        actual_source_tables = set(self.sources.keys()) if self.sources else set()

        source_table_columns = {}
        target_columns = []

        special_sources = {'Hardcoded', 'Derived', 'SYSTEM', 'UNCONNECTED', 'SEQUENCE_GENERATOR', 'Lookup/Expression', ''}

        # Collect lookup tables referenced by lineage rows
        lookup_tables = {}  # table_name -> {condition, columns}

        # First pass: identify which actual source tables and lookup tables are used
        for _, row in target_lineage.iterrows():
            target_col = row['Target_Column']
            source_table = row.get('Source_Table_INSERT', '')
            source_col = row.get('Source_Column_INSERT', '')

            target_columns.append(target_col)

            source_table_clean = source_table.replace('Shortcut_to_', '') if source_table else ''

            if source_table_clean and source_table_clean not in special_sources:
                if source_table_clean in actual_source_tables:
                    if source_table_clean not in source_table_columns:
                        source_table_columns[source_table_clean] = []
                    clean_col = clean_source_column(source_col)
                    if clean_col:
                        source_table_columns[source_table_clean].append((clean_col, target_col))
                else:
                    # This is a lookup table — collect for LEFT JOIN
                    if source_table_clean not in lookup_tables:
                        lookup_condition = row.get('Lookup_Condition', '')
                        lookup_tables[source_table_clean] = {
                            'condition': lookup_condition,
                            'columns': [],
                        }
                    clean_col = clean_source_column(source_col)
                    if clean_col:
                        lookup_tables[source_table_clean]['columns'].append((clean_col, target_col))

        source_tables_list = list(source_table_columns.keys())
        table_aliases = {table: f"src{i+1}" for i, table in enumerate(source_tables_list)}

        # Assign aliases for lookup tables
        lkp_idx = 1
        for lkp_table in lookup_tables:
            table_aliases[lkp_table] = f"lkp{lkp_idx}"
            lkp_idx += 1

        # Collect Source_Join_Condition and Source_Filter from lineage
        lineage_join_condition = ''
        lineage_source_filter = ''
        for _, row in target_lineage.iterrows():
            if not lineage_join_condition and row.get('Source_Join_Condition', ''):
                lineage_join_condition = row['Source_Join_Condition']
            if not lineage_source_filter and row.get('Source_Filter', ''):
                lineage_source_filter = row['Source_Filter']

        join_keys = find_join_keys(self.sources, source_tables_list)

        # Build source expressions
        source_expressions = []
        for _, row in target_lineage.iterrows():
            target_col = row['Target_Column']
            source_table = row.get('Source_Table_INSERT', '')
            source_col = row.get('Source_Column_INSERT', '')
            expression = row.get('Expression_Logic', '')

            source_table_clean = source_table.replace('Shortcut_to_', '') if source_table else ''
            clean_col = clean_source_column(source_col)

            if expression:
                converted = convert_informatica_expression(expression, self.target_platform)
                if include_comments and len(expression) > 0:
                    source_expressions.append(f"    {converted} AS {target_col}  -- Expr: {expression[:40]}...")
                else:
                    source_expressions.append(f"    {converted} AS {target_col}")
            elif source_table in special_sources or source_table_clean in special_sources:
                if source_col and source_col.startswith("'") and source_col.endswith("'"):
                    source_expressions.append(f"    {source_col} AS {target_col}")
                elif source_col == 'SYSDATE' or 'SYSDATE' in str(source_col).upper():
                    source_expressions.append(f"    CURRENT_TIMESTAMP() AS {target_col}")
                elif source_table == 'UNCONNECTED':
                    source_expressions.append(f"    NULL AS {target_col}  -- UNCONNECTED")
                elif source_table == 'SEQUENCE_GENERATOR':
                    if self.target_platform in ('databricks_sql', 'databricks_python'):
                        source_expressions.append(f"    MONOTONICALLY_INCREASING_ID() AS {target_col}  -- Sequence")
                    elif self.target_platform == 'mssql':
                        source_expressions.append(f"    NEXT VALUE FOR {target_col}_SEQ AS {target_col}  -- Sequence")
                    else:
                        source_expressions.append(f"    nextval('{target_col}_seq') AS {target_col}  -- Sequence")
                elif clean_col:
                    source_expressions.append(f"    {clean_col} AS {target_col}  -- Derived")
                else:
                    source_expressions.append(f"    NULL AS {target_col}  -- Derived/Special")
            elif source_table_clean in actual_source_tables and clean_col:
                alias = table_aliases.get(source_table_clean, 'src')
                source_expressions.append(f"    {alias}.{clean_col} AS {target_col}")
            elif source_table_clean in lookup_tables and clean_col:
                alias = table_aliases.get(source_table_clean, 'lkp')
                source_expressions.append(f"    {alias}.{clean_col} AS {target_col}")
            elif source_table_clean and clean_col:
                source_expressions.append(f"    NULL AS {target_col}  -- Source: {source_table_clean}.{clean_col} (not found)")
            else:
                source_expressions.append(f"    NULL AS {target_col}  -- No source mapping")

        # Build FROM clause with JOINs
        join_hint = ""
        if len(source_tables_list) == 0:
            from_clause = "-- No actual source tables found; check lookups above"
            join_hint = "\n-- NOTE: All columns appear to come from lookups or derived expressions."
        elif len(source_tables_list) == 1:
            table = source_tables_list[0]
            alias = table_aliases[table]
            from_clause = f"FROM {source_schema}.{table} {alias}"
        else:
            main_table = source_tables_list[0]
            main_alias = table_aliases[main_table]
            from_clause = f"FROM {source_schema}.{main_table} {main_alias}"

            join_clauses = []
            for table in source_tables_list[1:]:
                alias = table_aliases[table]
                join_clauses.append(f"JOIN {source_schema}.{table} {alias}")

                pair_key = (main_table, table)
                if pair_key in join_keys and join_keys[pair_key]:
                    key_conditions = [f"{main_alias}.{k} = {alias}.{k}" for k in join_keys[pair_key]]
                    join_clauses.append(f"    ON {' AND '.join(key_conditions)}")
                else:
                    join_clauses.append(f"    ON {main_alias}.<join_key> = {alias}.<join_key>  -- TODO: Specify join condition")

            # Use Source_Join_Condition from lineage if available
            if lineage_join_condition and not join_clauses:
                from_clause = from_clause + "\n" + lineage_join_condition
            elif join_clauses:
                from_clause = from_clause + "\n" + "\n".join(join_clauses)

            missing_keys = [t for t in source_tables_list[1:] if (main_table, t) not in join_keys]
            if missing_keys:
                join_hint = f"\n-- NOTE: Could not detect join keys for: {', '.join(missing_keys)}. Please verify."

        # Append lookup table LEFT JOINs
        main_alias = table_aliases.get(source_tables_list[0], 'src1') if source_tables_list else 'src'
        for lkp_table, lkp_info in lookup_tables.items():
            lkp_alias = table_aliases[lkp_table]
            lkp_condition = lkp_info.get('condition', '')
            if lkp_condition:
                # Convert "in_FIELD = FIELD" style to "main_alias.FIELD = lkp_alias.FIELD"
                on_clause = lkp_condition
                on_clause = re.sub(r'\bin_([A-Za-z][A-Za-z0-9_]*)\b', rf'{main_alias}.\1', on_clause)
                # If right-side fields don't have a table prefix, add lookup alias
                parts = on_clause.split('=')
                if len(parts) == 2:
                    right = parts[1].strip()
                    if '.' not in right:
                        on_clause = f"{parts[0].strip()} = {lkp_alias}.{right}"
                from_clause += f"\nLEFT JOIN {source_schema}.{lkp_table} {lkp_alias}\n    ON {on_clause}"
            else:
                from_clause += f"\nLEFT JOIN {source_schema}.{lkp_table} {lkp_alias}\n    ON {main_alias}.<key> = {lkp_alias}.<key>  -- TODO: Specify lookup join condition"

        # Build SQL
        platform_labels = {
            'postgresql': 'PostgreSQL', 'mssql': 'Microsoft SQL (T-SQL)',
            'databricks_sql': 'Databricks SQL', 'databricks_python': 'Databricks Python',
        }
        platform_label = platform_labels.get(self.target_platform, self.target_platform)
        sql_parts = [
            f"-- Generated INSERT for: {target_table} ({platform_label})",
            f"-- Source tables (from XML): {', '.join(source_tables_list) if source_tables_list else 'None'}",
        ]

        if join_hint:
            sql_parts.append(join_hint)

        sql_parts.extend([
            "",
            f"INSERT INTO {target_schema}.{target_table} (",
            "    " + ",\n    ".join(target_columns),
            ")",
            "SELECT",
            ",\n".join(source_expressions),
            from_clause,
        ])

        if lineage_source_filter:
            sql_parts.append(f"WHERE {lineage_source_filter}")
        else:
            sql_parts.append("-- WHERE <filter_conditions>")
        sql_parts.append(";")

        return '\n'.join(sql_parts)

    def generate_merge_sql(self, target_table: str,
                          merge_keys: List[str],
                          source_schema: str = 'SOURCE',
                          target_schema: str = 'TARGET') -> str:
        """Generate MERGE statement for upsert operations."""
        target_lineage = self.lineage[self.lineage['Target_Table'] == target_table]

        if target_lineage.empty:
            return f"-- No lineage found for target table: {target_table}"

        actual_source_tables = set(self.sources.keys()) if self.sources else set()

        special_sources = {'Hardcoded', 'Derived', 'SYSTEM', 'UNCONNECTED', 'SEQUENCE_GENERATOR', 'Lookup/Expression', ''}

        all_columns = []
        source_expressions = []
        update_sets = []
        source_table_columns = {}

        # First pass: identify actual source tables
        for _, row in target_lineage.iterrows():
            source_table = row.get('Source_Table_INSERT', '')
            source_table_clean = source_table.replace('Shortcut_to_', '') if source_table else ''
            source_col = row.get('Source_Column_INSERT', '')
            target_col = row['Target_Column']

            if source_table_clean in actual_source_tables:
                if source_table_clean not in source_table_columns:
                    source_table_columns[source_table_clean] = []
                clean_col = clean_source_column(source_col)
                if clean_col:
                    source_table_columns[source_table_clean].append((clean_col, target_col))

        source_tables_list = list(source_table_columns.keys())
        table_aliases = {table: f"s{i+1}" for i, table in enumerate(source_tables_list)}

        source_join_keys = find_join_keys(self.sources, source_tables_list)

        # Build source expressions
        for _, row in target_lineage.iterrows():
            target_col = row['Target_Column']
            source_table = row.get('Source_Table_INSERT', '')
            source_col = row.get('Source_Column_INSERT', '')
            expression = row.get('Expression_Logic', '')

            source_table_clean = source_table.replace('Shortcut_to_', '') if source_table else ''
            clean_col = clean_source_column(source_col)

            all_columns.append(target_col)

            if expression:
                converted = convert_informatica_expression(expression, self.target_platform)
                source_expressions.append(f"        {converted} AS {target_col}")
            elif source_table in special_sources or source_table_clean in special_sources:
                if source_col and source_col.startswith("'"):
                    source_expressions.append(f"        {source_col} AS {target_col}")
                elif source_col == 'SYSDATE' or 'SYSDATE' in str(source_col).upper():
                    source_expressions.append(f"        CURRENT_TIMESTAMP() AS {target_col}")
                elif clean_col:
                    source_expressions.append(f"        {clean_col} AS {target_col}  -- Derived")
                else:
                    source_expressions.append(f"        NULL AS {target_col}")
            elif source_table_clean in actual_source_tables and clean_col:
                alias = table_aliases.get(source_table_clean, 's1')
                source_expressions.append(f"        {alias}.{clean_col} AS {target_col}")
            elif source_table_clean and clean_col:
                source_expressions.append(f"        NULL AS {target_col}  -- Lookup: {source_table_clean}.{clean_col}")
            else:
                source_expressions.append(f"        NULL AS {target_col}")

            if target_col not in merge_keys:
                update_sets.append(f"    tgt.{target_col} = src.{target_col}")

        # Build FROM clause
        if len(source_tables_list) == 0:
            from_clause = "-- No actual source tables"
        elif len(source_tables_list) == 1:
            table = source_tables_list[0]
            alias = table_aliases[table]
            from_clause = f"FROM {source_schema}.{table} {alias}"
        else:
            main_table = source_tables_list[0]
            main_alias = table_aliases[main_table]
            join_parts = [f"FROM {source_schema}.{main_table} {main_alias}"]

            for table in source_tables_list[1:]:
                alias = table_aliases[table]
                join_parts.append(f"        JOIN {source_schema}.{table} {alias}")

                pair_key = (main_table, table)
                if pair_key in source_join_keys and source_join_keys[pair_key]:
                    key_conditions = [f"{main_alias}.{k} = {alias}.{k}" for k in source_join_keys[pair_key]]
                    join_parts.append(f"            ON {' AND '.join(key_conditions)}")
                else:
                    join_parts.append(f"            ON {main_alias}.<join_key> = {alias}.<join_key>  -- TODO: Verify")

            from_clause = "\n".join(join_parts)

        merge_condition = ' AND '.join([f"tgt.{k} = src.{k}" for k in merge_keys])

        platform_labels = {
            'postgresql': 'PostgreSQL', 'mssql': 'Microsoft SQL (T-SQL)',
            'databricks_sql': 'Databricks SQL', 'databricks_python': 'Databricks Python',
        }
        platform_label = platform_labels.get(self.target_platform, self.target_platform)
        sql = f"""-- Generated MERGE for: {target_table} ({platform_label})
-- Source tables (from XML): {', '.join(source_tables_list) if source_tables_list else 'None'}
-- Merge keys: {', '.join(merge_keys)}

MERGE INTO {target_schema}.{target_table} AS tgt
USING (
    SELECT
{chr(10).join(source_expressions)}
    {from_clause}
    -- WHERE <source_filter>
) AS src
ON {merge_condition}

WHEN MATCHED THEN UPDATE SET
{chr(10).join(update_sets)}

WHEN NOT MATCHED THEN INSERT (
    {', '.join(all_columns)}
) VALUES (
    {', '.join([f'src.{c}' for c in all_columns])}
);"""

        return sql

    def generate_stored_procedure(self, procedure_name: str,
                                 target_table: str,
                                 operation: str = 'INSERT',
                                 schema: str = 'ETL') -> str:
        """Generate a stored procedure (Snowflake) or Python notebook (Databricks)."""
        # Generate the core DML
        if operation.upper() == 'MERGE':
            pk_cols = self._get_primary_keys(target_table)
            if not pk_cols:
                pk_cols = [self.lineage[self.lineage['Target_Table'] == target_table]['Target_Column'].iloc[0]]
            dml = self.generate_merge_sql(target_table, pk_cols)
        else:
            dml = self.generate_insert_sql(target_table, include_comments=False)

        dml_indented = '\n'.join(['    ' + line for line in dml.split('\n')])

        templates = get_sql_templates(self.target_platform)

        if self.target_platform == 'databricks_python':
            return templates['notebook'].format(
                schema=schema,
                procedure_name=procedure_name,
                target_table=target_table,
                etl_logic=dml_indented,
            )

        return templates['stored_procedure'].format(
            schema=schema,
            procedure_name=procedure_name,
            etl_logic=dml_indented,
        )

    def generate_all_ddl(self, source_schema: str = 'SOURCE',
                        target_schema: str = 'TARGET') -> str:
        parts = [
            "-- ============================================",
            "-- SOURCE TABLE DDL",
            "-- ============================================",
            "",
            self.generate_source_ddl(schema=source_schema),
            "",
            "-- ============================================",
            "-- TARGET TABLE DDL",
            "-- ============================================",
            "",
            self.generate_target_ddl(schema=target_schema),
        ]

        return '\n'.join(parts)

    def _build_table_definition(self, table_name: str,
                               fields: Dict, schema: str) -> TableDefinition:
        """Build TableDefinition from parser output"""
        columns = []
        primary_keys = []

        for field_name, field_info in fields.items():
            col = ColumnDefinition(
                name=field_name,
                datatype=field_info.get('datatype', 'VARCHAR'),
                precision=field_info.get('precision'),
                scale=field_info.get('scale'),
                nullable=field_info.get('nullable', 'Y') != 'NOTNULL',
                is_primary_key='PRIMARY' in str(field_info.get('keytype', '')).upper(),
                is_foreign_key='FOREIGN' in str(field_info.get('keytype', '')).upper(),
            )
            columns.append(col)

            if col.is_primary_key:
                primary_keys.append(field_name)

        return TableDefinition(
            name=table_name,
            schema=schema,
            columns=columns,
            primary_keys=primary_keys
        )

    def _get_primary_keys(self, table_name: str) -> List[str]:
        """Get primary key columns for a table"""
        if table_name in self.targets:
            fields = self.targets[table_name].get('fields', {})
            return [f for f, info in fields.items()
                    if 'PRIMARY' in str(info.get('keytype', '')).upper()]
        return []


# =============================================================================
# STANDALONE FUNCTIONS
# =============================================================================

def map_datatype(informatica_type: str,
                 precision: str = None,
                 scale: str = None,
                 target_platform: str = 'postgresql') -> str:
    """
    Map Informatica datatype to the target platform datatype.

    Args:
        informatica_type: Informatica datatype name
        precision: Precision value
        scale: Scale value
        target_platform: 'postgresql' | 'mssql' | 'databricks_sql' | 'databricks_python'

    Returns:
        Target platform datatype string
    """
    is_databricks = target_platform in ('databricks_sql', 'databricks_python')

    if not informatica_type:
        return 'STRING' if is_databricks else 'VARCHAR'

    type_lower = informatica_type.lower().strip()
    type_lower = re.sub(r'\([^)]*\)', '', type_lower).strip()

    type_map = get_type_mappings(target_platform)
    default_type = 'STRING' if is_databricks else 'VARCHAR'
    mapped_type = type_map.get(type_lower, default_type)

    # Add precision/scale where appropriate
    if mapped_type in ('DECIMAL', 'NUMERIC', 'NUMBER'):
        if precision and scale:
            return f"{mapped_type}({precision},{scale})"
        elif precision:
            return f"{mapped_type}({precision})"
    elif mapped_type in ('VARCHAR', 'CHAR', 'NVARCHAR', 'BINARY', 'VARBINARY'):
        if precision:
            return f"{mapped_type}({precision})"

    return mapped_type


# Backward-compat wrapper
def map_datatype_to_snowflake(informatica_type: str,
                              precision: str = None,
                              scale: str = None) -> str:
    """Backward-compat: now maps to PostgreSQL (closest ANSI equivalent)."""
    return map_datatype(informatica_type, precision, scale, 'postgresql')


def _convert_decode_to_case(expr: str) -> str:
    """Convert Informatica DECODE(expr, search1, result1, ..., default) to CASE WHEN for Databricks."""
    match = re.search(r'\bDECODE\s*\(', expr, flags=re.IGNORECASE)
    if not match:
        return expr

    # Find matching closing paren
    start = match.start()
    paren_start = match.end() - 1
    depth = 1
    i = paren_start + 1
    while i < len(expr) and depth > 0:
        if expr[i] == '(':
            depth += 1
        elif expr[i] == ')':
            depth -= 1
        i += 1

    if depth != 0:
        return expr  # Unbalanced parens, return as-is

    inner = expr[paren_start + 1:i - 1]
    # Split by top-level commas only
    args = []
    current = ''
    depth = 0
    for ch in inner:
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        elif ch == ',' and depth == 0:
            args.append(current.strip())
            current = ''
            continue
        current += ch
    if current.strip():
        args.append(current.strip())

    if len(args) < 3:
        return expr  # Not enough args for DECODE

    test_expr = args[0]
    pairs = args[1:]

    case_parts = ['CASE']
    idx = 0
    while idx + 1 < len(pairs):
        case_parts.append(f" WHEN {test_expr} = {pairs[idx]} THEN {pairs[idx + 1]}")
        idx += 2

    # Remaining arg is the default
    if idx < len(pairs):
        case_parts.append(f" ELSE {pairs[idx]}")

    case_parts.append(' END')
    case_sql = ''.join(case_parts)

    return expr[:start] + case_sql + expr[i:]


def convert_informatica_expression(expression: str,
                                   target_platform: str = 'postgresql') -> str:
    """
    Convert Informatica expression to target platform SQL.

    Handles:
    - Transformation chain metadata (exp_xxx: expr | exp_yyy: expr)
    - Input port references (in_COLUMN -> COLUMN)
    - Variable references (var_XXX -> placeholder)
    - Function conversions per platform

    Args:
        expression: Informatica expression string
        target_platform: 'postgresql' | 'mssql' | 'databricks_sql' | 'databricks_python'

    Returns:
        Target platform SQL expression
    """
    if not expression:
        return 'NULL'

    result = str(expression).strip()

    # ===========================================
    # Step 1: Clean up transformation chain metadata
    # ===========================================
    if ' | ' in result:
        parts = result.split(' | ')
        best_expr = None

        for part in reversed(parts):
            part = part.strip()
            if not part:
                continue

            if ':' in part:
                colon_idx = part.find(':')
                expr = part[colon_idx + 1:].strip()

                if expr and not (expr.startswith('var_') and ' ' not in expr):
                    if any(c in expr for c in '()+/*-') or 'IIF' in expr.upper() or 'IFF' in expr.upper():
                        best_expr = expr
                        break
                    elif not best_expr:
                        best_expr = expr
            else:
                if part and not part.startswith('var_'):
                    best_expr = part

        if best_expr:
            result = best_expr

    if ':' in result:
        colon_idx = result.find(':')
        prefix = result[:colon_idx].strip().lower()
        if any(prefix.startswith(p) for p in ['exp_', 'lkp_', 'sq_', 'rtr_', 'flt_', 'agg_', 'jnr_', 'union_']):
            result = result[colon_idx + 1:].strip()

    # ===========================================
    # Step 2: Handle AS alias pattern
    # ===========================================
    as_match = re.match(r"^([^A-Za-z]*?|'[^']*'|\"[^\"]*\"|\d+(?:\.\d+)?)\s+AS\s+\w+$", result.strip(), re.IGNORECASE)
    if as_match:
        result = as_match.group(1).strip()

    # ===========================================
    # Step 3: Handle input/output port references
    # ===========================================
    result = re.sub(r'\bin_([A-Za-z][A-Za-z0-9_]*)\b', r'\1', result)
    result = re.sub(r'\bout_([A-Za-z][A-Za-z0-9_]*)\b', r'\1', result)

    if result.strip().startswith('var_') and ' ' not in result.strip():
        var_name = result.strip()
        col_name = var_name[4:]
        result = col_name
    else:
        result = re.sub(r'\bvar_([A-Za-z][A-Za-z0-9_]*)\b', r'\1', result)

    # ===========================================
    # Step 4: Function conversions (platform-aware)
    # ===========================================
    is_databricks = target_platform in ('databricks_sql', 'databricks_python')
    is_mssql = (target_platform == 'mssql')
    is_postgresql = (target_platform == 'postgresql')

    # Handle SYSDATE / SYSTIMESTAMP
    sysdate_fn = 'GETDATE()' if is_mssql else 'CURRENT_TIMESTAMP'
    result = re.sub(r'\bSYSDATE\b', sysdate_fn, result, flags=re.IGNORECASE)
    result = re.sub(r'\bSYSTIMESTAMP\b', sysdate_fn, result, flags=re.IGNORECASE)
    result = re.sub(r'\bSESSSTARTTIME\b', sysdate_fn, result, flags=re.IGNORECASE)

    # Handle IIF: Databricks → IF(), MSSQL → IIF() (native), PostgreSQL → CASE WHEN
    if is_databricks:
        result = re.sub(r'\bIIF\s*\(', 'IF(', result, flags=re.IGNORECASE)
    elif is_mssql:
        pass  # IIF is native in T-SQL — keep as-is
    else:
        # PostgreSQL: convert IIF(cond, a, b) -> CASE WHEN cond THEN a ELSE b END
        result = re.sub(
            r'\bIIF\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)',
            r'CASE WHEN \1 THEN \2 ELSE \3 END',
            result, flags=re.IGNORECASE
        )

    # Handle ISNULL with two args -> COALESCE (all platforms)
    result = re.sub(r'\bISNULL\s*\(\s*([^,)]+)\s*,\s*([^)]+)\s*\)',
                    r'COALESCE(\1, \2)', result, flags=re.IGNORECASE)

    # Handle standalone isnull(x) -> x IS NULL
    result = re.sub(r'\bisnull\s*\(\s*([^,)]+)\s*\)',
                    lambda m: f"{m.group(1).strip()} IS NULL",
                    result, flags=re.IGNORECASE)

    # Handle NVL -> COALESCE (PostgreSQL, Databricks); ISNULL (MSSQL)
    if is_mssql:
        result = re.sub(r'\bNVL\s*\(', 'ISNULL(', result, flags=re.IGNORECASE)
    else:
        result = re.sub(r'\bNVL\s*\(', 'COALESCE(', result, flags=re.IGNORECASE)

    # Handle INSTR -> POSITION (PostgreSQL) / CHARINDEX (MSSQL) / LOCATE (Databricks)
    if is_databricks:
        result = re.sub(r'\bINSTR\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
                        r'LOCATE(\2, \1)', result, flags=re.IGNORECASE)
    elif is_mssql:
        result = re.sub(r'\bINSTR\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
                        r'CHARINDEX(\2, \1)', result, flags=re.IGNORECASE)
    else:
        result = re.sub(r'\bINSTR\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
                        r'POSITION(\2 IN \1)', result, flags=re.IGNORECASE)

    # Handle REPLACESTR -> REPLACE
    result = re.sub(r'\bREPLACESTR\s*\(', 'REPLACE(', result, flags=re.IGNORECASE)

    # Handle ADD_TO_DATE
    def convert_add_to_date(match):
        date_expr = match.group(1)
        interval_type = match.group(2).strip("'\"").upper()
        num_expr = match.group(3)
        interval_map = {
            'DD': 'DAY', 'D': 'DAY', 'MM': 'MONTH', 'M': 'MONTH',
            'YY': 'YEAR', 'YYYY': 'YEAR', 'Y': 'YEAR',
            'HH': 'HOUR', 'HH24': 'HOUR', 'H': 'HOUR',
            'MI': 'MINUTE', 'SS': 'SECOND', 'S': 'SECOND',
        }
        interval_name = interval_map.get(interval_type, 'DAY')
        if is_databricks:
            return f"DATE_ADD({date_expr}, {num_expr})" if interval_name == 'DAY' else f"({date_expr} + INTERVAL {num_expr} {interval_name})"
        elif is_mssql:
            return f"DATEADD({interval_name}, {num_expr}, {date_expr})"
        else:
            return f"({date_expr} + INTERVAL '{num_expr} {interval_name}')"

    result = re.sub(
        r'\bADD_TO_DATE\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)',
        convert_add_to_date, result, flags=re.IGNORECASE
    )

    # Handle DATE_DIFF
    def convert_date_diff(match):
        date1 = match.group(1)
        date2 = match.group(2)
        interval_type = match.group(3).strip("'\"").upper()
        interval_map = {
            'DD': 'DAY', 'D': 'DAY', 'MM': 'MONTH', 'M': 'MONTH',
            'YY': 'YEAR', 'YYYY': 'YEAR', 'Y': 'YEAR',
            'HH': 'HOUR', 'HH24': 'HOUR', 'MI': 'MINUTE', 'SS': 'SECOND',
        }
        interval_name = interval_map.get(interval_type, 'DAY')
        if is_databricks:
            return f"DATEDIFF({date1}, {date2})"
        elif is_mssql:
            return f"DATEDIFF({interval_name}, {date2}, {date1})"
        else:
            return f"DATE_PART('{interval_name}', AGE({date1}, {date2}))"

    result = re.sub(
        r'\bDATE_DIFF\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)',
        convert_date_diff, result, flags=re.IGNORECASE
    )

    # Handle TRUNC -> DATE_TRUNC (PostgreSQL/Databricks) / DATETRUNC (MSSQL 2022+)
    def convert_trunc(match):
        expr = match.group(1)
        interval = (match.group(2) or "'DAY'").strip("'\"").upper()
        interval_map = {
            'DD': 'DAY', 'D': 'DAY', 'DDD': 'DAY',
            'MM': 'MONTH', 'MON': 'MONTH', 'MONTH': 'MONTH',
            'YY': 'YEAR', 'YYYY': 'YEAR', 'YEAR': 'YEAR',
            'HH': 'HOUR', 'HH24': 'HOUR', 'MI': 'MINUTE',
            'Q': 'QUARTER', 'WW': 'WEEK', 'W': 'WEEK',
        }
        mapped = interval_map.get(interval, 'DAY')
        if is_mssql:
            return f"DATETRUNC({mapped}, {expr})"
        return f"DATE_TRUNC('{mapped}', {expr})"

    result = re.sub(
        r'\bTRUNC\s*\(\s*([^,)]+)(?:\s*,\s*([^)]+))?\s*\)',
        convert_trunc, result, flags=re.IGNORECASE
    )

    # Handle GET_DATE_PART -> EXTRACT (all platforms)
    if is_mssql:
        result = re.sub(r'\bGET_DATE_PART\s*\(', 'DATEPART(', result, flags=re.IGNORECASE)
    else:
        result = re.sub(r'\bGET_DATE_PART\s*\(', 'EXTRACT(', result, flags=re.IGNORECASE)

    # Handle TO_CHAR
    if is_databricks:
        result = re.sub(r'\bTO_CHAR\s*\(', 'DATE_FORMAT(', result, flags=re.IGNORECASE)
    elif is_mssql:
        result = re.sub(r'\bTO_CHAR\s*\(', 'FORMAT(', result, flags=re.IGNORECASE)
    # PostgreSQL: TO_CHAR is native — no change needed

    # Handle ERROR() / ABORT() -> NULL
    result = re.sub(r'\bERROR\s*\([^)]*\)', 'NULL', result, flags=re.IGNORECASE)
    result = re.sub(r'\bABORT\s*\([^)]*\)', 'NULL', result, flags=re.IGNORECASE)

    # Handle TO_INTEGER, TO_BIGINT, TO_FLOAT -> CAST for all platforms
    result = re.sub(r'\bTO_INTEGER\s*\(\s*([^)]+)\s*\)', r'CAST(\1 AS INTEGER)', result, flags=re.IGNORECASE)
    result = re.sub(r'\bTO_BIGINT\s*\(\s*([^)]+)\s*\)', r'CAST(\1 AS BIGINT)', result, flags=re.IGNORECASE)
    result = re.sub(r'\bTO_FLOAT\s*\(\s*([^)]+)\s*\)',
                    lambda m: f"CAST({m.group(1)} AS DOUBLE)" if is_databricks else f"CAST({m.group(1)} AS DOUBLE PRECISION)",
                    result, flags=re.IGNORECASE)
    result = re.sub(r'\bTO_DECIMAL\s*\(\s*([^)]+)\s*\)', r'CAST(\1 AS DECIMAL)', result, flags=re.IGNORECASE)

    # Handle DECODE -> CASE WHEN (all platforms except Databricks which has DECODE natively)
    if not is_databricks:
        decode_match = re.search(r'\bDECODE\s*\(', result, flags=re.IGNORECASE)
        if decode_match:
            result = _convert_decode_to_case(result)

    # Handle LOOKUP references — replace :LKP.FIELD with just FIELD
    result = re.sub(r':LKP\.(\w+)', r'\1', result)

    # Handle REG_EXTRACT / REG_MATCH
    if is_databricks:
        result = re.sub(r'\bREG_EXTRACT\s*\(', 'REGEXP_EXTRACT(', result, flags=re.IGNORECASE)
        result = re.sub(r'\bREG_MATCH\s*\(', 'RLIKE(', result, flags=re.IGNORECASE)
    elif is_mssql:
        result = re.sub(r'\bREG_EXTRACT\s*\(', 'PATINDEX(', result, flags=re.IGNORECASE)
        result = re.sub(r'\bREG_MATCH\s*\(', 'PATINDEX(', result, flags=re.IGNORECASE)
    else:
        result = re.sub(r'\bREG_EXTRACT\s*\(', 'REGEXP_MATCH(', result, flags=re.IGNORECASE)
        result = re.sub(r'\bREG_MATCH\s*\(', 'REGEXP_MATCH(', result, flags=re.IGNORECASE)

    result = re.sub(r'\bREG_REPLACE\s*\(', 'REGEXP_REPLACE(', result, flags=re.IGNORECASE)

    # ===========================================
    # Step 5: Remove trailing AS alias (we add our own later)
    # ===========================================
    result = re.sub(r'\s+AS\s+[A-Za-z_][A-Za-z0-9_]*\s*$', '', result, flags=re.IGNORECASE)

    # ===========================================
    # Step 6: Clean up whitespace
    # ===========================================
    result = ' '.join(result.split())

    return result


def _build_ddl_block(table_name: str, columns: list, primary_keys: list,
                     schema: str, target_platform: str) -> str:
    """Build a CREATE TABLE DDL block for any supported platform."""
    if primary_keys:
        columns.append(f"    PRIMARY KEY ({', '.join(primary_keys)})")

    col_sql = chr(10).join(columns)

    if target_platform in ('databricks_sql', 'databricks_python'):
        return f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} (\n{col_sql}\n)\nUSING DELTA;"
    elif target_platform == 'mssql':
        return (
            f"IF NOT EXISTS (\n"
            f"    SELECT * FROM INFORMATION_SCHEMA.TABLES\n"
            f"    WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'\n"
            f")\nCREATE TABLE [{schema}].[{table_name}] (\n{col_sql}\n);"
        )
    else:
        return f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} (\n{col_sql}\n);"


def generate_ddl_from_sources(sources_dict: Dict, schema: str = 'SOURCE',
                              target_platform: str = 'postgresql') -> str:
    """Generate DDL for all source tables."""
    ddl_parts = []

    for table_name, table_info in sources_dict.items():
        fields = table_info.get('fields', {})
        columns = []
        primary_keys = []

        for field_name, field_info in fields.items():
            col_type = map_datatype(
                field_info.get('datatype', 'VARCHAR'),
                field_info.get('precision'),
                field_info.get('scale'),
                target_platform
            )
            nullable = '' if field_info.get('nullable', 'Y') == 'Y' else ' NOT NULL'
            columns.append(f"    {field_name} {col_type}{nullable}")
            if 'PRIMARY' in str(field_info.get('keytype', '')).upper():
                primary_keys.append(field_name)

        ddl_parts.append(_build_ddl_block(table_name, columns, primary_keys, schema, target_platform))

    return '\n\n'.join(ddl_parts)


def generate_ddl_from_targets(targets_dict: Dict, schema: str = 'TARGET',
                              target_platform: str = 'postgresql') -> str:
    """Generate DDL for all target tables."""
    ddl_parts = []

    for table_name, table_info in targets_dict.items():
        fields = table_info.get('fields', {})
        columns = []
        primary_keys = []

        for field_name, field_info in fields.items():
            col_type = map_datatype(
                field_info.get('datatype', 'VARCHAR'),
                field_info.get('precision'),
                field_info.get('scale'),
                target_platform
            )
            nullable = '' if field_info.get('nullable', 'Y') == 'Y' else ' NOT NULL'
            columns.append(f"    {field_name} {col_type}{nullable}")
            if 'PRIMARY' in str(field_info.get('keytype', '')).upper():
                primary_keys.append(field_name)

        ddl_parts.append(_build_ddl_block(table_name, columns, primary_keys, schema, target_platform))

    return '\n\n'.join(ddl_parts)


def generate_insert_sql(lineage_df: pd.DataFrame,
                       target_table: str,
                       source_schema: str = 'SOURCE',
                       target_schema: str = 'TARGET',
                       target_platform: str = 'postgresql') -> str:
    """Standalone function to generate INSERT SQL from lineage DataFrame."""
    generator = SQLGenerator({}, {}, lineage_df, target_platform=target_platform)
    return generator.generate_insert_sql(target_table, source_schema, target_schema)


def generate_merge_sql(lineage_df: pd.DataFrame,
                      target_table: str,
                      merge_keys: List[str],
                      source_schema: str = 'SOURCE',
                      target_schema: str = 'TARGET',
                      target_platform: str = 'postgresql') -> str:
    """Standalone function to generate MERGE SQL from lineage DataFrame."""
    generator = SQLGenerator({}, {}, lineage_df, target_platform=target_platform)
    return generator.generate_merge_sql(target_table, merge_keys, source_schema, target_schema)
