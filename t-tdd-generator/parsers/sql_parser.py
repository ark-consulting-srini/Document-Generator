"""
SQL File Lineage Parser for T - TDD Generator

Parses uploaded .sql files (CREATE VIEW, INSERT INTO SELECT, stored procedures, CTEs)
and extracts field-level lineage in the same 22-column format used by InformaticaLineageParser.

Strategy:
  Tier 1 — sqlglot AST parsing (fast, deterministic, handles most ANSI SQL patterns)
  Tier 2 — LLM fallback (for stored procedures, dynamic SQL, or anything sqlglot cannot parse)
"""

import os
import re
import json
from collections import Counter
from typing import Optional, Callable

try:
    import sqlglot
    import sqlglot.expressions as exp
    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False

from utils.helpers import format_business_name, is_special_source


# Special source labels that match InformaticaLineageParser conventions
_SPECIAL_SOURCES = {'Derived', 'Hardcoded', 'SYSTEM', 'UNCONNECTED', 'SEQUENCE_GENERATOR', 'Lookup/Expression'}


class SQLLineageParser:
    """
    Parses .sql files and produces the same 22-column lineage records
    as InformaticaLineageParser.build_lineage().

    Usage:
        parser = SQLLineageParser(sql_bytes, filename="my_view.sql")
        parser.parse_all()
        records = parser.build_lineage(call_llm_fn=call_llm, llm_model="llama3-70b")
    """

    def __init__(self, sql_content, filename: str = "unknown.sql"):
        if isinstance(sql_content, bytes):
            sql_content = sql_content.decode("utf-8", errors="replace")
        self.sql_content = sql_content
        self.filename = filename
        self.mapping_name = os.path.splitext(os.path.basename(filename))[0]

        # Output data — mirrors InformaticaLineageParser public attributes
        self.sources: dict = {}
        self.targets: dict = {}
        self.mappings: dict = {}
        self.sources_data: list = []
        self.targets_data: list = []
        self.transformations_data: list = []
        self.instances_data: list = []
        self.connectors_data: list = []

        self._ast_statements: list = []   # parsed sqlglot statement objects
        self._raw_fallbacks: list = []    # raw SQL strings that sqlglot could not parse

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def parse_all(self):
        """Split the SQL file into individual statements and build the AST."""
        self._ast_statements, self._raw_fallbacks = self._split_and_parse()

    def build_lineage(
        self,
        progress_callback: Optional[Callable] = None,
        call_llm_fn: Optional[Callable] = None,
        llm_model: Optional[str] = None,
    ) -> list:
        """
        Extract field-level lineage from all statements.

        Args:
            progress_callback: Optional fn(current, total, mapping_name)
            call_llm_fn:       platform_utils.call_llm — required for Tier 2 fallback
            llm_model:         LLM model name string for Tier 2 fallback

        Returns:
            List of 22-column lineage dicts (same schema as InformaticaLineageParser).
        """
        records = []
        total = len(self._ast_statements) + len(self._raw_fallbacks)
        processed = 0

        # Tier 1 — sqlglot AST statements
        for stmt in self._ast_statements:
            processed += 1
            if progress_callback:
                progress_callback(processed, total, self.mapping_name)

            stmt_records = self._parse_with_sqlglot(stmt)

            # Tier 2 fallback if sqlglot returned nothing for this statement
            if not stmt_records and call_llm_fn and llm_model:
                try:
                    raw_sql = stmt.sql(dialect="ansi") if hasattr(stmt, "sql") else ""
                except Exception:
                    raw_sql = ""
                if raw_sql.strip():
                    stmt_records = self._parse_with_llm(raw_sql, call_llm_fn, llm_model)

            records.extend(stmt_records)

        # Raw fallbacks — always go to LLM if available
        for raw_sql in self._raw_fallbacks:
            processed += 1
            if progress_callback:
                progress_callback(processed, total, self.mapping_name)

            if call_llm_fn and llm_model and raw_sql.strip():
                records.extend(self._parse_with_llm(raw_sql, call_llm_fn, llm_model))

        # Populate metadata dicts from collected records
        self._build_metadata_from_records(records)

        return records

    # ------------------------------------------------------------------
    # Tier 1 — sqlglot AST parsing
    # ------------------------------------------------------------------

    def _split_and_parse(self):
        """
        Parse the SQL file into sqlglot AST objects.
        Returns (ast_list, raw_fallback_list).
        """
        if not SQLGLOT_AVAILABLE:
            return [], [self.sql_content]

        ast_statements = []
        raw_fallbacks = []

        try:
            parsed = sqlglot.parse(self.sql_content, error_level=sqlglot.ErrorLevel.WARN)
        except Exception:
            parsed = []

        for stmt in parsed:
            if stmt is None:
                continue
            # Stored procedures / CREATE PROCEDURE blocks are complex — send to LLM
            if isinstance(stmt, exp.Create) and stmt.args.get("kind", "").upper() == "PROCEDURE":
                try:
                    raw_fallbacks.append(stmt.sql(dialect="ansi"))
                except Exception:
                    raw_fallbacks.append(str(stmt))
            else:
                ast_statements.append(stmt)

        # If nothing parsed, try splitting on semicolons as a last resort
        if not ast_statements and not raw_fallbacks:
            for chunk in self.sql_content.split(";"):
                chunk = chunk.strip()
                if not chunk:
                    continue
                try:
                    single = sqlglot.parse_one(chunk, error_level=sqlglot.ErrorLevel.WARN)
                    if single:
                        ast_statements.append(single)
                    else:
                        raw_fallbacks.append(chunk)
                except Exception:
                    raw_fallbacks.append(chunk)

        return ast_statements, raw_fallbacks

    def _parse_with_sqlglot(self, stmt) -> list:
        """Dispatch parsed statement to the correct handler."""
        # WITH ... (CTE) wraps the real statement — unwrap it and pass CTE alias map along
        if isinstance(stmt, exp.With):
            cte_alias_map = {cte.alias.lower(): cte.alias.upper() for cte in stmt.expressions}
            inner = stmt.this  # the INSERT / SELECT underneath the CTE
            return self._parse_with_sqlglot_inner(inner, cte_alias_map)
        return self._parse_with_sqlglot_inner(stmt, {})

    def _parse_with_sqlglot_inner(self, stmt, cte_alias_map: dict) -> list:
        """Dispatch after CTE unwrapping."""
        if isinstance(stmt, exp.Insert):
            return self._handle_insert(stmt, extra_alias_map=cte_alias_map)
        elif isinstance(stmt, exp.Create):
            kind = stmt.args.get("kind", "").upper()
            if kind in ("VIEW", "TABLE"):
                return self._handle_create(stmt, extra_alias_map=cte_alias_map)
        elif isinstance(stmt, exp.Select):
            return self._handle_select(stmt, target_table="QUERY_RESULT",
                                       target_col_names=[], extra_alias_map=cte_alias_map)
        return []

    def _handle_insert(self, stmt: "exp.Insert", extra_alias_map: dict = None) -> list:
        """INSERT INTO target [(cols)] SELECT ... FROM sources"""
        target_node = stmt.this

        # When INSERT has an inline column list, sqlglot wraps target in exp.Schema
        if isinstance(target_node, exp.Schema):
            target_table = self._get_table_name(target_node.this)
            target_col_names = [c.name for c in target_node.expressions]
        else:
            target_table = self._get_table_name(target_node)
            # Explicit column list as separate arg (older sqlglot versions)
            target_col_names = []
            if stmt.args.get("columns"):
                target_col_names = [c.name for c in stmt.args["columns"]]

        select = stmt.expression
        if select is None or not isinstance(select, (exp.Select, exp.Union)):
            return []
        return self._handle_select(select, target_table, target_col_names,
                                   extra_alias_map=extra_alias_map)

    def _handle_create(self, stmt: "exp.Create", extra_alias_map: dict = None) -> list:
        """CREATE [OR REPLACE] VIEW name AS SELECT ..."""
        target_table = self._get_table_name(stmt.this)
        select = stmt.expression
        if select is None or not isinstance(select, (exp.Select, exp.Union)):
            return []
        return self._handle_select(select, target_table, [], extra_alias_map=extra_alias_map)

    def _handle_select(self, select, target_table: str, target_col_names: list,
                       extra_alias_map: dict = None) -> list:
        """Core: walk SELECT expressions and produce lineage records."""
        # Unwrap UNION — use first branch for lineage; note it in expression
        union_note = ""
        if isinstance(select, exp.Union):
            union_note = "UNION"
            select = select.this  # first SELECT branch

        alias_map = self._build_alias_map(select)
        # Merge CTE aliases so FROM cte_name resolves correctly
        if extra_alias_map:
            for k, v in extra_alias_map.items():
                alias_map.setdefault(k, v)
        join_condition = self._extract_join_conditions(select)
        source_filter = self._extract_filter(select)

        records = []
        selections = select.expressions if hasattr(select, "expressions") else []

        for i, sel_expr in enumerate(selections):
            # Target column name comes from explicit column list, alias, or expression
            target_col_override = target_col_names[i] if i < len(target_col_names) else None

            record = self._process_selection(
                sel_expr, target_table, target_col_override,
                alias_map, join_condition, source_filter, union_note,
            )
            if record:
                records.append(record)

        return records

    def _process_selection(
        self, sel_expr, target_table: str, target_col_override: Optional[str],
        alias_map: dict, join_condition: str, source_filter: str, union_note: str,
    ) -> Optional[dict]:
        """Convert one SELECT expression into a 22-col lineage record."""
        # Peel alias
        if isinstance(sel_expr, exp.Alias):
            target_col = sel_expr.alias
            inner = sel_expr.this
        else:
            target_col = self._expr_to_col_name(sel_expr)
            inner = sel_expr

        if target_col_override:
            target_col = target_col_override

        if not target_col:
            return None

        # Handle SELECT * — produce one synthetic record
        if isinstance(inner, exp.Star):
            source_table = list(alias_map.values())[0] if alias_map else "UNKNOWN"
            return self._make_record(
                source_table=source_table,
                source_col="*",
                target_table=target_table,
                target_col=target_col,
                expression="*" + (f" [{union_note}]" if union_note else ""),
                join_condition=join_condition,
                source_filter=source_filter,
            )

        src_table, src_col, expr_logic = self._resolve_source(inner, alias_map)

        if union_note and expr_logic:
            expr_logic = f"{expr_logic} [{union_note}]"
        elif union_note:
            expr_logic = f"[{union_note}]"

        return self._make_record(
            source_table=src_table,
            source_col=src_col,
            target_table=target_table,
            target_col=target_col,
            expression=expr_logic,
            join_condition=join_condition,
            source_filter=source_filter,
        )

    # ------------------------------------------------------------------
    # Expression resolution
    # ------------------------------------------------------------------

    def _resolve_source(self, expr, alias_map: dict):
        """
        Recursively resolve a SELECT expression to (source_table, source_column, expression_logic).
        Always returns exactly 3 values.
        """
        if isinstance(expr, exp.Column):
            col_name = expr.name
            table_ref = expr.table  # alias or table name qualifier

            if table_ref:
                actual = alias_map.get(table_ref.lower(), table_ref.upper())
                return actual, col_name, ""
            else:
                tables = list(alias_map.values())
                if len(tables) == 1:
                    return tables[0], col_name, ""
                elif not tables:
                    return "UNKNOWN", col_name, ""
                else:
                    # Ambiguous — multiple source tables, can't determine without schema info
                    return "Derived", col_name, col_name

        elif isinstance(expr, exp.Literal):
            val = f"'{expr.this}'" if expr.is_string else str(expr.this)
            return "Hardcoded", val, str(expr)

        elif isinstance(expr, exp.Null):
            return "Hardcoded", "NULL", "NULL"

        elif isinstance(expr, exp.Star):
            src = list(alias_map.values())[0] if alias_map else "UNKNOWN"
            return src, "*", "*"

        elif isinstance(expr, exp.Cast):
            # CAST(col AS type) — lineage follows the inner expression
            return self._resolve_source(expr.this, alias_map)

        elif isinstance(expr, (exp.Coalesce, exp.If, exp.Case)):
            # For COALESCE/IIF/CASE: use the first column-bearing argument as the "primary" source
            for child in expr.walk():
                if isinstance(child, exp.Column) and not isinstance(child, exp.Star):
                    src_t, src_c, _ = self._resolve_source(child, alias_map)
                    return src_t, src_c, expr.sql()
            return "Derived", expr.sql()[:60], expr.sql()

        else:
            # All other expressions: functions, arithmetic, CASE, subqueries, etc.
            expr_sql = expr.sql()
            inner_cols = [c for c in expr.find_all(exp.Column) if not isinstance(c, exp.Star)]

            if not inner_cols:
                return "Derived", expr_sql[:60], expr_sql

            # Collect resolved tables for each inner column
            resolved_tables = []
            for col in inner_cols:
                tbl = col.table
                if tbl:
                    resolved_tables.append(alias_map.get(tbl.lower(), tbl.upper()))
                elif len(alias_map) == 1:
                    resolved_tables.append(list(alias_map.values())[0])

            if resolved_tables:
                src_table = Counter(resolved_tables).most_common(1)[0][0]
            elif len(alias_map) == 1:
                src_table = list(alias_map.values())[0]
            else:
                src_table = "Derived"

            return src_table, inner_cols[0].name, expr_sql

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_alias_map(self, select) -> dict:
        """Build {alias_lower: ACTUAL_TABLE_NAME} from FROM and JOINs."""
        alias_map = {}
        # sqlglot uses both "from" and "from_" depending on version/context
        from_clause = select.args.get("from") or select.args.get("from_")
        joins = select.args.get("joins") or []

        table_exprs = []
        if from_clause and from_clause.this:
            table_exprs.append(from_clause.this)
        for join in joins:
            if join.this:
                table_exprs.append(join.this)

        for te in table_exprs:
            if isinstance(te, exp.Table):
                actual = self._get_table_name(te)
                alias_map[actual.lower()] = actual  # table name maps to itself
                if te.alias:
                    alias_map[te.alias.lower()] = actual  # alias maps to actual name
            elif isinstance(te, exp.Alias):
                # Older sqlglot versions may wrap in Alias
                actual = self._get_table_name(te.this)
                alias_map[actual.lower()] = actual
                if te.alias:
                    alias_map[te.alias.lower()] = actual
            elif isinstance(te, exp.Subquery):
                label = te.alias.upper() if te.alias else "SUBQUERY"
                alias_map[label.lower()] = label

        return alias_map

    def _extract_join_conditions(self, select) -> str:
        """Collect all JOIN ON conditions as a single string."""
        conditions = []
        for join in (select.args.get("joins") or []):
            on = join.args.get("on")
            if on:
                conditions.append(on.sql())
        return " AND ".join(conditions)

    def _extract_filter(self, select) -> str:
        """Return WHERE clause as a string."""
        where = select.args.get("where") or select.args.get("where_")
        if where:
            inner = where.this if hasattr(where, "this") else where
            try:
                return inner.sql() if inner else ""
            except Exception:
                return ""
        return ""

    def _get_table_name(self, table_expr) -> str:
        """Return the fully-qualified table name (SCHEMA.TABLE or TABLE) in upper case."""
        if table_expr is None:
            return "UNKNOWN"
        if isinstance(table_expr, exp.Table):
            parts = [p for p in [table_expr.db, table_expr.name] if p]
            return ".".join(parts).upper() if parts else "UNKNOWN"
        elif isinstance(table_expr, exp.Alias):
            return self._get_table_name(table_expr.this)
        elif hasattr(table_expr, "name") and table_expr.name:
            return table_expr.name.upper()
        return "UNKNOWN"

    def _expr_to_col_name(self, expr) -> Optional[str]:
        """Derive a column name from an unaliased expression."""
        if isinstance(expr, exp.Column):
            return expr.name
        if isinstance(expr, exp.Star):
            return "*"
        if hasattr(expr, "alias") and expr.alias:
            return expr.alias
        return None

    def _make_record(
        self,
        source_table: str,
        source_col: str,
        target_table: str,
        target_col: str,
        expression: str = "",
        join_condition: str = "",
        source_filter: str = "",
        sql_override: str = "",
    ) -> dict:
        """Produce a 22-column lineage dict matching InformaticaLineageParser output."""
        src_is_special = source_table in _SPECIAL_SOURCES or not source_table
        return {
            "Mapping_Name": self.mapping_name,
            "Source_Table_INSERT": source_table or "",
            "Source_Table_Business_Name": "" if src_is_special else format_business_name(source_table),
            "Source_Column_INSERT": source_col or "",
            "Source_Column_Business_Name": format_business_name(source_col) if source_col else "",
            "Source_Table_UPDATE": "",
            "Source_Column_UPDATE": "",
            "Source_Datatype": "",
            "Source_Key": "",
            "Target_Table": target_table or "",
            "Target_Table_Business_Name": format_business_name(target_table) if target_table else "",
            "Target_Column": target_col or "",
            "Target_Column_Business_Name": format_business_name(target_col) if target_col else "",
            "Target_Datatype": "",
            "Target_Key": "",
            "Expression_Logic": expression or "",
            "Lookup_Condition": join_condition or "",
            "SQL_Override": sql_override or "",
            "Source_Join_Condition": join_condition or "",
            "Source_Filter": source_filter or "",
            "Transformations_INSERT": "",
            "Transformations_UPDATE": "",
        }

    # ------------------------------------------------------------------
    # Tier 2 — LLM fallback
    # ------------------------------------------------------------------

    def _parse_with_llm(self, raw_sql: str, call_llm_fn: Callable, llm_model: str) -> list:
        """
        Ask the LLM to extract field-level lineage from SQL that sqlglot could not parse.
        Returns a list of 22-column lineage dicts.
        """
        prompt = f"""You are a data lineage expert. Analyze the SQL below and extract field-level lineage.

SQL:
```sql
{raw_sql[:5000]}
```

Return a JSON array. Each element is one target column mapping:
[
  {{
    "source_table": "TABLE_NAME",
    "source_column": "COLUMN_NAME",
    "target_table": "TARGET_TABLE_NAME",
    "target_column": "TARGET_COLUMN_NAME",
    "expression": "transformation expression or empty string",
    "join_condition": "JOIN ON condition or empty string",
    "source_filter": "WHERE clause or empty string"
  }}
]

Rules:
- Direct column copy: expression = ""
- Function / expression: expression = the full SQL expression
- Hardcoded literal: source_table = "Hardcoded", source_column = the literal value
- System-generated (CURRENT_TIMESTAMP, GETDATE, etc.): source_table = "SYSTEM"
- Calculated with no clear single source: source_table = "Derived"
- For SELECT *, create one record per known column if possible, or one record with source_column = "*"
- Return ONLY the JSON array. No explanation, no markdown fences."""

        try:
            response, error = call_llm_fn(llm_model, prompt, max_tokens=4096)
            if error or not response:
                return []

            # Extract JSON array from response (strip any surrounding markdown)
            json_match = re.search(r"\[[\s\S]*\]", response)
            if not json_match:
                return []

            data = json.loads(json_match.group())
            records = []
            for item in data:
                if not isinstance(item, dict):
                    continue
                records.append(self._make_record(
                    source_table=item.get("source_table", ""),
                    source_col=item.get("source_column", ""),
                    target_table=item.get("target_table", ""),
                    target_col=item.get("target_column", ""),
                    expression=item.get("expression", ""),
                    join_condition=item.get("join_condition", ""),
                    source_filter=item.get("source_filter", ""),
                    sql_override=raw_sql[:500],
                ))
            return records

        except Exception:
            return []

    # ------------------------------------------------------------------
    # Metadata population (mirrors InformaticaLineageParser attributes)
    # ------------------------------------------------------------------

    def _build_metadata_from_records(self, records: list):
        """
        Populate self.sources, self.targets, self.mappings, and the *_data lists
        from the collected lineage records so that all downstream generators
        (SQL generator, dbt, data models) work without modification.
        """
        seen_src_cols: set = set()
        seen_tgt_cols: set = set()

        for rec in records:
            src_table = rec["Source_Table_INSERT"]
            tgt_table = rec["Target_Table"]
            src_col = rec["Source_Column_INSERT"]
            tgt_col = rec["Target_Column"]
            mname = rec["Mapping_Name"]

            # --- sources ---
            if src_table and src_table not in _SPECIAL_SOURCES:
                if src_table not in self.sources:
                    self.sources[src_table] = {"name": src_table, "fields": {}}
                if src_col:
                    self.sources[src_table]["fields"][src_col] = {
                        "datatype": "", "precision": "", "scale": "",
                        "nullable": "Y", "keytype": "",
                    }
                    key = (src_table, src_col)
                    if key not in seen_src_cols:
                        seen_src_cols.add(key)
                        self.sources_data.append({
                            "Source_Name": src_table,
                            "Field_Name": src_col,
                            "Datatype": "",
                            "Precision": "",
                            "Scale": "",
                            "Nullable": "Y",
                            "Key_Type": "",
                            "Business_Name": rec["Source_Column_Business_Name"],
                            "Description": "",
                        })

            # --- targets ---
            if tgt_table:
                if tgt_table not in self.targets:
                    self.targets[tgt_table] = {"name": tgt_table, "fields": {}}
                if tgt_col:
                    self.targets[tgt_table]["fields"][tgt_col] = {
                        "datatype": "", "precision": "", "scale": "",
                        "nullable": "Y", "keytype": "",
                    }
                    key = (tgt_table, tgt_col)
                    if key not in seen_tgt_cols:
                        seen_tgt_cols.add(key)
                        self.targets_data.append({
                            "Target_Name": tgt_table,
                            "Field_Name": tgt_col,
                            "Datatype": "",
                            "Precision": "",
                            "Scale": "",
                            "Nullable": "Y",
                            "Key_Type": "",
                            "Business_Name": rec["Target_Column_Business_Name"],
                            "Description": "",
                        })

            # --- mappings ---
            if mname not in self.mappings:
                self.mappings[mname] = {
                    "name": mname,
                    "instances": {},
                    "target_instances": {},
                    "source_instances": {},
                    "transformations": {},
                    "connectors": [],
                    "field_expressions": {},
                    "expression_logic": {},
                    "sq_field_sources": {},
                    "transformation_fields": {},
                }
            if src_table and src_table not in _SPECIAL_SOURCES:
                self.mappings[mname]["source_instances"][src_table] = src_table
            if tgt_table:
                self.mappings[mname]["target_instances"][tgt_table] = tgt_table
