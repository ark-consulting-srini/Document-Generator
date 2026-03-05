"""
SQL Generator Tab UI for T - TDD Generator

UI components for the XML to SQL conversion feature.
Supports Snowflake and Databricks target platforms.
"""

import streamlit as st
import pandas as pd
from typing import Dict, Optional

from generators.sql_generator import (
    SQLGenerator,
    generate_ddl_from_sources,
    generate_ddl_from_targets,
    convert_informatica_expression,
)
from utils.export_utils import create_sql_export


def render_sql_generator_tab(sources_dict: Dict, targets_dict: Dict,
                             lineage_df: pd.DataFrame, mappings_dict: Dict = None):
    """
    Render the SQL Generator tab.

    Args:
        sources_dict: Dictionary of source definitions
        targets_dict: Dictionary of target definitions
        lineage_df: DataFrame with lineage data
        mappings_dict: Dictionary of mapping structures
    """
    target_platform = st.session_state.get('target_platform', 'snowflake')
    platform_label = "Databricks Spark SQL" if target_platform == 'databricks' else "Snowflake SQL"

    st.markdown(f"### 🔄 XML to {platform_label} Conversion")
    st.markdown(f"Convert Informatica XML mappings to {platform_label} statements")

    # Initialize SQL Generator with target platform
    sql_gen = SQLGenerator(sources_dict, targets_dict, lineage_df, mappings_dict,
                           target_platform=target_platform)

    # Configuration section
    st.markdown("---")
    st.markdown("#### ⚙️ Configuration")

    col1, col2, col3 = st.columns(3)

    with col1:
        source_schema = st.text_input(
            "Source Schema",
            value=st.session_state.get('sql_source_schema', 'SOURCE'),
            key="sql_source_schema_input"
        )
        st.session_state.sql_source_schema = source_schema

    with col2:
        target_schema = st.text_input(
            "Target Schema",
            value=st.session_state.get('sql_target_schema', 'TARGET'),
            key="sql_target_schema_input"
        )
        st.session_state.sql_target_schema = target_schema

    with col3:
        target_tables = list(targets_dict.keys()) if targets_dict else ['No targets found']
        selected_target = st.selectbox(
            "Target Table (for DML)",
            options=target_tables,
            key="sql_selected_target"
        )

    st.markdown("---")

    # SQL Type Selection — label changes based on platform
    st.markdown("#### 📋 SQL Generation Options")

    proc_label = "Full ETL - Python Notebook" if target_platform == 'databricks' else "Full ETL - Stored Procedure"

    sql_type = st.radio(
        "Select SQL Type to Generate",
        options=[
            "DDL - CREATE TABLE (Sources)",
            "DDL - CREATE TABLE (Targets)",
            "DDL - All Tables",
            "DML - INSERT Statement",
            "DML - MERGE Statement",
            proc_label
        ],
        index=0,
        key="sql_type_radio",
        horizontal=True
    )

    # Additional options based on SQL type
    if "MERGE" in sql_type:
        st.markdown("##### Merge Key Configuration")

        if selected_target in targets_dict:
            target_fields = list(targets_dict[selected_target].get('fields', {}).keys())

            pk_cols = [f for f, info in targets_dict[selected_target].get('fields', {}).items()
                      if 'PRIMARY' in str(info.get('keytype', '')).upper()]

            merge_keys = st.multiselect(
                "Select Merge Key Columns",
                options=target_fields,
                default=pk_cols if pk_cols else (target_fields[:1] if target_fields else []),
                help="Columns used to match records for UPDATE vs INSERT",
                key="merge_key_selection"
            )
        else:
            merge_keys = []
            st.warning("No target table selected")

    if "Stored Procedure" in sql_type or "Python Notebook" in sql_type:
        col1, col2 = st.columns(2)
        with col1:
            proc_name = st.text_input(
                "Procedure / Notebook Name",
                value=f"SP_LOAD_{selected_target}" if selected_target else "SP_LOAD_DATA",
                key="proc_name_input"
            )
        with col2:
            proc_schema = st.text_input(
                "Procedure Schema",
                value="ETL",
                key="proc_schema_input"
            )

        proc_operation = st.radio(
            "ETL Operation",
            options=["INSERT", "MERGE"],
            index=0,
            horizontal=True,
            key="proc_operation_radio"
        )

    st.markdown("---")

    # Generate button
    if st.button("🚀 Generate SQL", type="primary", key="generate_sql_btn"):
        with st.spinner("Generating SQL..."):
            try:
                sql_result = ""

                if sql_type == "DDL - CREATE TABLE (Sources)":
                    sql_result = sql_gen.generate_source_ddl(schema=source_schema)

                elif sql_type == "DDL - CREATE TABLE (Targets)":
                    sql_result = sql_gen.generate_target_ddl(schema=target_schema)

                elif sql_type == "DDL - All Tables":
                    sql_result = sql_gen.generate_all_ddl(
                        source_schema=source_schema,
                        target_schema=target_schema
                    )

                elif sql_type == "DML - INSERT Statement":
                    sql_result = sql_gen.generate_insert_sql(
                        target_table=selected_target,
                        source_schema=source_schema,
                        target_schema=target_schema
                    )

                elif sql_type == "DML - MERGE Statement":
                    if not merge_keys:
                        st.error("Please select at least one merge key column")
                    else:
                        sql_result = sql_gen.generate_merge_sql(
                            target_table=selected_target,
                            merge_keys=merge_keys,
                            source_schema=source_schema,
                            target_schema=target_schema
                        )

                elif "Stored Procedure" in sql_type or "Python Notebook" in sql_type:
                    sql_result = sql_gen.generate_stored_procedure(
                        procedure_name=proc_name,
                        target_table=selected_target,
                        operation=proc_operation,
                        schema=proc_schema
                    )

                if sql_result:
                    st.session_state.sql_generator_results = {
                        'sql': sql_result,
                        'type': sql_type,
                        'target': selected_target
                    }
                    st.success("✅ SQL generated successfully!")

            except Exception as e:
                st.error(f"Error generating SQL: {str(e)}")

    # Display results
    if st.session_state.get('sql_generator_results'):
        results = st.session_state.sql_generator_results

        st.markdown("---")
        st.markdown("#### 📄 Generated SQL")
        st.markdown(f"**Type**: {results.get('type', 'Unknown')} | **Platform**: {platform_label}")

        lang = 'python' if target_platform == 'databricks' and 'Notebook' in results.get('type', '') else 'sql'
        st.code(results.get('sql', ''), language=lang)

        col1, col2, col3 = st.columns([1, 1, 2])

        with col1:
            sql_content = results.get('sql', '')
            ext = '.py' if lang == 'python' else '.sql'
            filename = f"generated_{results.get('type', 'sql').replace(' ', '_').lower()}{ext}"

            st.download_button(
                label="📥 Download SQL",
                data=sql_content,
                file_name=filename,
                mime="text/plain",
                key="download_sql_btn"
            )

        with col2:
            if st.button("📋 Copy to Clipboard", key="copy_sql_btn"):
                st.info("Use Ctrl+C / Cmd+C to copy from the code block above")

        with col3:
            if st.button("🗑️ Clear Results", key="clear_sql_btn"):
                st.session_state.sql_generator_results = {}
                st.rerun()

    # Expression Converter section
    st.markdown("---")
    st.markdown("#### 🔄 Expression Converter")
    st.markdown(f"Convert individual Informatica expressions to {platform_label}")

    if target_platform == 'databricks':
        placeholder = "IIF(ISNULL(FIELD1), 'N/A', FIELD1)  →  IF(COALESCE(FIELD1) IS NULL, 'N/A', FIELD1)"
    else:
        placeholder = "IIF(ISNULL(FIELD1), 'N/A', FIELD1)"

    expr_input = st.text_area(
        "Enter Informatica Expression",
        height=100,
        placeholder=placeholder,
        key="expr_converter_input"
    )

    if st.button("Convert Expression", key="convert_expr_btn"):
        if expr_input:
            converted = convert_informatica_expression(expr_input, target_platform)

            st.markdown(f"**{platform_label}:**")
            st.code(converted, language='sql')

            with st.expander("📝 Conversion Notes"):
                if target_platform == 'databricks':
                    st.markdown("""
                    **Common Conversions (Databricks):**
                    - `IIF()` → `IF()`
                    - `SYSDATE` → `CURRENT_TIMESTAMP()`
                    - `ISNULL(x, y)` → `COALESCE(x, y)`
                    - `NVL(x, y)` → `COALESCE(x, y)`
                    - `INSTR(str, substr)` → `LOCATE(substr, str)`
                    - `ADD_TO_DATE()` → `DATE_ADD()` / `INTERVAL`
                    - `DATE_DIFF()` → `DATEDIFF(end, start)` (days only)
                    - `TO_INTEGER(x)` → `CAST(x AS INT)`
                    - `TO_FLOAT(x)` → `CAST(x AS DOUBLE)`
                    - `SEQ.NEXTVAL` → `MONOTONICALLY_INCREASING_ID()`
                    - `ERROR()` / `ABORT()` → `NULL`
                    """)
                else:
                    st.markdown("""
                    **Common Conversions (Snowflake):**
                    - `IIF()` → `IFF()`
                    - `SYSDATE` → `CURRENT_TIMESTAMP()`
                    - `ISNULL(x, y)` → `NVL(x, y)`
                    - `INSTR(str, substr)` → `POSITION(substr IN str)`
                    - `ADD_TO_DATE()` → `DATEADD()`
                    - `DATE_DIFF()` → `DATEDIFF()`
                    - `ERROR()` / `ABORT()` → `NULL`
                    """)
        else:
            st.warning("Please enter an expression to convert")

    # Help section
    with st.expander("ℹ️ SQL Generator Help"):
        st.markdown(f"""
        ### SQL Generation Types ({platform_label})

        | Type | Description |
        |------|-------------|
        | **DDL - Sources** | CREATE TABLE statements for source tables |
        | **DDL - Targets** | CREATE TABLE statements for target tables |
        | **DDL - All** | All CREATE TABLE statements |
        | **DML - INSERT** | INSERT INTO ... SELECT statement |
        | **DML - MERGE** | MERGE statement for upsert operations |
        | **{proc_label.split(' - ')[-1] if 'proc_label' in dir() else 'ETL'}** | Complete ETL with error handling |

        ### Expression Conversion

        The expression converter handles common Informatica functions:
        - Conditional: IIF, DECODE
        - String: LTRIM, RTRIM, SUBSTR, REPLACE
        - Date: TO_DATE, TO_CHAR, ADD_TO_DATE, DATE_DIFF
        - Null handling: ISNULL, NVL

        ### Best Practices

        1. Review generated SQL before executing
        2. Validate datatype mappings
        3. Check for NULL handling
        4. Test with sample data first
        """)
