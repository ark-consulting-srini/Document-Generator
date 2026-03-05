"""
dbt Model Generator Tab UI for T - TDD Generator

UI components for generating dbt bronze + silver models from Informatica lineage.
"""

import json
import streamlit as st
import pandas as pd
from typing import Dict, List

from generators.dbt_generator import DbtGenerator
from utils.platform_utils import call_llm


def _build_llm_unconnected_resolver(model_name: str):
    """Build a callable that batch-resolves UNCONNECTED columns via LLM."""

    def resolver(columns: List[str]) -> Dict[str, str]:
        prompt = (
            "Given these target table column names that have no source mapping "
            "(UNCONNECTED) in an Informatica ETL process, suggest the most "
            "appropriate default SQL expression for each in Snowflake SQL.\n\n"
            "Common patterns:\n"
            "- Audit timestamps (INSERT_TS, UPDATE_TS) -> CURRENT_TIMESTAMP()\n"
            "- Audit users (INSERT_BY, UPDATE_BY) -> CURRENT_USER()\n"
            "- SCD2 dates (EFF_START_DT, EFF_END_DT) -> CURRENT_DATE() or '9999-12-31'::DATE\n"
            "- Flags (IS_CURRENT, IS_ACTIVE, IS_DELETED) -> TRUE or FALSE\n"
            "- Version numbers -> 1\n"
            "- Source system identifiers -> 'INFORMATICA'\n"
            "- If you cannot determine a reasonable default, use NULL\n\n"
            "Columns to resolve:\n"
            + "\n".join(f"- {col}" for col in columns)
            + "\n\nRespond with ONLY a JSON object mapping column name to SQL "
            "expression. Example:\n"
            '{"INSERT_TS": "CURRENT_TIMESTAMP()", "IS_CURRENT": "TRUE"}'
        )
        response, error = call_llm(model_name, prompt)
        if error or not response:
            return {}
        try:
            text = response.strip()
            start = text.find('{')
            end = text.rfind('}') + 1
            if start >= 0 and end > start:
                return json.loads(text[start:end])
        except (json.JSONDecodeError, ValueError):
            pass
        return {}

    return resolver


def render_dbt_generator_tab(sources_dict: Dict, targets_dict: Dict,
                              lineage_df: pd.DataFrame, mappings_dict: Dict = None):
    """
    Render the dbt Model Generator tab.

    Args:
        sources_dict: Dictionary of source definitions
        targets_dict: Dictionary of target definitions
        lineage_df: DataFrame with lineage data
        mappings_dict: Dictionary of mapping structures
    """
    st.markdown("### dbt Model Generator")
    st.markdown("Generate dbt **bronze** (staging) and **silver** (transform) layer models from Informatica lineage")

    st.markdown("---")
    st.markdown("#### Configuration")

    col1, col2, col3 = st.columns(3)

    with col1:
        raw_source_name = st.text_input(
            "dbt Source Name",
            value=st.session_state.get('dbt_raw_source_name', 'raw'),
            help="Name used in {{ source('name', 'TABLE') }}",
            key="dbt_source_name_input",
        )
        st.session_state.dbt_raw_source_name = raw_source_name

    with col2:
        bronze_mat = st.selectbox(
            "Bronze Materialization",
            options=['view', 'incremental', 'table'],
            index=0,
            help="How bronze models are materialized in dbt",
            key="dbt_bronze_mat_select",
        )

    with col3:
        silver_mat = st.selectbox(
            "Silver Materialization",
            options=['table', 'incremental', 'view'],
            index=0,
            help="How silver models are materialized in dbt",
            key="dbt_silver_mat_select",
        )

    # UNCONNECTED column resolution options
    st.markdown("#### UNCONNECTED Column Resolution")
    st.caption(
        "UNCONNECTED columns have no source mapping in Informatica. "
        "Tier 1 uses pattern matching (INSERT_TS, UPDATE_TS, etc.) to assign "
        "default expressions. Tier 2 uses an LLM for ambiguous column names."
    )
    ucol1, ucol2 = st.columns(2)
    with ucol1:
        resolve_unconnected = st.checkbox(
            "Resolve UNCONNECTED columns (Tier 1: pattern matching)",
            value=True,
            key="dbt_resolve_unconnected",
            help="Automatically assign default values like CURRENT_TIMESTAMP() to audit/housekeeping columns",
        )
    with ucol2:
        use_llm_tier2 = st.checkbox(
            "Use LLM for unresolved columns (Tier 2)",
            value=False,
            key="dbt_llm_tier2",
            help="Send ambiguous column names to an LLM for intelligent resolution",
            disabled=not resolve_unconnected,
        )
    if use_llm_tier2 and resolve_unconnected:
        llm_model = st.session_state.get('selected_model', 'claude-4-sonnet')
        st.caption(f"LLM model: **{llm_model}**")

    # Advanced configuration
    with st.expander("Advanced Configuration"):
        adv1, adv2 = st.columns(2)
        with adv1:
            bronze_prefix = st.text_input(
                "Bronze Model Prefix",
                value=st.session_state.get('dbt_bronze_prefix', 'brz'),
                help="Prefix for bronze model filenames (e.g., brz_table_name.sql)",
                key="dbt_bronze_prefix_input",
            )
            bronze_layer = st.text_input(
                "Bronze Layer Directory",
                value=st.session_state.get('dbt_bronze_layer', 'bronze'),
                help="Directory name under models/ for bronze layer",
                key="dbt_bronze_layer_input",
            )
        with adv2:
            silver_prefix = st.text_input(
                "Silver Model Prefix",
                value=st.session_state.get('dbt_silver_prefix', 'slv'),
                help="Prefix for silver model filenames (e.g., slv_table_name.sql)",
                key="dbt_silver_prefix_input",
            )
            silver_layer = st.text_input(
                "Silver Layer Directory",
                value=st.session_state.get('dbt_silver_layer', 'silver'),
                help="Directory name under models/ for silver layer",
                key="dbt_silver_layer_input",
            )
        target_schema = st.text_input(
            "Target Schema (optional)",
            value=st.session_state.get('dbt_target_schema', ''),
            help="If set, adds schema='...' to dbt config blocks",
            key="dbt_target_schema_input",
        )
        dbt_tags_input = st.text_input(
            "dbt Tags (comma-separated, optional)",
            value=st.session_state.get('dbt_tags', ''),
            help="Tags added to dbt config blocks, e.g., 'informatica, etl'",
            key="dbt_tags_input",
        )
        dbt_tags = [t.strip() for t in dbt_tags_input.split(',') if t.strip()] if dbt_tags_input else []

    st.markdown("---")

    # Preview section
    temp_gen = DbtGenerator(sources_dict, targets_dict, lineage_df, mappings_dict)
    source_tables = temp_gen.get_source_tables()
    target_tables = temp_gen.get_target_tables()

    with st.expander(f"Preview: {len(source_tables)} {bronze_layer} + {len(target_tables)} {silver_layer} models"):
        pcol1, pcol2 = st.columns(2)
        with pcol1:
            st.markdown(f"**{bronze_layer.title()} (source tables):**")
            for tbl in source_tables:
                st.markdown(f"- `{bronze_prefix}_{tbl.lower()}.sql`")
        with pcol2:
            st.markdown(f"**{silver_layer.title()} (target tables):**")
            for tbl in target_tables:
                st.markdown(f"- `{silver_prefix}_{tbl.lower()}.sql`")

    st.markdown("---")

    # Generate button
    if st.button("Generate dbt Models", type="primary", key="generate_dbt_btn"):
        with st.spinner("Generating dbt models..."):
            try:
                # Build UNCONNECTED resolver based on user config
                uc_resolver = None
                if resolve_unconnected:
                    if use_llm_tier2:
                        llm_model = st.session_state.get('selected_model', 'claude-4-sonnet')
                        uc_resolver = _build_llm_unconnected_resolver(llm_model)
                    else:
                        # Tier 1 only — pass a no-op resolver so pattern matching
                        # still runs (it's built into generate_silver_model), but
                        # no LLM fallback is invoked.
                        uc_resolver = None

                dbt_gen = DbtGenerator(
                    sources_dict, targets_dict, lineage_df, mappings_dict,
                    raw_source_name=raw_source_name,
                    bronze_materialization=bronze_mat,
                    silver_materialization=silver_mat,
                    resolve_unconnected=resolve_unconnected,
                    unconnected_resolver=uc_resolver,
                    bronze_prefix=bronze_prefix,
                    silver_prefix=silver_prefix,
                    bronze_layer=bronze_layer,
                    silver_layer=silver_layer,
                    target_schema=target_schema,
                    dbt_tags=dbt_tags,
                )
                files = dbt_gen.generate_all()
                zip_buffer = dbt_gen.generate_zip()
                validation_issues = dbt_gen.validate_models(files)

                st.session_state.dbt_generator_results = {
                    'files': files,
                    'zip': zip_buffer,
                    'validation': validation_issues,
                }

                errors = sum(1 for i in validation_issues if i['severity'] == 'error')
                warnings = sum(1 for i in validation_issues if i['severity'] == 'warning')
                if errors:
                    st.warning(f"Generated {len(files)} files with {errors} error(s) and {warnings} warning(s)")
                else:
                    st.success(f"Generated {len(files)} dbt model files!")

            except Exception as e:
                st.error(f"Error generating dbt models: {str(e)}")

    # Display results
    if st.session_state.get('dbt_generator_results'):
        results = st.session_state.dbt_generator_results
        files = results.get('files', {})

        st.markdown("---")
        st.markdown("#### Generated Files")

        # Download button
        col1, col2 = st.columns([1, 3])
        with col1:
            st.download_button(
                label="Download ZIP",
                data=results['zip'],
                file_name="dbt_models.zip",
                mime="application/zip",
                key="download_dbt_zip",
            )
        with col2:
            if st.button("Clear Results", key="clear_dbt_btn"):
                st.session_state.dbt_generator_results = {}
                st.rerun()

        # Validation results
        validation = results.get('validation', [])
        if validation:
            errors = [i for i in validation if i['severity'] == 'error']
            warnings = [i for i in validation if i['severity'] == 'warning']
            infos = [i for i in validation if i['severity'] == 'info']

            if errors or warnings:
                with st.expander(
                    f"Validation: {len(errors)} error(s), {len(warnings)} warning(s)",
                    expanded=bool(errors),
                ):
                    for issue in errors:
                        st.error(f"**{issue['file']}**: {issue['message']}")
                    for issue in warnings:
                        st.warning(f"**{issue['file']}**: {issue['message']}")
                    for issue in infos:
                        st.info(issue['message'])
            else:
                st.success("All models passed validation checks")

        # File tree
        st.markdown(f"**{len(files)} files** in `dbt_models/`:")

        # Group by directory (use dynamic layer names)
        brz_dir = f"/{bronze_layer}/"
        slv_dir = f"/{silver_layer}/"
        bronze_files = {k: v for k, v in files.items() if brz_dir in k}
        silver_files = {k: v for k, v in files.items() if slv_dir in k}
        root_files = {k: v for k, v in files.items() if brz_dir not in k and slv_dir not in k}

        # Bronze
        if bronze_files:
            with st.expander(f"models/{bronze_layer}/ ({len(bronze_files)} files)", expanded=True):
                for path in sorted(bronze_files.keys()):
                    content = bronze_files[path]
                    fname = path.split('/')[-1]
                    lang = 'yaml' if fname.endswith('.yml') else 'sql'
                    with st.expander(f"`{fname}` ({len(content)} chars)"):
                        st.code(content, language=lang)

        # Silver
        if silver_files:
            with st.expander(f"models/{silver_layer}/ ({len(silver_files)} files)", expanded=True):
                for path in sorted(silver_files.keys()):
                    content = silver_files[path]
                    fname = path.split('/')[-1]
                    lang = 'yaml' if fname.endswith('.yml') else 'sql'
                    with st.expander(f"`{fname}` ({len(content)} chars)"):
                        st.code(content, language=lang)

        # Root files
        if root_files:
            with st.expander(f"Root files ({len(root_files)} files)"):
                for path in sorted(root_files.keys()):
                    content = root_files[path]
                    fname = path.split('/')[-1]
                    lang = 'yaml' if fname.endswith('.yml') else 'sql'
                    with st.expander(f"`{fname}` ({len(content)} chars)"):
                        st.code(content, language=lang)

    # Help section
    with st.expander("dbt Models Help"):
        st.markdown("""
        ### Generated Structure

        ```
        dbt_models/
        ├── models/
        │   ├── bronze/          # Staging layer
        │   │   ├── brz_*.sql    # One per source table
        │   │   └── schema.yml   # Tests & descriptions
        │   └── silver/          # Transform layer
        │       ├── slv_*.sql    # One per target table
        │       └── schema.yml   # Tests & descriptions
        └── sources.yml          # Raw source definitions
        ```

        ### Bronze Layer
        - **1:1 copy** of each source table with light cleansing
        - `TRIM(NULLIF(col, ''))` on string columns
        - Adds `_loaded_at` and `_source` metadata columns
        - References raw sources via `{{ source('raw', 'TABLE') }}`

        ### Silver Layer
        - **One model per target table** with full transformation logic
        - References bronze models via `{{ ref('brz_*') }}`
        - Applies expression conversions (IIF→IFF, DECODE, etc.)
        - LEFT JOINs for lookup table enrichment
        - Includes source filter as WHERE clause

        ### Integration
        1. Extract the ZIP into your dbt project
        2. Set `raw_schema` variable in `dbt_project.yml`:
           ```yaml
           vars:
             raw_schema: YOUR_RAW_SCHEMA
           ```
        3. Run `dbt run --select bronze` then `dbt run --select silver`
        """)
