#!/usr/bin/env python3
"""
Technical Document Generator (TDG) v3.0

Main Streamlit application entry point.

Features:
1. Parse source files (Informatica XML, SQL, and more via plugin registry)
2. Generate Conceptual, Logical, and Physical data models
3. Multi-platform LLM integration (Anthropic Claude / Databricks / Snowflake Cortex)
4. LLM-powered Business Functional Requirements generation
5. XML to SQL Conversion (DDL, DML, Stored Procedures)
6. Workflow orchestration analysis (execution DAG, sessions, connections)

Usage:
    streamlit run streamlit_app.py
"""

import streamlit as st
import pandas as pd
import xml.etree.ElementTree as ET
import sys
import os

# Add the app directory to path for imports
app_dir = os.path.dirname(os.path.abspath(__file__))
if app_dir not in sys.path:
    sys.path.insert(0, app_dir)

# Import configuration
from config.settings import (
    APP_TITLE, APP_ICON, PAGE_LAYOUT, APP_VERSION,
    LLM_PLATFORM_AVAILABLE, GRAPHVIZ_AVAILABLE,
    get_llm_models, get_default_model,
)

# Import utilities (platform-agnostic)
from utils.platform_utils import get_connection, call_llm, get_platform_name
from utils.export_utils import create_excel_export, create_word_export

# Import generators
from generators.brd_generator import prepare_raw_xml_summary, prepare_lineage_summary
from generators.sql_generator import SQLGenerator
from generators.data_model_generator import generate_data_model
from generators.prompts import create_raw_xml_brd_prompt, create_workflow_tdd_prompt

# Import new generators
from generators.enhanced_sttm_generator import generate_enhanced_sttm, enhanced_sttm_to_csv
from generators.lineage_diagram_generator import generate_lineage_diagrams
from generators.conversion_report_generator import generate_conversion_report

# Import UI components
from ui.session_state import init_session_state, check_file_changes, clear_parsed_data
from ui.tabs.tab_sql_generator import render_sql_generator_tab
from ui.tabs.tab_notebook_overview import render_notebook_overview_tab

# Import parsers via registry (registers all built-in parsers on import)
import parsers  # noqa: F401 — triggers __init__.py registration
from parsers.registry import ParserRegistry
from parsers.informatica_parser import InformaticaLineageParser
from parsers.workflow_parser import InformaticaWorkflowParser

# Page configuration
st.set_page_config(
    page_title=APP_TITLE,
    page_icon=APP_ICON,
    layout=PAGE_LAYOUT
)

# ── Global UI styling ────────────────────────────────────────────────────────
st.markdown("""
<style>
/* ── Fonts ──────────────────────────────────────────────────────────────── */
html, body, [class*="css"] {
    font-family: 'Inter', 'Segoe UI', system-ui, sans-serif;
}

/* ── Branded header card ─────────────────────────────────────────────────── */
.tdg-header {
    background: linear-gradient(135deg, #0f2744 0%, #0d7377 100%);
    border-radius: 14px;
    padding: 28px 36px 24px;
    margin-bottom: 28px;
    color: white;
    box-shadow: 0 4px 20px rgba(13,115,119,0.25);
}
.tdg-header h1 {
    color: white !important;
    margin: 0 0 6px;
    font-size: 2rem;
    font-weight: 800;
    letter-spacing: -0.5px;
}
.tdg-header p {
    color: rgba(255,255,255,0.78);
    margin: 0;
    font-size: 1rem;
}

/* ── Feature cards (landing page) ───────────────────────────────────────── */
.feature-card {
    background: white;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 20px 18px;
    height: 100%;
    box-shadow: 0 2px 8px rgba(0,0,0,0.05);
    transition: box-shadow 0.2s, transform 0.2s;
}
.feature-card:hover {
    box-shadow: 0 6px 20px rgba(0,0,0,0.10);
    transform: translateY(-2px);
}
.feature-icon { font-size: 2rem; margin-bottom: 10px; line-height: 1; }
.feature-title { font-weight: 700; font-size: 0.92rem; color: #0f2744; margin-bottom: 5px; }
.feature-desc { font-size: 0.8rem; color: #64748b; line-height: 1.5; }

/* ── How-it-works steps ─────────────────────────────────────────────────── */
.how-step {
    display: flex;
    align-items: flex-start;
    gap: 14px;
    padding: 14px 0;
    border-bottom: 1px solid #f1f5f9;
}
.how-step:last-child { border-bottom: none; }
.step-num {
    min-width: 34px; height: 34px;
    background: linear-gradient(135deg, #0f2744, #0d7377);
    color: white;
    border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    font-weight: 800; font-size: 0.85rem;
    box-shadow: 0 2px 6px rgba(13,115,119,0.3);
}
.step-body { flex: 1; }
.step-title { font-weight: 700; font-size: 0.9rem; color: #1e293b; margin-bottom: 2px; }
.step-desc { font-size: 0.8rem; color: #64748b; }

/* ── File format badges ─────────────────────────────────────────────────── */
.fmt-badge {
    display: inline-block;
    background: #eff6ff;
    border: 1px solid #bfdbfe;
    border-radius: 6px;
    padding: 3px 10px;
    font-size: 0.75rem;
    font-weight: 700;
    color: #1e40af;
    margin: 3px 3px 0 0;
    letter-spacing: 0.3px;
}
.fmt-badge.green { background: #f0fdf4; border-color: #bbf7d0; color: #166534; }
.fmt-badge.purple { background: #faf5ff; border-color: #e9d5ff; color: #6b21a8; }
.fmt-badge.orange { background: #fff7ed; border-color: #fed7aa; color: #c2410c; }

/* ── LLM status pill ────────────────────────────────────────────────────── */
.status-pill {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 4px 12px; border-radius: 20px;
    font-size: 0.75rem; font-weight: 600;
}
.status-on  { background: #dcfce7; color: #166534; }
.status-off { background: #fee2e2; color: #991b1b; }

/* ── Tabs ───────────────────────────────────────────────────────────────── */
div[data-baseweb="tab-list"] {
    overflow-x: auto;
    flex-wrap: nowrap !important;
    scrollbar-width: thin;
    -webkit-overflow-scrolling: touch;
    gap: 4px;
    background: #f8fafc;
    border-radius: 10px;
    padding: 4px 6px;
    border: 1px solid #e2e8f0;
}
div[data-baseweb="tab-list"]::-webkit-scrollbar { height: 4px; }
div[data-baseweb="tab-list"]::-webkit-scrollbar-thumb {
    background: #cbd5e1; border-radius: 2px;
}
div[data-baseweb="tab-list"] button[data-baseweb="tab"] {
    white-space: nowrap;
    flex-shrink: 0;
    padding: 8px 18px;
    font-size: 13px;
    border-radius: 7px;
    font-weight: 500;
    transition: background 0.15s;
}

/* ── Sidebar ────────────────────────────────────────────────────────────── */
section[data-testid="stSidebar"] {
    background: #f8fafc;
    border-right: 1px solid #e2e8f0;
}
section[data-testid="stSidebar"] .block-container { padding-top: 1.5rem; }

/* ── Metric containers ──────────────────────────────────────────────────── */
[data-testid="metric-container"] {
    background: white;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    padding: 12px 16px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.04);
}

/* ── File uploader drop zone ────────────────────────────────────────────── */
[data-testid="stFileUploader"] {
    border: 2px dashed #93c5fd;
    border-radius: 12px;
    background: #eff6ff;
    padding: 6px;
    transition: border-color 0.2s, background 0.2s;
}
[data-testid="stFileUploader"]:hover {
    border-color: #0d7377;
    background: #ecfdf5;
}

/* ── Primary button ─────────────────────────────────────────────────────── */
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #0f2744 0%, #0d7377 100%) !important;
    border: none !important;
    border-radius: 8px !important;
    color: white !important;
    font-weight: 600 !important;
    padding: 10px 28px !important;
    box-shadow: 0 3px 12px rgba(13,115,119,0.3) !important;
    transition: transform 0.15s, box-shadow 0.15s !important;
}
.stButton > button[kind="primary"]:hover {
    transform: translateY(-1px) !important;
    box-shadow: 0 5px 18px rgba(13,115,119,0.4) !important;
}

/* ── Success / info / warning tweak ────────────────────────────────────── */
[data-testid="stNotification"] { border-radius: 10px; }
</style>
""", unsafe_allow_html=True)

# Initialize session state
init_session_state()


def main():
    """Main application function."""

    # ---- Sidebar ----
    with st.sidebar:
        st.markdown(f"**{APP_TITLE}**")
        st.caption(f"v{APP_VERSION}")

        # LLM availability indicator
        if LLM_PLATFORM_AVAILABLE:
            st.markdown('<span class="status-pill status-on">● AI Connected</span>',
                        unsafe_allow_html=True)
        else:
            st.markdown('<span class="status-pill status-off">○ AI Offline</span>',
                        unsafe_allow_html=True)
        st.markdown("---")

        st.markdown("### Target Platform")
        _platform_options = ["databricks_sql", "databricks_python", "postgresql", "mssql"]
        _platform_labels = {
            "databricks_sql":    "Databricks SQL",
            "databricks_python": "Databricks Python Notebooks",
            "postgresql":        "PostgreSQL",
            "mssql":             "Microsoft SQL (T-SQL)",
        }
        _current = st.session_state.get('target_platform', 'databricks_sql')
        _idx = _platform_options.index(_current) if _current in _platform_options else 0
        target_platform = st.radio(
            "Generate SQL for:",
            options=_platform_options,
            format_func=lambda x: _platform_labels[x],
            index=_idx,
            key="target_platform_radio",
        )
        st.session_state.target_platform = target_platform
        st.caption("Controls SQL dialect, type mappings, and expression conversion.")

        st.markdown("---")
        st.markdown("### Output Tabs")
        st.caption("Toggle optional output tabs.")
        st.checkbox("SQL Generator", key="enable_sql_generator", value=True,
                     help="Generate DDL/DML/Stored Procedures from lineage")
        if st.session_state.get('has_notebook_parsed'):
            st.checkbox("Notebook Overview", key="enable_notebook_overview", value=True,
                         help="View notebook cells as a readable document")

    st.markdown(f"""
<div class="tdg-header">
  <h1>{APP_ICON} {APP_TITLE}</h1>
  <p>Upload source files to automatically generate TDD, STTM, Data Models, SQL, and more.</p>
</div>
""", unsafe_allow_html=True)

    # File uploader (key changes on clear to reset the widget)
    uploaded_files = st.file_uploader(
        "Upload source files (XML, SQL, and more)",
        type=ParserRegistry.supported_extensions_for_uploader(),
        accept_multiple_files=True,
        help="Upload Informatica mapping/workflow XMLs, SQL files, or any other supported format",
        key=f"file_uploader_{st.session_state.uploader_key}",
    )

    # If sample data was loaded via sidebar button, use those files
    sample_files = st.session_state.pop('_sample_files_loaded', None)
    if sample_files and not uploaded_files:
        uploaded_files = sample_files
        st.info(f"Loaded {len(uploaded_files)} sample file(s) from sample_xmls/")

    # Check for file changes and clear token cache if files changed
    check_file_changes(uploaded_files)

    if uploaded_files:
        # Store files in session state for token estimation
        st.session_state.current_uploaded_files = uploaded_files

        st.success(f"✓ {len(uploaded_files)} file(s) uploaded")

        # Quick scan: warn about missing standalone mapping files
        _check_mapping_coverage(uploaded_files)

        # Configuration section
        render_configuration_section()

        # Auto-parse sample data, or manual parse button
        if sample_files:
            parse_and_process_files(uploaded_files)
        elif st.button("🚀 Parse & Generate Documentation", type="primary"):
            parse_and_process_files(uploaded_files)
    else:
        # Clear token estimate cache when no files
        if 'preview_token_estimate' in st.session_state:
            del st.session_state.preview_token_estimate
        st.session_state.current_uploaded_files = []

    # Show results if data has been parsed
    if st.session_state.get('parsed_data'):
        # Clear files button — lets user start fresh with different files
        if st.button("🗑️ Clear Existing Files & Start Over", key="clear_files_btn"):
            clear_parsed_data()
            st.session_state.last_uploaded_files = None
            st.session_state.current_uploaded_files = []
            st.session_state.uploader_key += 1  # resets file_uploader widget
            st.rerun()
        render_results_tabs()
    else:
        render_instructions()


def _load_sample_data():
    """Load sample XML files from sample_xmls/ directory for quick testing."""
    import io
    sample_dir = os.path.join(app_dir, 'sample_xmls')
    if not os.path.isdir(sample_dir):
        st.sidebar.error("sample_xmls/ directory not found")
        return

    xml_files = sorted(f for f in os.listdir(sample_dir) if f.upper().endswith('.XML'))
    if not xml_files:
        st.sidebar.error("No XML files found in sample_xmls/")
        return

    loaded = []
    for fname in xml_files:
        fpath = os.path.join(sample_dir, fname)
        with open(fpath, 'rb') as f:
            data = f.read()
        buf = io.BytesIO(data)
        buf.name = fname
        buf.size = len(data)
        loaded.append(buf)

    st.session_state['_sample_files_loaded'] = loaded
    st.session_state.last_uploaded_files = None  # force re-parse
    st.rerun()


def _check_mapping_coverage(uploaded_files):
    """Quick XML scan to warn about missing standalone mapping files immediately after upload.

    A "standalone mapping file" is any file that contains a MAPPING definition
    but is NOT the same file that defines the workflow referencing it.  This means
    uploading a *separate* copy of the mapping XML satisfies the check, even if the
    workflow XML also embeds the mapping.
    """
    # Per-file analysis: collect (has_workflow, referenced_mappings, contained_mappings)
    file_info = []
    for f in uploaded_files:
        try:
            f.seek(0)
            content = f.read()
            f.seek(0)
            root = ET.fromstring(content)

            has_workflow = len(root.findall('.//WORKFLOW')) > 0
            mapping_names = {m.get('NAME', '') for m in root.findall('.//MAPPING') if m.get('NAME')}
            session_refs = set()
            if has_workflow:
                for sess in root.findall('.//SESSION'):
                    mname = sess.get('MAPPINGNAME', '')
                    if mname:
                        session_refs.add(mname)

            file_info.append({
                'name': getattr(f, 'name', ''),
                'has_workflow': has_workflow,
                'mapping_names': mapping_names,
                'session_refs': session_refs,
            })
        except Exception:
            continue

    # Collect all mappings referenced by workflows
    all_referenced = set()
    for fi in file_info:
        if fi['has_workflow']:
            all_referenced.update(fi['session_refs'])

    if not all_referenced:
        return

    # A mapping is "covered" if it appears in more than one uploaded file.
    # If it only exists in a single file (the workflow XML that embeds it),
    # the user should upload the standalone mapping XML as well.
    uncovered = set()
    for mname in all_referenced:
        files_containing = [fi['name'] for fi in file_info
                            if mname in fi['mapping_names']]
        if len(files_containing) <= 1:
            uncovered.add(mname)

    if uncovered:
        st.info(
            f"**{len(uncovered)} mapping(s)** referenced by workflow sessions don't have "
            f"standalone mapping XML files uploaded: **{', '.join(sorted(uncovered))}**. "
            "Upload the individual mapping XML file(s) for a more complete analysis."
        )


def render_configuration_section():
    """Render the configuration section for document generation."""
    st.markdown("---")

    with st.expander("⚙️ Document Settings", expanded=True):
        col_ctx1, col_ctx2 = st.columns(2)
        with col_ctx1:
            st.session_state.main_business_context = st.text_area(
                "Business Context (Optional)",
                value=st.session_state.get('main_business_context', ''),
                height=80,
                placeholder="Enter business context...",
                key="biz_context_input"
            )

        with col_ctx2:
            st.session_state.main_additional_requirements = st.text_area(
                "Additional Requirements (Optional)",
                value=st.session_state.get('main_additional_requirements', ''),
                height=80,
                placeholder="Enter additional requirements...",
                key="addl_req_input"
            )

        # LLM Model Selection — primary always visible, advanced in nested expander
        available_llms = get_llm_models()
        llm_names = [m['name'] if isinstance(m, dict) else m for m in available_llms]

        col_m1, col_m2 = st.columns([3, 1])
        with col_m1:
            st.session_state.primary_model = st.selectbox(
                "🧠 LLM Model",
                llm_names,
                index=0,
                key="primary_model_select"
            )
        with col_m2:
            st.session_state.use_primary = st.checkbox("Enabled", value=True, key="use_primary_cb")

        with st.expander("Advanced: Multi-model settings"):
            col_m2, col_m3 = st.columns(2)
            with col_m2:
                st.session_state.secondary_model = st.selectbox(
                    "Secondary Model",
                    llm_names,
                    index=min(1, len(llm_names)-1),
                    key="secondary_model_select"
                )
                st.session_state.use_secondary = st.checkbox("Use Secondary", value=False, key="use_secondary_cb")

            with col_m3:
                st.session_state.consolidation_model = st.selectbox(
                    "Consolidation Model",
                    llm_names,
                    index=min(2, len(llm_names)-1),
                    key="consolidation_model_select"
                )
                st.session_state.use_consolidation = st.checkbox(
                    "Use Consolidation",
                    value=False,
                    disabled=not (st.session_state.get('use_primary') and st.session_state.get('use_secondary')),
                    key="use_consolidation_cb"
                )

    # Generation Mode
    st.markdown("### 📊 Generation Mode")
    
    # Quick token estimation by doing a lightweight parse of uploaded files
    estimated_tokens_full = 0
    estimated_tokens_optimized = 0
    
    # Get uploaded files from session state or file uploader
    uploaded_files = st.session_state.get('current_uploaded_files', [])
    
    if uploaded_files and 'preview_token_estimate' not in st.session_state:
        # Do a quick parse to estimate tokens accurately
        with st.spinner("Estimating token count..."):
            try:
                temp_sources = {}
                temp_targets = {}
                temp_mappings = {}
                
                for f in uploaded_files:
                    f.seek(0)
                    xml_content = f.read()
                    f.seek(0)  # Reset for later use
                    
                    # Quick parse using the same parser
                    try:
                        temp_parser = InformaticaLineageParser(xml_content)
                        temp_parser.parse_all()
                        temp_sources.update(temp_parser.sources)
                        temp_targets.update(temp_parser.targets)
                        temp_mappings.update(temp_parser.mappings)
                    except:
                        pass
                
                # Generate summaries to get accurate token counts
                if temp_sources or temp_targets or temp_mappings:
                    full_summary = prepare_raw_xml_summary(temp_sources, temp_targets, temp_mappings, optimized=False)
                    optimized_summary = prepare_raw_xml_summary(temp_sources, temp_targets, temp_mappings, optimized=True)
                    
                    estimated_tokens_full = len(full_summary) // 4
                    estimated_tokens_optimized = len(optimized_summary) // 4
                    
                    # Cache the estimates
                    st.session_state.preview_token_estimate = {
                        'full': estimated_tokens_full,
                        'optimized': estimated_tokens_optimized
                    }
            except Exception as e:
                # Fallback to file size estimation if parsing fails
                total_size = 0
                for f in uploaded_files:
                    f.seek(0, 2)
                    total_size += f.tell()
                    f.seek(0)
                estimated_tokens_full = total_size // 16  # More conservative estimate
                estimated_tokens_optimized = estimated_tokens_full // 3
                st.session_state.preview_token_estimate = {
                    'full': estimated_tokens_full,
                    'optimized': estimated_tokens_optimized
                }
    
    # Use cached estimates if available
    if 'preview_token_estimate' in st.session_state:
        estimated_tokens_full = st.session_state.preview_token_estimate['full']
        estimated_tokens_optimized = st.session_state.preview_token_estimate['optimized']
    
    # Show token estimate based on selected mode
    if estimated_tokens_full > 0 or estimated_tokens_optimized > 0:
        user_mode = st.session_state.get('tdd_generation_mode', 'auto')
        
        if user_mode == 'auto':
            if estimated_tokens_full > 15000:
                display_tokens = estimated_tokens_optimized
                mode_note = "Auto will use Optimized"
            else:
                display_tokens = estimated_tokens_full
                mode_note = "Auto will use Full"
        elif user_mode == 'full':
            display_tokens = estimated_tokens_full
            mode_note = "Full mode"
        else:  # optimized
            display_tokens = estimated_tokens_optimized
            mode_note = "Optimized mode"
        
        if estimated_tokens_full > 15000:
            st.markdown(f"Choose how much data to send to the LLM for TDD generation.")
            st.caption(f"📊 **{mode_note}**: ~{display_tokens:,} tokens | Full: ~{estimated_tokens_full:,} | Optimized: ~{estimated_tokens_optimized:,}")
        else:
            st.markdown(f"Choose how much data to send to the LLM for TDD generation. **(~{display_tokens:,} tokens)**")
    else:
        st.markdown("Choose how much data to send to the LLM for TDD generation.")
    
    mode_options = {
        "auto": "🔄 Auto (Recommended) - Selects based on data size",
        "full": "📄 Full - Send all data to LLM",
        "optimized": "⚡ Optimized - Smart sampling"
    }
    
    st.session_state.tdd_generation_mode = st.radio(
        "Select Mode",
        options=list(mode_options.keys()),
        format_func=lambda x: mode_options[x],
        index=0,
        horizontal=True,
        key="tdd_mode_radio"
    )


def resolve_cross_mapping_lineage(df_lineage, mappings_dict):
    """Resolve cross-mapping lineage and filter to final target tables only.

    When mappings chain (m1→staging→m2→staging2→m3→final_target), this function:
    1. Traces each field back through the chain to find the original source
    2. Filters out intermediate target tables — keeps only final targets
    3. Replaces immediate source references with the resolved original source

    The resulting STTM shows: original_source → final_target (across all mappings).
    """
    # Build lookup: which tables are targets of which mappings
    all_target_tables = set()   # every table that is a target of any mapping
    all_source_tables = set()   # every table that is a source of any mapping
    for m_name, m_data in mappings_dict.items():
        for tgt_table in set(m_data.get('target_instances', {}).values()):
            all_target_tables.add(tgt_table.upper())
        for src_table in set(m_data.get('source_instances', {}).values()):
            all_source_tables.add(src_table.upper())

    # Intermediate tables = tables that are BOTH a target of one mapping AND a source of another
    intermediate_tables = all_target_tables & all_source_tables

    # Final target tables = target tables that are NOT used as a source by any mapping
    final_target_tables = all_target_tables - all_source_tables

    # If we can't determine final targets (single mapping or no chaining), return as-is
    if not intermediate_tables or not final_target_tables:
        return df_lineage

    # Build lookup: (target_table_upper, target_column_upper) → lineage row as dict
    target_field_lookup = {}
    for _, row in df_lineage.iterrows():
        tgt_table = str(row.get('Target_Table', '')).strip().upper()
        tgt_col = str(row.get('Target_Column', '')).strip().upper()
        if tgt_table and tgt_col:
            key = (tgt_table, tgt_col)
            if key not in target_field_lookup:
                target_field_lookup[key] = row.to_dict()

    special_sources = {'Hardcoded', 'Derived', 'SYSTEM', 'UNCONNECTED',
                       'SEQUENCE_GENERATOR', 'Lookup/Expression', ''}

    def trace_upstream(src_table, src_col, expr, mapping_name):
        """Recursively trace a field back through chained mappings."""
        path = [mapping_name]
        visited = set()
        current_src_table = src_table
        current_src_col = src_col
        current_expr = expr
        resolved = False

        for _ in range(10):  # max depth
            src_upper = current_src_table.upper() if current_src_table else ''

            if current_src_table in special_sources or not src_upper:
                break
            if src_upper not in intermediate_tables:
                break

            trace_key = (src_upper, current_src_col.upper() if current_src_col else '')
            if trace_key in visited:
                break
            visited.add(trace_key)

            # Find upstream lineage row
            upstream_row = target_field_lookup.get(trace_key)

            # Try cleaning the column name if no exact match
            if upstream_row is None and current_src_col:
                clean_col = current_src_col
                if ' <- ' in clean_col:
                    clean_col = clean_col.split(' <- ')[0].strip()
                if clean_col.startswith('in_'):
                    clean_col = clean_col[3:]
                upstream_row = target_field_lookup.get((src_upper, clean_col.upper()))

            if upstream_row is None:
                break

            upstream_mapping = str(upstream_row.get('Mapping_Name', '')).strip()
            if upstream_mapping and upstream_mapping not in path:
                path.insert(0, upstream_mapping)

            current_src_table = str(upstream_row.get('Source_Table_INSERT', '')).strip()
            current_src_col = str(upstream_row.get('Source_Column_INSERT', '')).strip()
            upstream_expr = str(upstream_row.get('Expression_Logic', '')).strip()
            if upstream_expr:
                current_expr = upstream_expr
            resolved = True

        return current_src_table, current_src_col, current_expr, path, resolved

    # Filter to final target rows only and resolve their sources
    final_rows = []
    for _, row in df_lineage.iterrows():
        tgt_table = str(row.get('Target_Table', '')).strip().upper()
        if tgt_table not in final_target_tables:
            continue

        row_dict = row.to_dict()
        src_table = str(row.get('Source_Table_INSERT', '')).strip()
        src_col = str(row.get('Source_Column_INSERT', '')).strip()
        expr = str(row.get('Expression_Logic', '')).strip()
        mapping_name = str(row.get('Mapping_Name', '')).strip()

        # Trace upstream through chained mappings
        orig_table, orig_col, orig_expr, path, resolved = trace_upstream(
            src_table, src_col, expr, mapping_name)

        if resolved and (orig_table != src_table or orig_col != src_col):
            # Promote resolved original source into the main columns
            row_dict['Source_Table_INSERT'] = orig_table
            row_dict['Source_Column_INSERT'] = orig_col
            if orig_expr:
                row_dict['Expression_Logic'] = orig_expr
            row_dict['Cross_Mapping_Path'] = ' → '.join(path)
        else:
            row_dict['Cross_Mapping_Path'] = ''

        final_rows.append(row_dict)

    if final_rows:
        return pd.DataFrame(final_rows)
    else:
        # No final target rows found — return original (shouldn't happen)
        df_lineage['Cross_Mapping_Path'] = ''
        return df_lineage


def parse_and_process_files(uploaded_files):
    """Parse uploaded XML files and generate documentation."""

    all_lineage = []
    all_sources = []
    all_targets = []
    all_transformations = []
    all_instances = []
    all_connectors = []
    all_sources_dict = {}
    all_targets_dict = {}
    all_mappings_dict = {}

    # Notebook data (populated from DatabricksNotebookParser)
    all_notebook_cells = []
    all_notebook_names = []

    # Workflow data
    all_workflow_data = []
    all_session_data = []
    all_task_data = []
    all_link_data = []
    all_connection_data = []
    all_command_data = []
    workflow_summary_text = ""
    has_workflows = False
    has_mappings = False
    workflow_parsers = []

    # Track mapping origin: standalone mapping XML vs embedded in workflow XML
    mappings_from_standalone = set()   # mapping names from mapping-only XML files
    mappings_from_workflow = set()     # mapping names embedded inside workflow XML files

    # Progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()

    # Determine which LLM model to use for SQL Tier-2 fallback
    _sql_llm_model = st.session_state.get('primary_llm_model') or get_default_model()

    for file_idx, uploaded_file in enumerate(uploaded_files):
        status_text.text(f"Processing {uploaded_file.name}...")

        try:
            # Read file content
            file_content = uploaded_file.read()
            fname_lower = uploaded_file.name.lower()

            # ---- Route non-XML files via ParserRegistry ----
            if not fname_lower.endswith('.xml'):
                parser_class = ParserRegistry.get_parser(uploaded_file.name, file_content)
                if parser_class is None:
                    st.warning(f"No parser registered for {uploaded_file.name}. Skipping.")
                    continue

                status_text.text(f"Parsing {uploaded_file.name}...")
                generic_parser = parser_class(file_content, filename=uploaded_file.name)
                generic_parser.parse_all()

                # Capture notebook cells/context if this is a notebook parser
                if hasattr(generic_parser, 'cell_sequence') and generic_parser.cell_sequence:
                    all_notebook_cells.extend(generic_parser.cell_sequence)
                    all_notebook_names.extend(
                        getattr(generic_parser, 'notebooks', []) or [generic_parser.mapping_name]
                    )
                    # Pre-fill business context from notebook markdown (if user hasn't typed anything)
                    nb_ctx = getattr(generic_parser, 'notebook_context', '')
                    if nb_ctx and not st.session_state.get('main_business_context', '').strip():
                        st.session_state.main_business_context = nb_ctx[:3000]

                def sql_progress(current, total, mname):
                    overall = (file_idx + current / max(total, 1)) / len(uploaded_files)
                    progress_bar.progress(min(overall * 0.7, 0.7))
                    status_text.text(f"{uploaded_file.name}: statement {current}/{total}")

                generic_lineage = generic_parser.build_lineage(
                    progress_callback=sql_progress,
                    call_llm_fn=call_llm if LLM_PLATFORM_AVAILABLE else None,
                    llm_model=_sql_llm_model if LLM_PLATFORM_AVAILABLE else None,
                )

                if generic_lineage:
                    has_mappings = True
                    all_lineage.extend(generic_lineage)
                    all_sources.extend(generic_parser.sources_data)
                    all_targets.extend(generic_parser.targets_data)
                    all_sources_dict.update(generic_parser.sources)
                    all_targets_dict.update(generic_parser.targets)
                    all_mappings_dict.update(generic_parser.mappings)
                    mappings_from_standalone.update(generic_parser.mappings.keys())
                else:
                    st.warning(f"No lineage extracted from {uploaded_file.name}. "
                               "The file may contain only DDL (CREATE TABLE) or unsupported syntax.")
                continue  # skip XML path

            # ---- XML files — existing logic unchanged ----
            xml_content = file_content

            # Auto-detect XML type
            xml_type = InformaticaWorkflowParser.get_xml_type(xml_content)

            if xml_type == 'workflow':
                # Parse as workflow XML
                has_workflows = True
                status_text.text(f"Processing workflow: {uploaded_file.name}...")

                wf_parser = InformaticaWorkflowParser(xml_content)

                def wf_progress(pct, msg):
                    overall = (file_idx + pct) / len(uploaded_files)
                    progress_bar.progress(min(overall * 0.7, 0.7))
                    status_text.text(f"{uploaded_file.name}: {msg}")

                wf_parser.parse_all(progress_callback=wf_progress)

                all_workflow_data.extend(wf_parser.workflow_data)
                all_session_data.extend(wf_parser.session_data)
                all_task_data.extend(wf_parser.task_data)
                all_link_data.extend(wf_parser.link_data)
                all_connection_data.extend(wf_parser.connection_data)
                all_command_data.extend(wf_parser.command_data)
                workflow_parsers.append(wf_parser)

            if xml_type == 'mapping' or xml_type == 'workflow':
                # Mapping XMLs always get parsed for lineage
                # Workflow XMLs may also contain source/target definitions referenced by sessions
                has_mappings_in_file = len(ET.fromstring(xml_content).findall('.//MAPPING')) > 0
                if has_mappings_in_file:
                    has_mappings = True

                    parser = InformaticaLineageParser(xml_content)
                    parser.parse_all()

                    def update_progress(current, total, mapping_name):
                        overall_progress = (file_idx + current / total) / len(uploaded_files)
                        progress_bar.progress(min(overall_progress * 0.7, 0.7))
                        status_text.text(f"Processing {uploaded_file.name}: {mapping_name}")

                    lineage = parser.build_lineage(progress_callback=update_progress)
                    all_lineage.extend(lineage)

                    all_sources.extend(parser.sources_data)
                    all_targets.extend(parser.targets_data)
                    all_transformations.extend(parser.transformations_data)
                    all_instances.extend(parser.instances_data)
                    all_connectors.extend(parser.connectors_data)

                    all_sources_dict.update(parser.sources)
                    all_targets_dict.update(parser.targets)
                    all_mappings_dict.update(parser.mappings)

                    # Track whether these mappings came from a standalone file or a workflow file
                    if xml_type == 'workflow':
                        mappings_from_workflow.update(parser.mappings.keys())
                    else:
                        mappings_from_standalone.update(parser.mappings.keys())

        except Exception as e:
            st.error(f"Error processing {uploaded_file.name}: {str(e)}")
            import traceback
            st.code(traceback.format_exc())
            continue

    # ---- Correlation: link workflow sessions to parsed mappings ----
    session_mapping_details = {}
    missing_mappings = set()
    mapping_warnings = []

    if workflow_parsers:
        # Generate workflow summary — unified when mappings are present
        for wf_parser in workflow_parsers:
            if has_mappings and all_mappings_dict:
                wf_summary = wf_parser.generate_unified_summary(
                    all_mappings_dict, all_sources_dict, all_targets_dict)
            else:
                wf_summary = wf_parser.generate_workflow_summary()
            if wf_summary:
                workflow_summary_text += wf_summary + "\n\n"

        # Build session→mapping detail map and detect missing mappings
        referenced = set()
        for wf_parser in workflow_parsers:
            referenced.update(wf_parser.get_referenced_mappings())
            for sess_name, sess in wf_parser.sessions.items():
                mapping_name = sess['mapping_name']
                mapping_data = all_mappings_dict.get(mapping_name)

                # Resolve source/target tables from the mapping's instances
                src_tables = []
                tgt_tables = []
                if mapping_data:
                    src_tables = sorted(set(mapping_data.get('source_instances', {}).values()))
                    tgt_tables = sorted(set(mapping_data.get('target_instances', {}).values()))

                session_mapping_details[sess_name] = {
                    'session': sess,
                    'mapping_name': mapping_name,
                    'mapping': mapping_data,
                    'source_tables': src_tables,
                    'target_tables': tgt_tables,
                }

        uploaded_mapping_names = set(all_mappings_dict.keys())
        missing_mappings = referenced - uploaded_mapping_names

        # Warn about mappings only available as embedded copies inside workflow XMLs
        embedded_only = (referenced & mappings_from_workflow) - mappings_from_standalone

        # Store warnings in session state so they persist across reruns
        mapping_warnings = []
        if missing_mappings:
            mapping_warnings.append({
                'type': 'warning',
                'msg': (
                    f"The workflow references {len(missing_mappings)} mapping(s) not found in uploaded files: "
                    f"**{', '.join(sorted(missing_mappings))}**. "
                    "Upload these mapping XMLs for a complete analysis."
                ),
            })
        if embedded_only:
            mapping_warnings.append({
                'type': 'info',
                'msg': (
                    f"**{len(embedded_only)} mapping(s)** are only available from embedded copies "
                    f"inside workflow XML(s): **{', '.join(sorted(embedded_only))}**. "
                    "For a more complete analysis, upload the standalone mapping XML file(s) as well."
                ),
            })

    progress_bar.progress(0.7)
    status_text.text("XML parsing complete. Preparing data...")

    if all_lineage or has_workflows:
        # Create DataFrames for mappings
        df_lineage = pd.DataFrame(all_lineage) if all_lineage else pd.DataFrame()
        df_sources = pd.DataFrame(all_sources) if all_sources else pd.DataFrame()
        df_targets = pd.DataFrame(all_targets) if all_targets else pd.DataFrame()
        df_transformations = pd.DataFrame(all_transformations) if all_transformations else pd.DataFrame()
        df_instances = pd.DataFrame(all_instances) if all_instances else pd.DataFrame()
        df_connectors = pd.DataFrame(all_connectors) if all_connectors else pd.DataFrame()

        # ---- Cross-mapping lineage resolution ----
        # When multiple mappings chain together (m1→staging→m2→staging2→m3→target),
        # trace each field back to its original source across mapping boundaries.
        if has_workflows and has_mappings and len(df_lineage) > 0 and len(all_mappings_dict) > 1:
            status_text.text("Resolving cross-mapping lineage...")
            df_lineage = resolve_cross_mapping_lineage(df_lineage, all_mappings_dict)

        # ---- Deduplicate lineage ----
        # Same mapping may appear in both a workflow XML and a separate mapping XML,
        # producing identical lineage rows. Drop exact-duplicate rows only.
        if len(df_lineage) > 0:
            df_lineage = df_lineage.drop_duplicates(keep='first').reset_index(drop=True)

        # Create DataFrames for workflows (deduplicate — same workflow can appear
        # when both workflow XML and mapping XML are uploaded from the same file)
        df_workflows = pd.DataFrame(all_workflow_data) if all_workflow_data else pd.DataFrame()
        df_sessions = pd.DataFrame(all_session_data) if all_session_data else pd.DataFrame()
        df_tasks = pd.DataFrame(all_task_data) if all_task_data else pd.DataFrame()
        df_links = pd.DataFrame(all_link_data) if all_link_data else pd.DataFrame()
        df_connections = pd.DataFrame(all_connection_data) if all_connection_data else pd.DataFrame()
        df_commands = pd.DataFrame(all_command_data) if all_command_data else pd.DataFrame()

        # Drop exact-duplicate rows only (preserves distinct workflows/sessions/tasks
        # that happen to share similar names across different workflows)
        if not df_workflows.empty:
            df_workflows = df_workflows.drop_duplicates(keep='first').reset_index(drop=True)
        if not df_sessions.empty:
            df_sessions = df_sessions.drop_duplicates(keep='first').reset_index(drop=True)
        if not df_tasks.empty:
            df_tasks = df_tasks.drop_duplicates(keep='first').reset_index(drop=True)
        if not df_links.empty:
            df_links = df_links.drop_duplicates(keep='first').reset_index(drop=True)
        if not df_connections.empty:
            df_connections = df_connections.drop_duplicates(keep='first').reset_index(drop=True)
        if not df_commands.empty:
            df_commands = df_commands.drop_duplicates(keep='first').reset_index(drop=True)

        # Store in session state
        has_sql_files = any(f.name.lower().endswith('.sql') for f in uploaded_files)
        has_notebook = bool(all_notebook_cells)
        st.session_state.has_notebook_parsed = has_notebook
        st.session_state.parsed_data = {
            'df_lineage': df_lineage,
            'df_sources': df_sources,
            'df_targets': df_targets,
            'df_transformations': df_transformations,
            'df_instances': df_instances,
            'df_connectors': df_connectors,
            'sources_dict': all_sources_dict,
            'targets_dict': all_targets_dict,
            'mappings_dict': all_mappings_dict,
            'has_sql_files': has_sql_files,
            # Workflow data
            'df_workflows': df_workflows,
            'df_sessions': df_sessions,
            'df_tasks': df_tasks,
            'df_links': df_links,
            'df_connections': df_connections,
            'df_commands': df_commands,
            'workflow_summary': workflow_summary_text,
            'has_workflows': has_workflows,
            'has_mappings': has_mappings,
            'session_mapping_details': session_mapping_details,
            'missing_mappings': sorted(missing_mappings) if missing_mappings else [],
            'mapping_warnings': mapping_warnings,
            'workflow_parsers': workflow_parsers,
            # Notebook data
            'notebook_cell_sequence': all_notebook_cells,
            'notebook_names': all_notebook_names,
            'has_notebook': has_notebook,
        }

        # Generate TDD during initial processing
        if LLM_PLATFORM_AVAILABLE:
            progress_bar.progress(0.75)
            status_text.text("Generating TDD using AI... (this may take 30-60 seconds)")
            is_unified = has_workflows and has_mappings and bool(all_mappings_dict)
            generate_tdd_from_xml(all_sources_dict, all_targets_dict, all_mappings_dict,
                                 progress_bar, status_text,
                                 workflow_summary=workflow_summary_text if has_workflows else None,
                                 is_unified=is_unified)

        progress_bar.progress(1.0)
        status_text.text("All processing complete!")
        st.rerun()
    else:
        st.warning("No data was extracted from the uploaded files.")


def generate_tdd_from_xml(sources_dict, targets_dict, mappings_dict,
                          progress_bar=None, status_text=None,
                          workflow_summary=None, is_unified=False):
    """Generate TDD using the active LLM platform (Snowflake Cortex or Databricks)."""

    def update_status(msg, progress=None):
        if status_text:
            status_text.text(msg)
        if progress_bar and progress:
            progress_bar.progress(progress)

    conn, error = get_connection()
    if not conn:
        st.session_state.xml_tdd_result = {'error': f"LLM platform connection failed: {error}"}
        return

    # Determine mode
    user_mode = st.session_state.get('tdd_generation_mode', 'auto')

    # Check content size for auto mode
    update_status("Analyzing content size...", 0.76)
    raw_summary_check = prepare_raw_xml_summary(sources_dict, targets_dict, mappings_dict, optimized=False) if sources_dict else ""
    estimated_tokens = len(raw_summary_check) // 4
    if workflow_summary:
        estimated_tokens += len(workflow_summary) // 4

    if user_mode == 'auto':
        use_optimized = estimated_tokens > 15000
    elif user_mode == 'full':
        use_optimized = False
    else:
        use_optimized = True

    # Prepare mapping summary
    update_status(f"Preparing {'optimized' if use_optimized else 'full'} summary...", 0.78)
    raw_summary = ""
    if sources_dict:
        raw_summary = prepare_raw_xml_summary(sources_dict, targets_dict, mappings_dict, optimized=use_optimized)

    # Store mode info
    st.session_state.tdd_use_optimized = use_optimized
    st.session_state.tdd_estimated_tokens = estimated_tokens
    st.session_state.tdd_mode_used = "optimized" if use_optimized else "full"

    tdd_results = {}

    # Create prompt — use workflow prompt when workflow data is available
    if workflow_summary:
        # When unified, the workflow summary already contains mapping details inline,
        # so we pass mapping_summary only as supplemental context (or skip if too large)
        mapping_context = raw_summary if not is_unified else ""
        full_prompt = create_workflow_tdd_prompt(
            workflow_summary=workflow_summary,
            mapping_summary=mapping_context,
            business_context=st.session_state.get('main_business_context', ''),
            additional_requirements=st.session_state.get('main_additional_requirements', ''),
            is_unified=is_unified,
        )
    else:
        full_prompt = create_raw_xml_brd_prompt(
            raw_summary,
            st.session_state.get('main_business_context', ''),
            st.session_state.get('main_additional_requirements', '')
        )
    
    # Track errors for display
    llm_errors = []

    # Primary Model
    if st.session_state.get('use_primary', True):
        primary_model = st.session_state.get('primary_model', get_default_model('primary'))
        update_status(f"Generating TDD with {primary_model}... (this may take 30-60 seconds)", 0.80)
        response, err = call_llm(primary_model, full_prompt)
        if response:
            tdd_results['primary'] = response
            tdd_results['primary_model'] = primary_model
        elif err:
            llm_errors.append(f"Primary model ({primary_model}): {err}")
            update_status(f"Primary model error: {err[:80]}...", 0.82)

    # Secondary Model
    if st.session_state.get('use_secondary', False):
        secondary_model = st.session_state.get('secondary_model', get_default_model('secondary'))
        update_status(f"Generating TDD with {secondary_model}...", 0.85)
        response, err = call_llm(secondary_model, full_prompt)
        if response:
            tdd_results['secondary'] = response
            tdd_results['secondary_model'] = secondary_model
        elif err:
            llm_errors.append(f"Secondary model ({secondary_model}): {err}")

    # Consolidation
    if (st.session_state.get('use_consolidation', False) and
        tdd_results.get('primary') and tdd_results.get('secondary')):
        from generators.prompts import create_consolidation_prompt
        consolidation_model = st.session_state.get('consolidation_model', get_default_model('consolidation'))
        update_status(f"Consolidating results with {consolidation_model}...", 0.90)
        consolidation_prompt = create_consolidation_prompt(
            tdd_results['primary'],
            tdd_results['secondary'],
            st.session_state.get('main_business_context', '')
        )
        response, err = call_llm(consolidation_model, consolidation_prompt)
        if response:
            tdd_results['consolidated'] = response
            tdd_results['consolidation_model'] = consolidation_model
        elif err:
            llm_errors.append(f"Consolidation model ({consolidation_model}): {err}")

    update_status("TDD generation complete!", 0.95)
    if tdd_results:
        st.session_state.xml_tdd_result = tdd_results
    else:
        error_detail = "\n".join(llm_errors) if llm_errors else "No response from LLM"
        st.session_state.xml_tdd_result = {'error': f"TDD generation failed.\n{error_detail}"}


def _render_download_all_button(data):
    """Render a Download Everything button that exports all outputs to a single Excel file."""
    import io as _io

    def _build_all_excel():
        sheets = {}

        # TDD (markdown as text in a single cell)
        tdd_results = st.session_state.get('xml_tdd_result')
        if tdd_results and 'error' not in tdd_results:
            tdd_content = tdd_results.get('consolidated') or tdd_results.get('primary') or tdd_results.get('secondary')
            if tdd_content:
                sheets['TDD'] = pd.DataFrame({'Technical Design Document': [tdd_content]})

        # Enhanced STTM
        esttm = st.session_state.get('enhanced_sttm_df')
        if esttm is not None and not esttm.empty:
            sheets['Enhanced STTM'] = esttm

        # Raw STTM / Lineage
        if data.get('df_lineage') is not None and len(data['df_lineage']) > 0:
            sheets['STTM Lineage'] = data['df_lineage']

        # Sources
        if data.get('df_sources') is not None and len(data['df_sources']) > 0:
            sheets['Sources'] = data['df_sources']

        # Targets
        if data.get('df_targets') is not None and len(data['df_targets']) > 0:
            sheets['Targets'] = data['df_targets']

        # Transformations
        if data.get('df_transformations') is not None and len(data['df_transformations']) > 0:
            sheets['Transformations'] = data['df_transformations']

        # Workflows
        if data.get('df_workflows') is not None and len(data['df_workflows']) > 0:
            sheets['Workflows'] = data['df_workflows']

        # Sessions
        if data.get('df_sessions') is not None and len(data['df_sessions']) > 0:
            sheets['Sessions'] = data['df_sessions']

        # Connections
        if data.get('df_connections') is not None and len(data['df_connections']) > 0:
            sheets['Connections'] = data['df_connections']

        # Lineage Diagram (markdown)
        lineage_md = st.session_state.get('lineage_diagrams_md')
        if lineage_md:
            sheets['Lineage Diagram'] = pd.DataFrame({'Lineage Diagram (Markdown)': [lineage_md]})

        # Conversion Report (markdown)
        conv_report = st.session_state.get('conversion_report_md')
        if conv_report:
            sheets['Conversion Report'] = pd.DataFrame({'Conversion Report': [conv_report]})

        # Updated STTM
        updated = st.session_state.get('updated_lineage')
        if updated is not None and not updated.empty:
            sheets['Updated STTM'] = updated

        # SQL Generator results
        sql_results = st.session_state.get('sql_generator_results', {})
        if sql_results:
            sql_rows = []
            for key, result in sql_results.items():
                if isinstance(result, dict) and result.get('sql'):
                    sql_rows.append({
                        'Type': result.get('type', key),
                        'Target': result.get('target', ''),
                        'SQL': result['sql'],
                    })
            if sql_rows:
                sheets['SQL'] = pd.DataFrame(sql_rows)

        if not sheets:
            return None

        buf = _io.BytesIO()
        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
            for sheet_name, df in sheets.items():
                # Excel sheet names max 31 chars
                safe_name = sheet_name[:31]
                df.to_excel(writer, sheet_name=safe_name, index=False)
                # Auto-width for text-heavy sheets
                try:
                    from openpyxl.styles import Alignment
                    ws = writer.sheets[safe_name]
                    for col_cells in ws.columns:
                        max_len = max(len(str(c.value or '')) for c in col_cells)
                        col_letter = col_cells[0].column_letter
                        ws.column_dimensions[col_letter].width = min(max_len + 2, 80)
                    # Wrap text for single-cell markdown sheets
                    if len(df) == 1 and len(df.columns) == 1:
                        ws.column_dimensions['A'].width = 120
                        for row in ws.iter_rows(min_row=2):
                            for cell in row:
                                cell.alignment = Alignment(wrap_text=True, vertical='top')
                except Exception:
                    pass
        buf.seek(0)
        return buf

    excel_data = _build_all_excel()
    if excel_data:
        st.download_button(
            "📥 Download Everything (Excel)",
            data=excel_data.getvalue(),
            file_name="TDD_all_outputs.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_all_excel",
        )


def render_results_tabs():
    """Render the results tabs after parsing."""

    data = st.session_state.parsed_data
    has_workflows = data.get('has_workflows', False)
    has_mappings = data.get('has_mappings', False)

    # Summary metrics
    st.markdown("---")
    if has_workflows and has_mappings:
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Workflows", len(data.get('df_workflows', [])))
        with col2:
            st.metric("Sessions", len(data.get('df_sessions', [])))
        with col3:
            st.metric("Lineage Records", len(data['df_lineage']))
        with col4:
            st.metric("Source Fields", len(data['df_sources']))
        with col5:
            st.metric("Target Fields", len(data['df_targets']))
    elif has_workflows:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Workflows", len(data.get('df_workflows', [])))
        with col2:
            st.metric("Sessions", len(data.get('df_sessions', [])))
        with col3:
            st.metric("Tasks", len(data.get('df_tasks', [])))
        with col4:
            st.metric("Connections", len(data.get('df_connections', [])))
    else:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Lineage Records", len(data['df_lineage']))
        with col2:
            st.metric("Source Fields", len(data['df_sources']))
        with col3:
            st.metric("Target Fields", len(data['df_targets']))
        with col4:
            st.metric("Transformations", len(data['df_transformations']))

    # Display persistent mapping warnings (stored during parsing)
    for warn in data.get('mapping_warnings', []):
        if warn['type'] == 'warning':
            st.warning(warn['msg'])
        else:
            st.info(warn['msg'])

    # Download Everything button
    _render_download_all_button(data)

    # Build tab list dynamically — optional tabs controlled by sidebar checkboxes
    tab_entries = []  # list of (label, renderer_callable)

    tab_entries.append(("TDD", lambda: render_tdd_tab(data)))
    tab_entries.append(("Parser Output", lambda: _render_parser_output_tab(data, has_workflows, has_mappings)))
    tab_entries.append(("Enhanced STTM", lambda: render_enhanced_sttm_tab(data)))
    tab_entries.append(("Lineage Diagram", lambda: render_lineage_diagram_tab(data)))
    tab_entries.append(("Data Model", lambda: render_data_model_tab(data)))
    tab_entries.append(("Update STTM", lambda: render_update_sttm_tab(data)))

    if st.session_state.get('enable_conversion_report', False):
        tab_entries.append(("Conversion Report", lambda: render_conversion_report_tab(data)))

    if st.session_state.get('enable_sql_generator', False):
        tab_entries.append(("SQL Generator", lambda: render_sql_generator_tab(
            sources_dict=data['sources_dict'],
            targets_dict=data['targets_dict'],
            lineage_df=data['df_lineage'],
            mappings_dict=data.get('mappings_dict', {}),
        )))

    if st.session_state.get('enable_notebook_overview', False) and data.get('has_notebook'):
        tab_entries.append(("📓 Notebook Overview", lambda: render_notebook_overview_tab(data)))

    tab_labels = [label for label, _ in tab_entries]
    tabs = st.tabs(tab_labels)

    for i, (_, renderer) in enumerate(tab_entries):
        with tabs[i]:
            renderer()


def _render_parser_output_tab(data, has_workflows, has_mappings):
    """Render the Parser Output tab with nested sub-tabs for all parsed data."""
    st.markdown("### Parsed XML Output")

    sub_labels = []
    sub_renderers = []

    # Workflow sub-tabs
    if has_workflows:
        sub_labels.append("Workflows")
        sub_renderers.append(lambda d=data: render_workflow_tab(d))

        sub_labels.append("Sessions")
        sub_renderers.append(lambda d=data: render_dataframe_tab(d.get('df_sessions', pd.DataFrame()), "Sessions"))

        if data.get('session_mapping_details'):
            sub_labels.append("Execution Pipeline")
            sub_renderers.append(lambda d=data: render_execution_pipeline_tab(d))

        sub_labels.append("Task DAG")
        sub_renderers.append(lambda d=data: render_task_dag_tab(d))

        sub_labels.append("Connections")
        sub_renderers.append(lambda d=data: render_dataframe_tab(d.get('df_connections', pd.DataFrame()), "Connections"))

        if data.get('df_commands') is not None and len(data['df_commands']) > 0:
            sub_labels.append("Commands")
            sub_renderers.append(lambda d=data: render_dataframe_tab(d['df_commands'], "Commands"))

    # Mapping sub-tabs
    if has_mappings:
        sub_labels.append("Sources")
        sub_renderers.append(lambda d=data: render_dataframe_tab(d['df_sources'], "Sources"))

        sub_labels.append("Targets")
        sub_renderers.append(lambda d=data: render_dataframe_tab(d['df_targets'], "Targets"))

        sub_labels.append("Transformations")
        sub_renderers.append(lambda d=data: render_dataframe_tab(d['df_transformations'], "Transformations"))

        sub_labels.append("Instances")
        sub_renderers.append(lambda d=data: render_dataframe_tab(d['df_instances'], "Instances"))

        sub_labels.append("Connectors")
        sub_renderers.append(lambda d=data: render_dataframe_tab(d['df_connectors'], "Connectors"))

        sub_labels.append("STTM (Lineage)")
        sub_renderers.append(lambda d=data: render_dataframe_tab(d['df_lineage'], "STTM (Lineage)"))

    if not sub_labels:
        st.info("No parser output available. Upload XML files and parse them first.")
        return

    sub_tabs = st.tabs(sub_labels)
    for i, stab in enumerate(sub_tabs):
        with stab:
            sub_renderers[i]()


def render_tdd_tab(data):
    """Render the TDD tab - displays TDD generated during parsing."""
    st.markdown("### 📋 Technical Design Document (TDD)")
    
    # Check if TDD was generated
    if st.session_state.get('xml_tdd_result'):
        tdd_results = st.session_state.xml_tdd_result
        
        if 'error' in tdd_results:
            err_msg = tdd_results['error']
            # Connection/credential errors are expected in local dev — show as info, not error
            if 'connection failed' in err_msg.lower() or 'secrets.toml' in err_msg.lower() or 'not configured' in err_msg.lower():
                st.info(
                    "ℹ️ **TDD generation skipped** — no LLM credentials configured.\n\n"
                    "Open **`.streamlit/secrets.toml`** in the app folder and add one of these, "
                    "then restart the app (`Ctrl+C` → `streamlit run streamlit_app.py`):\n\n"
                    "**Anthropic Claude (easiest):**\n"
                    "```\n[claude]\napi_key = \"sk-ant-api03-...\"\n```\n"
                    "Get a key at: https://console.anthropic.com/settings/keys\n\n"
                    "**Databricks:**\n"
                    "```\n[databricks]\nhost  = \"your-workspace.azuredatabricks.net\"\n"
                    "token = \"dapi...\"\n```\n\n"
                    "All other tabs (STTM, SQL Generator, Lineage Diagrams) work without credentials."
                )
            else:
                st.error(err_msg)
        else:
            # Show mode info
            mode_used = st.session_state.get('tdd_mode_used', 'full')
            tokens_used = st.session_state.get('tdd_estimated_tokens', 0)
            if mode_used == 'optimized':
                st.info(f"⚡ **Mode**: Optimized | **Estimated tokens**: ~{tokens_used:,}")
            else:
                st.success(f"📄 **Mode**: Full | **Estimated tokens**: ~{tokens_used:,}")
            
            # Display content
            content = tdd_results.get('consolidated') or tdd_results.get('primary') or tdd_results.get('secondary')
            model_used = tdd_results.get('consolidation_model') or tdd_results.get('primary_model') or tdd_results.get('secondary_model')
            
            if content:
                if model_used:
                    st.caption(f"Generated by: {model_used}")
                st.markdown(content)
                
                # Download options
                st.markdown("---")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.download_button(
                        "📥 Download as Markdown",
                        content,
                        file_name="TDD_document.md",
                        mime="text/markdown",
                        key="download_tdd_md"
                    )
                with col2:
                    st.download_button(
                        "📥 Download as Text",
                        content,
                        file_name="TDD_document.txt",
                        mime="text/plain",
                        key="download_tdd_txt"
                    )
                with col3:
                    docx_buf = create_word_export(
                        content,
                        title_text="Technical Design Document (TDD)",
                        model_info=model_used or "",
                    )
                    if docx_buf:
                        st.download_button(
                            "📥 Download as Word",
                            data=docx_buf.getvalue(),
                            file_name="TDD_document.docx",
                            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                            key="download_tdd_docx"
                        )
    else:
        # TDD not generated (no LLM platform available)
        st.warning(f"TDD was not generated. This requires an LLM platform connection ({get_platform_name()}).")


def render_dataframe_tab(df, title):
    """Render a generic dataframe tab."""
    st.markdown(f"### {title}")
    
    if df is not None and len(df) > 0:
        st.dataframe(df, use_container_width=True, height=400)
        st.caption(f"Total rows: {len(df)}")
    else:
        st.info(f"No {title.lower()} data available")


def render_enhanced_sttm_tab(data):
    """Render the Enhanced STTM tab."""
    st.markdown("### Enhanced Source-to-Target Mapping")

    lineage_df = data.get('df_lineage')
    if lineage_df is None or lineage_df.empty:
        st.info("No lineage data available. Upload and parse your source files first.")
        return

    # Auto-generate if not cached
    if st.session_state.enhanced_sttm_df is None:
        st.session_state.enhanced_sttm_df = generate_enhanced_sttm(
            lineage_df, data.get('sources_dict', {}), data.get('targets_dict', {}))

    esttm = st.session_state.enhanced_sttm_df
    if esttm is None or esttm.empty:
        st.warning("Could not generate Enhanced STTM from lineage data.")
        return

    # Stats
    type_counts = esttm['Source Type'].value_counts()
    cols = st.columns(len(type_counts) + 1)
    with cols[0]:
        st.metric("Total Columns", len(esttm))
    for i, (stype, cnt) in enumerate(type_counts.items()):
        with cols[i + 1]:
            st.metric(stype, cnt)

    # Display table
    st.dataframe(esttm, use_container_width=True, height=500)

    # Downloads
    dcol1, dcol2 = st.columns(2)
    with dcol1:
        csv_data = enhanced_sttm_to_csv(esttm)
        st.download_button(
            "Download CSV",
            data=csv_data,
            file_name="enhanced_sttm.csv",
            mime="text/csv",
            key="download_enhanced_sttm_csv",
        )
    with dcol2:
        try:
            import io
            excel_buf = io.BytesIO()
            esttm.to_excel(excel_buf, index=False, sheet_name='Enhanced_STTM')
            st.download_button(
                "Download Excel",
                data=excel_buf.getvalue(),
                file_name="enhanced_sttm.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_enhanced_sttm_xlsx",
            )
        except Exception:
            pass  # openpyxl may not be available


def render_lineage_diagram_tab(data):
    """Render the Lineage Diagram tab."""
    st.markdown("### Data Lineage Diagrams")

    lineage_df = data.get('df_lineage')
    if lineage_df is None or lineage_df.empty:
        st.info("No lineage data available. Upload and parse your source files first.")
        return

    # Auto-generate if not cached
    if st.session_state.lineage_diagrams_md is None:
        st.session_state.lineage_diagrams_md = generate_lineage_diagrams(
            lineage_df,
            data.get('sources_dict', {}),
            data.get('targets_dict', {}),
            data.get('mappings_dict', {}),
        )

    md_content = st.session_state.lineage_diagrams_md
    if not md_content:
        st.warning("Could not generate lineage diagrams.")
        return

    # Download button
    st.download_button(
        "Download Markdown",
        data=md_content,
        file_name="data_lineage_diagram.md",
        mime="text/markdown",
        key="download_lineage_md",
    )

    # Render the markdown (Streamlit renders Mermaid natively)
    st.markdown(md_content, unsafe_allow_html=True)


def render_conversion_report_tab(data):
    """Render the Conversion Report tab — on-demand generation only."""
    st.markdown("### Informatica Workflow Conversion Report")

    lineage_df = data.get('df_lineage')
    if lineage_df is None or lineage_df.empty:
        st.info("No lineage data available. Upload and parse your source files first.")
        return

    if not LLM_PLATFORM_AVAILABLE:
        st.warning("No LLM platform available. The Conversion Report requires an LLM.")
        return

    # Show cached report if available
    report = st.session_state.conversion_report_md
    error = st.session_state.conversion_report_error

    if report is None and error is None:
        # Not yet generated — show generate button
        st.info("Click the button below to generate the conversion report. This may take a minute.")
        if st.button("Generate Conversion Report", type="primary", key="gen_conversion_report"):
            model_name = st.session_state.get('selected_model', get_default_model())
            business_ctx = st.session_state.get('main_business_context', '')
            with st.spinner("Generating conversion report via LLM..."):
                rpt, err = generate_conversion_report(
                    lineage_df,
                    data.get('sources_dict', {}),
                    data.get('targets_dict', {}),
                    data.get('mappings_dict', {}),
                    data.get('workflow_data', {}),
                    model_name,
                    business_context=business_ctx,
                )
                st.session_state.conversion_report_md = rpt
                st.session_state.conversion_report_error = err
                st.rerun()
        return

    # Show error if any
    if error:
        st.error(error)

    # Regenerate button
    if st.button("Regenerate Report", key="regen_conversion_report"):
        st.session_state.conversion_report_md = None
        st.session_state.conversion_report_error = None
        st.rerun()

    if report:
        st.download_button(
            "Download Markdown",
            data=report,
            file_name="conversion_report.md",
            mime="text/markdown",
            key="download_conversion_report_md",
        )
        st.markdown(report)


def render_data_model_tab(data):
    """Render the data model visualization tab."""
    st.markdown("### 🗺️ Data Model Visualization")
    
    if not GRAPHVIZ_AVAILABLE:
        st.warning("Graphviz not installed. Run: pip install graphviz")
        return
    
    model_type = st.radio(
        "Model Type",
        ["conceptual", "logical", "physical"],
        horizontal=True,
        key="model_type_radio"
    )
    
    dot, error = generate_data_model(
        data['sources_dict'],
        data['targets_dict'],
        data['df_lineage'],
        model_type
    )
    
    if dot:
        st.graphviz_chart(dot.source, use_container_width=True)
    elif error:
        st.warning(error)


def render_update_sttm_tab(data):
    """Render the Update STTM tab."""
    st.markdown("### 📊 Update STTM")
    st.markdown("Update the Source-to-Target Mapping based on additional requirements")
    
    df_lineage = data['df_lineage']
    
    # LLM Connection Status (collapsed)
    with st.expander(f"⚙️ {get_platform_name()} Connection Settings", expanded=False):
        if LLM_PLATFORM_AVAILABLE:
            conn, error = get_connection()
            if conn:
                st.success(f"✅ Connected to {get_platform_name()}")
            else:
                st.warning(f"⚠️ {get_platform_name()} connection not configured: {error}")
        else:
            st.info("💡 No LLM platform SDK installed (Snowflake or Databricks).")
    
    st.markdown("---")
    
    # ==================== INPUT SECTION ====================
    st.markdown("#### 📝 Additional Requirements")
    
    sttm_additional_requirements = st.text_area(
        "Enter requirements to update the STTM",
        value=st.session_state.get('sttm_additional_requirements', ''),
        height=150,
        placeholder="""Enter requirements to update the mapping, for example:
- ODS_CHANGE_DATE should be first day of month
- Rename ACCT_NO to Account Number
- Add validation for date fields
- OFFLINE_IND should default to 'N' when null
- Calculate days in offline (DIO) = CURRENT_DATE - OFFLINE_DATE""",
        help="Specify requirements that will update the Expression_Logic or Business Names in the STTM",
        key="sttm_requirements_input"
    )
    st.session_state.sttm_additional_requirements = sttm_additional_requirements
    
    st.markdown("---")
    
    # ==================== UPDATE BUTTON ====================
    update_sttm = st.button("🔄 Update STTM from Requirements", type="primary", key="update_sttm_btn",
                           disabled=not sttm_additional_requirements.strip())
    
    if not sttm_additional_requirements.strip():
        st.caption("Enter requirements above to enable the update button")
    
    # ==================== UPDATE STTM ====================
    if update_sttm and sttm_additional_requirements.strip():
        if not LLM_PLATFORM_AVAILABLE:
            st.error("⚠️ No LLM platform available. Cannot update STTM.")
        else:
            conn, error = get_connection()
            if not conn:
                st.error(f"Cannot connect to {get_platform_name()}: {error}")
            else:
                with st.spinner("Analyzing requirements and updating STTM..."):
                    model_to_use = st.session_state.get('primary_model', get_default_model('primary'))
                    
                    # Use original lineage as base
                    working_lineage = df_lineage.copy()
                    
                    # Get columns for LLM
                    cols_for_llm = ['Target_Column', 'Source_Table_INSERT', 'Source_Column_INSERT', 'Expression_Logic']
                    available_cols = [c for c in cols_for_llm if c in working_lineage.columns]
                    lineage_for_llm = working_lineage[available_cols].head(100).to_string(index=False)
                    
                    # Prompt that forces JSON output
                    tp = st.session_state.get('target_platform', 'postgresql')
                    _lbl = {"postgresql": "PostgreSQL", "mssql": "Microsoft SQL (T-SQL)",
                            "databricks_sql": "Databricks SQL", "databricks_python": "Databricks Python"}
                    platform_label = _lbl.get(tp, tp)
                    date_trunc_example = "DATE_TRUNC('MONTH', CURRENT_DATE)" if tp == 'postgresql' else "DATE_TRUNC('month', current_date())"

                    update_prompt = f"""You are a Data Engineer. Analyze the requirements and update the STTM (Source-to-Target Mapping).
The target platform is {platform_label}. Use {platform_label} syntax for all expression logic.

CURRENT STTM DATA:
{lineage_for_llm}

USER'S REQUIREMENTS:
{sttm_additional_requirements}

TASK: Based on the requirements above, identify which rows need updates.

You MUST respond with ONLY a valid JSON object in this exact format:
{{"modifications": [{{"target_column": "COLUMN_NAME", "field": "Expression_Logic", "new_value": "new value"}}], "new_rows": [], "business_name_updates": [{{"target_column": "COLUMN_NAME", "new_business_name": "New Business Name"}}]}}

FIELDS YOU CAN UPDATE:
- Expression_Logic: transformation logic
- Target_Column_Business_Name: business-friendly name for the column

Example - if user says "ODS_CHANGE_DATE should be first day of month and rename to Month Start Date":
{{"modifications": [{{"target_column": "ODS_CHANGE_DATE", "field": "Expression_Logic", "new_value": "{date_trunc_example}"}}], "new_rows": [], "business_name_updates": [{{"target_column": "ODS_CHANGE_DATE", "new_business_name": "Month Start Date"}}]}}

RESPOND WITH ONLY THE JSON:"""

                    response, err = call_llm(model_to_use, update_prompt)
                    
                    if response:
                        st.session_state.lineage_raw_response = response
                        
                        try:
                            # Clean up response - find JSON content
                            json_str = response.strip()
                            
                            # Remove markdown code blocks if present
                            if '```json' in json_str:
                                json_str = json_str.split('```json')[1].split('```')[0]
                            elif '```' in json_str:
                                parts = json_str.split('```')
                                for part in parts:
                                    if '{' in part and '}' in part:
                                        json_str = part
                                        break
                            
                            # Find JSON object boundaries
                            start_idx = json_str.find('{')
                            end_idx = json_str.rfind('}') + 1
                            if start_idx != -1 and end_idx > start_idx:
                                json_str = json_str[start_idx:end_idx]
                            
                            import json
                            changes = json.loads(json_str)
                            
                            updated_df = working_lineage.copy()
                            changes_made = []
                            
                            # Apply modifications to Expression_Logic
                            if 'modifications' in changes and changes['modifications']:
                                for mod in changes['modifications']:
                                    target_col = mod.get('target_column', '').strip()
                                    new_value = mod.get('new_value', '').strip()
                                    field = mod.get('field', 'Expression_Logic')
                                    
                                    if target_col and new_value:
                                        mask = updated_df['Target_Column'].str.strip().str.upper() == target_col.upper()
                                        match_count = mask.sum()
                                        
                                        if match_count > 0:
                                            update_field = 'Expression_Logic'
                                            if field and field in updated_df.columns:
                                                update_field = field
                                            
                                            old_val = str(updated_df.loc[mask, update_field].values[0])[:50]
                                            updated_df.loc[mask, update_field] = new_value
                                            changes_made.append(f"✏️ **{target_col}** ({update_field}): `{old_val}` → `{new_value}`")
                                        else:
                                            changes_made.append(f"⚠️ Column **{target_col}** not found in STTM")
                            
                            # Apply business name updates
                            if 'business_name_updates' in changes and changes['business_name_updates']:
                                if 'Target_Column_Business_Name' not in updated_df.columns:
                                    updated_df['Target_Column_Business_Name'] = ''
                                
                                for bn_update in changes['business_name_updates']:
                                    target_col = bn_update.get('target_column', '').strip()
                                    new_bn = bn_update.get('new_business_name', '').strip()
                                    
                                    if target_col and new_bn:
                                        mask = updated_df['Target_Column'].str.strip().str.upper() == target_col.upper()
                                        if mask.any():
                                            old_bn = str(updated_df.loc[mask, 'Target_Column_Business_Name'].values[0])[:30] if 'Target_Column_Business_Name' in updated_df.columns else ''
                                            updated_df.loc[mask, 'Target_Column_Business_Name'] = new_bn
                                            changes_made.append(f"🏷️ **{target_col}** Business Name: `{old_bn}` → `{new_bn}`")
                            
                            # Add new rows
                            if 'new_rows' in changes and changes['new_rows']:
                                for new_row in changes['new_rows']:
                                    target_col_value = (new_row.get('Target_Column') or 
                                                       new_row.get('target_column') or 
                                                       new_row.get('TARGET_COLUMN'))
                                    
                                    if target_col_value:
                                        new_row_data = {col: '' for col in updated_df.columns}
                                        
                                        key_mapping = {
                                            'target_column': 'Target_Column',
                                            'source_table_insert': 'Source_Table_INSERT',
                                            'source_column_insert': 'Source_Column_INSERT',
                                            'expression_logic': 'Expression_Logic',
                                            'target_table': 'Target_Table',
                                            'mapping_name': 'Mapping_Name',
                                        }
                                        
                                        for llm_key, df_col in key_mapping.items():
                                            if llm_key in new_row and df_col in new_row_data:
                                                new_row_data[df_col] = new_row[llm_key]
                                        
                                        for key, value in new_row.items():
                                            if key in new_row_data:
                                                new_row_data[key] = value
                                        
                                        if not new_row_data.get('Mapping_Name') and len(updated_df) > 0:
                                            new_row_data['Mapping_Name'] = updated_df['Mapping_Name'].iloc[0]
                                        if not new_row_data.get('Target_Table') and len(updated_df) > 0:
                                            new_row_data['Target_Table'] = updated_df['Target_Table'].iloc[0]
                                        
                                        updated_df = pd.concat([updated_df, pd.DataFrame([new_row_data])], ignore_index=True)
                                        changes_made.append(f"➕ Added: **{target_col_value}** with Expression_Logic: `{new_row_data.get('Expression_Logic', 'N/A')}`")
                            
                            # Store in session state
                            st.session_state.updated_lineage = updated_df
                            st.session_state.lineage_changes = changes_made
                            
                            if changes_made:
                                st.session_state.lineage_update_success = True
                                st.success(f"✅ {len(changes_made)} change(s) applied to STTM!")
                                st.rerun()
                            else:
                                st.session_state.lineage_update_success = False
                                st.warning("LLM returned empty modifications. Check if your requirements are specific enough.")
                                
                        except Exception as e:
                            st.error(f"Error processing response: {e}")
                            st.session_state.lineage_changes = []
                            st.session_state.lineage_update_success = False
                    else:
                        st.error(f"LLM Error: {err}")
    
    # ==================== DISPLAY CHANGES MADE ====================
    if st.session_state.get('lineage_changes'):
        st.markdown("---")
        st.markdown("#### ✅ Changes Applied")
        for change in st.session_state.lineage_changes:
            st.markdown(change)
    
    # Debug expander - show raw LLM response
    if st.session_state.get('lineage_raw_response'):
        with st.expander("🔍 Debug: View Raw LLM Response", expanded=False):
            st.code(st.session_state.lineage_raw_response)
    
    # ==================== DISPLAY STTM TABLES ====================
    st.markdown("---")
    
    # Always show Current STTM
    st.markdown("#### 📊 Current Source-to-Target Mapping")
    st.dataframe(df_lineage, use_container_width=True, height=300)
    st.caption(f"Total rows: {len(df_lineage)}")
    
    # Show Updated STTM if available
    has_updates = st.session_state.get('updated_lineage') is not None and st.session_state.get('lineage_update_success', False)
    
    if has_updates:
        st.markdown("---")
        st.markdown("#### 📊 Updated Source-to-Target Mapping")
        st.success("✅ This mapping has been modified based on your requirements.")
        
        display_df = st.session_state.updated_lineage
        st.dataframe(display_df, use_container_width=True, height=300)
        st.caption(f"Total rows: {len(display_df)}")
        
        # Reset button and download options
        col_reset, col_spacer, col_dl1, col_dl2 = st.columns([1, 1, 1, 1])
        with col_reset:
            if st.button("🔄 Reset to Original", key="reset_sttm"):
                st.session_state.updated_lineage = None
                st.session_state.lineage_changes = []
                st.session_state.lineage_update_success = False
                st.session_state.lineage_raw_response = None
                st.session_state.business_names_generated = False
                st.session_state.column_mappings = {}
                st.rerun()
        with col_dl1:
            csv = display_df.to_csv(index=False)
            st.download_button(
                label="📥 CSV",
                data=csv,
                file_name="updated_sttm.csv",
                mime="text/csv",
                key="download_updated_sttm_csv"
            )
        with col_dl2:
            import io
            lineage_excel = io.BytesIO()
            display_df.to_excel(lineage_excel, index=False, sheet_name='Updated_STTM')
            lineage_excel.seek(0)
            st.download_button(
                label="📥 Excel",
                data=lineage_excel,
                file_name="updated_sttm.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_updated_sttm_xlsx"
            )


def render_workflow_tab(data):
    """Render the Workflows overview tab."""
    st.markdown("### Workflow Overview")

    df_wf = data.get('df_workflows', pd.DataFrame())
    if df_wf is not None and len(df_wf) > 0:
        st.dataframe(df_wf, use_container_width=True, height=300)
        st.caption(f"Total workflows: {len(df_wf)}")
    else:
        st.info("No workflow data available")

    # Show workflow summary text
    wf_summary = data.get('workflow_summary', '')
    if wf_summary:
        with st.expander("View Full Workflow Summary (sent to LLM)", expanded=False):
            st.code(wf_summary, language='text')


def render_execution_pipeline_tab(data):
    """Render the Execution Pipeline tab showing session→mapping correlation."""
    st.markdown("### Execution Pipeline")
    st.markdown("Each session in the workflow is linked to the mapping it executes, showing the source→transform→target flow.")

    session_details = data.get('session_mapping_details', {})
    missing_mappings = data.get('missing_mappings', [])
    workflow_parsers = data.get('workflow_parsers', [])

    if missing_mappings:
        st.warning(
            f"**{len(missing_mappings)} mapping(s) not uploaded:** {', '.join(missing_mappings)}"
        )

    # Determine execution order from workflow DAG
    execution_order = []
    for wf_parser in workflow_parsers:
        for wf_name, wf in wf_parser.workflows.items():
            for task_name in wf.get('execution_order', []):
                ti = wf['task_instances'].get(task_name, {})
                if ti.get('task_type') == 'Session':
                    sess_task_name = ti.get('task_name', '')
                    if sess_task_name in session_details:
                        execution_order.append(sess_task_name)

    # Fall back to dict order if no DAG
    if not execution_order:
        execution_order = list(session_details.keys())

    for idx, sess_name in enumerate(execution_order, 1):
        detail = session_details.get(sess_name, {})
        mapping_name = detail.get('mapping_name', '?')
        mapping_data = detail.get('mapping')
        src_tables = detail.get('source_tables', [])
        tgt_tables = detail.get('target_tables', [])
        sess = detail.get('session', {})

        # Header
        status_icon = "✅" if mapping_data else "⚠️"
        with st.expander(f"{status_icon} Step {idx}: {sess_name} → {mapping_name}", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**Session:** `{sess_name}`")
                st.markdown(f"**Mapping:** `{mapping_name}`")
                if sess.get('description'):
                    st.markdown(f"**Description:** {sess['description']}")
            with col2:
                st.markdown(f"**Source Rows Treatment:** {sess.get('attributes', {}).get('Treat source rows as', 'N/A')}")
                st.markdown(f"**Recovery Strategy:** {sess.get('attributes', {}).get('Recovery Strategy', 'N/A')}")

            if not mapping_data:
                st.warning("Mapping XML not uploaded — detailed pipeline not available.")
                continue

            st.markdown("---")

            # Source → Transform → Target flow
            col_src, col_arrow1, col_trans, col_arrow2, col_tgt = st.columns([3, 1, 4, 1, 3])

            with col_src:
                st.markdown("**Sources**")
                for t in src_tables:
                    st.markdown(f"- `{t}`")

            with col_arrow1:
                st.markdown("")
                st.markdown("→")

            with col_trans:
                st.markdown("**Transformations**")
                transforms = mapping_data.get('transformations', {})
                if transforms:
                    from collections import Counter
                    type_counts = Counter(t.get('type', 'Unknown') for t in transforms.values())
                    for t_type, count in sorted(type_counts.items()):
                        st.markdown(f"- {t_type}: **{count}**")

            with col_arrow2:
                st.markdown("")
                st.markdown("→")

            with col_tgt:
                st.markdown("**Targets**")
                for t in tgt_tables:
                    st.markdown(f"- `{t}`")

            # Load strategy
            load_strategies = []
            for conn_info in sess.get('connections', []):
                if isinstance(conn_info, dict) and conn_info.get('type') == 'WRITER':
                    attrs = conn_info.get('attributes', {})
                    insert = attrs.get('Insert', '')
                    update = attrs.get('Update as Update', '')
                    upsert = attrs.get('Update else Insert', '')
                    truncate = attrs.get('Truncate target table option', '')
                    parts = []
                    if insert == 'YES': parts.append('Insert')
                    if update == 'YES': parts.append('Update')
                    if upsert == 'YES': parts.append('Upsert')
                    if truncate == 'YES': parts.append('Truncate')
                    if parts:
                        inst = conn_info.get('instance_name', '')
                        load_strategies.append(f"`{inst}`: {', '.join(parts)}")

            if load_strategies:
                st.markdown("**Load Strategy:** " + " | ".join(load_strategies))

            # Pre/Post SQL
            if sess.get('pre_sql'):
                st.markdown("**Pre-SQL:**")
                for ps in sess['pre_sql']:
                    st.code(f"[{ps['instance']}] {ps['sql']}", language='sql')
            if sess.get('post_sql'):
                st.markdown("**Post-SQL:**")
                for ps in sess['post_sql']:
                    st.code(f"[{ps['instance']}] {ps['sql']}", language='sql')


def render_task_dag_tab(data):
    """Render the Task DAG (execution dependency graph) tab."""
    st.markdown("### Task Execution DAG")

    df_links = data.get('df_links', pd.DataFrame())
    df_tasks = data.get('df_tasks', pd.DataFrame())

    if df_links is not None and len(df_links) > 0:
        # Render DAG using graphviz
        if GRAPHVIZ_AVAILABLE:
            try:
                import graphviz
                dot = graphviz.Digraph(comment='Workflow DAG', format='svg')
                dot.attr(rankdir='LR', bgcolor='transparent')
                dot.attr('node', shape='box', style='rounded,filled', fontsize='10')

                # Collect task types for coloring
                task_types = {}
                if df_tasks is not None and len(df_tasks) > 0:
                    for _, row in df_tasks.iterrows():
                        task_types[row.get('Task_Instance', '')] = row.get('Task_Type', '')

                type_colors = {
                    'Start': '#90EE90',
                    'Session': '#87CEEB',
                    'Command': '#FFD700',
                    'Email': '#FFB6C1',
                }

                # Add nodes
                nodes = set()
                for _, row in df_links.iterrows():
                    for task in [row['from_task'], row['to_task']]:
                        if task not in nodes:
                            nodes.add(task)
                            t_type = task_types.get(task, '')
                            color = type_colors.get(t_type, '#FFFFFF')
                            label = f"{task}\n[{t_type}]" if t_type else task
                            dot.node(task, label=label, fillcolor=color)

                # Add edges with conditions
                for _, row in df_links.iterrows():
                    cond = row.get('condition', '')
                    label = ''
                    if cond:
                        # Shorten condition for display
                        if 'SUCCEEDED' in cond:
                            label = 'OK'
                        elif 'FAILED' in cond:
                            label = 'FAIL'
                        else:
                            label = cond[:30]
                    dot.edge(row['from_task'], row['to_task'], label=label)

                st.graphviz_chart(dot.source, use_container_width=True)
            except Exception as e:
                st.warning(f"Could not render DAG: {e}")

        # Show links table
        st.markdown("#### Task Dependencies")
        st.dataframe(df_links, use_container_width=True, height=250)
        st.caption(f"Total links: {len(df_links)}")
    else:
        st.info("No task dependency data available")


def render_instructions():
    """Render landing page instructions when no files are uploaded."""

    # ── Supported formats banner ──────────────────────────────────────────
    st.markdown("""
<p style="margin-bottom:10px; font-size:0.85rem; color:#64748b; font-weight:600; letter-spacing:0.5px;">
SUPPORTED FILE FORMATS
</p>
<div style="margin-bottom:28px;">
  <span class="fmt-badge">📄 XML</span>
  <span class="fmt-badge">🗄️ SQL</span>
  <span class="fmt-badge green">📓 IPYNB</span>
  <span class="fmt-badge purple">🐍 PY</span>
  <span class="fmt-badge orange">📦 DBC</span>
</div>
""", unsafe_allow_html=True)

    # ── Feature cards ─────────────────────────────────────────────────────
    st.markdown("""
<p style="margin-bottom:14px; font-size:0.85rem; color:#64748b; font-weight:600; letter-spacing:0.5px;">
WHAT YOU CAN GENERATE
</p>
""", unsafe_allow_html=True)

    cols = st.columns(4)
    features = [
        ("📋", "Technical Design Document", "AI-generated TDD with business context, data flows, and transformation logic."),
        ("🔗", "Source-to-Target Mapping", "Complete STTM with 22-column lineage: sources, targets, expressions, keys."),
        ("🗺️", "Lineage Diagrams", "Visual data flow diagrams showing how data moves from source to target."),
        ("🔷", "Data Models", "Conceptual, Logical and Physical ERDs generated from your parsed schema."),
        ("⚡", "SQL Generator", "Platform-native DDL, DML, MERGE, and Stored Procedures / Python Notebooks."),
        ("📓", "Notebook Overview", "Render Databricks notebook cells as a readable, searchable document."),
        ("📊", "Parser Output", "Raw parsed tables, fields, transformations and connector data."),
        ("🔄", "Update STTM", "AI-assisted STTM refinement with business name enrichment."),
    ]

    # Show in two rows of 4
    for row_start in range(0, len(features), 4):
        row_cols = st.columns(4)
        for i, col in enumerate(row_cols):
            fi = row_start + i
            if fi < len(features):
                icon, title, desc = features[fi]
                with col:
                    st.markdown(f"""
<div class="feature-card">
  <div class="feature-icon">{icon}</div>
  <div class="feature-title">{title}</div>
  <div class="feature-desc">{desc}</div>
</div>
""", unsafe_allow_html=True)
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # ── How it works ──────────────────────────────────────────────────────
    st.markdown("""
<p style="margin-bottom:14px; font-size:0.85rem; color:#64748b; font-weight:600; letter-spacing:0.5px;">
HOW IT WORKS
</p>
""", unsafe_allow_html=True)

    steps = [
        ("1", "Upload Source Files",
         "Drop in Informatica XML mappings/workflows, SQL files, or Databricks notebooks (.ipynb, .py, .dbc)."),
        ("2", "Configure (Optional)",
         "Add business context to guide the AI. Select your LLM model and target SQL platform from the sidebar."),
        ("3", "Parse & Generate",
         "Click the button — TDG parses lineage, extracts schema, and triggers AI document generation."),
        ("4", "Explore Output Tabs",
         "Navigate TDD, STTM, Lineage Diagram, Data Model, SQL Generator, and Notebook Overview tabs."),
        ("5", "Export",
         "Download as Excel, Word, or individual SQL/Python files for your team."),
    ]

    steps_html = ""
    for num, title, desc in steps:
        steps_html += f"""
<div class="how-step">
  <div class="step-num">{num}</div>
  <div class="step-body">
    <div class="step-title">{title}</div>
    <div class="step-desc">{desc}</div>
  </div>
</div>"""

    st.markdown(f"""
<div style="background:white; border:1px solid #e2e8f0; border-radius:14px;
            padding:20px 24px; box-shadow:0 2px 8px rgba(0,0,0,0.04);">
{steps_html}
</div>
""", unsafe_allow_html=True)


if __name__ == "__main__":
    main()