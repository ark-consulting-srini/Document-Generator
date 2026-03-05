"""
Session State Management — Technical Document Generator (TDG)

Centralized session state initialization and management.
"""

import streamlit as st
from typing import Dict, Any, Optional


def init_session_state():
    """Initialize all session state variables."""
    
    # Parsed data
    if 'parsed_data' not in st.session_state:
        st.session_state.parsed_data = None

    # SQL file tracking
    if 'has_sql_files' not in st.session_state:
        st.session_state.has_sql_files = False
    
    # File tracking
    if 'last_uploaded_files' not in st.session_state:
        st.session_state.last_uploaded_files = None
    if 'uploader_key' not in st.session_state:
        st.session_state.uploader_key = 0
    
    # UI state
    if 'active_tab' not in st.session_state:
        st.session_state.active_tab = 0
    
    # BRD configuration
    if 'main_business_context' not in st.session_state:
        st.session_state.main_business_context = ""
    if 'main_additional_requirements' not in st.session_state:
        st.session_state.main_additional_requirements = ""
    
    # TDD generation mode
    if 'tdd_generation_mode' not in st.session_state:
        st.session_state.tdd_generation_mode = "auto"

    # TDD results
    if 'xml_tdd_result' not in st.session_state:
        st.session_state.xml_tdd_result = None
    if 'tdd_use_optimized' not in st.session_state:
        st.session_state.tdd_use_optimized = False
    if 'tdd_estimated_tokens' not in st.session_state:
        st.session_state.tdd_estimated_tokens = 0
    if 'tdd_mode_reason' not in st.session_state:
        st.session_state.tdd_mode_reason = ""
    if 'tdd_mode_used' not in st.session_state:
        st.session_state.tdd_mode_used = "full"
    
    # Data models
    if 'data_models_generated' not in st.session_state:
        st.session_state.data_models_generated = False
    if 'data_models' not in st.session_state:
        st.session_state.data_models = {}
    if 'selected_model_type' not in st.session_state:
        st.session_state.selected_model_type = 'conceptual'
    
    # Update STTM
    if 'sttm_additional_requirements' not in st.session_state:
        st.session_state.sttm_additional_requirements = ""
    if 'updated_lineage' not in st.session_state:
        st.session_state.updated_lineage = None
    if 'lineage_changes' not in st.session_state:
        st.session_state.lineage_changes = []
    if 'lineage_update_success' not in st.session_state:
        st.session_state.lineage_update_success = False
    if 'lineage_raw_response' not in st.session_state:
        st.session_state.lineage_raw_response = None
    if 'business_names_generated' not in st.session_state:
        st.session_state.business_names_generated = False
    if 'column_mappings' not in st.session_state:
        st.session_state.column_mappings = {}
    
    # Target platform (user preference — not cleared on file change)
    # Options: 'postgresql', 'mssql', 'databricks_sql', 'databricks_python'
    if 'target_platform' not in st.session_state:
        st.session_state.target_platform = 'databricks_sql'

    # Notebook parsing flag (controls sidebar checkbox visibility)
    if 'has_notebook_parsed' not in st.session_state:
        st.session_state.has_notebook_parsed = False

    # SQL Generator
    if 'sql_generator_results' not in st.session_state:
        st.session_state.sql_generator_results = {}
    if 'selected_sql_type' not in st.session_state:
        st.session_state.selected_sql_type = 'DDL'
    if 'sql_target_schema' not in st.session_state:
        st.session_state.sql_target_schema = 'TARGET'
    if 'sql_source_schema' not in st.session_state:
        st.session_state.sql_source_schema = 'SOURCE'

    # Enhanced STTM / Lineage Diagram / Conversion Report
    if 'enhanced_sttm_df' not in st.session_state:
        st.session_state.enhanced_sttm_df = None
    if 'lineage_diagrams_md' not in st.session_state:
        st.session_state.lineage_diagrams_md = None
    if 'conversion_report_md' not in st.session_state:
        st.session_state.conversion_report_md = None
    if 'conversion_report_error' not in st.session_state:
        st.session_state.conversion_report_error = None

    # Token estimation cache
    # Note: preview_token_estimate is NOT initialized here
    # It's created lazily when files are uploaded


def get_parsed_data() -> Optional[Dict[str, Any]]:
    """Get parsed data from session state."""
    return st.session_state.get('parsed_data')


def set_parsed_data(data: Dict[str, Any]):
    """Set parsed data in session state."""
    st.session_state.parsed_data = data


def clear_parsed_data():
    """Clear all parsed data and related state."""
    st.session_state.parsed_data = None
    st.session_state.data_models_generated = False
    st.session_state.xml_tdd_result = None
    st.session_state.tdd_use_optimized = False
    st.session_state.tdd_estimated_tokens = 0
    st.session_state.tdd_mode_reason = ""
    st.session_state.tdd_mode_used = "full"
    st.session_state.updated_lineage = None
    st.session_state.lineage_changes = []
    st.session_state.lineage_update_success = False
    st.session_state.sql_generator_results = {}
    st.session_state.has_notebook_parsed = False
    st.session_state.enhanced_sttm_df = None
    st.session_state.lineage_diagrams_md = None
    st.session_state.conversion_report_md = None
    st.session_state.conversion_report_error = None

    if 'preview_token_estimate' in st.session_state:
        del st.session_state.preview_token_estimate


def check_file_changes(uploaded_files) -> bool:
    """
    Check if uploaded files have changed.
    
    Args:
        uploaded_files: List of uploaded files
        
    Returns:
        True if files changed, False otherwise
    """
    current_file_names = [f.name for f in uploaded_files] if uploaded_files else []
    
    if st.session_state.last_uploaded_files != current_file_names:
        st.session_state.last_uploaded_files = current_file_names
        clear_parsed_data()
        return True
    
    return False
