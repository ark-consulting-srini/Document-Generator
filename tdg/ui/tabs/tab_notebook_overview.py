"""
Notebook Overview Tab — Technical Document Generator (TDG)

Renders a Databricks notebook as a readable document:
  - Markdown cells are displayed as formatted text
  - SQL cells are shown with SQL syntax highlighting
  - Python cells are shown with Python syntax highlighting

Appears automatically when a .ipynb, .py, or .dbc file is parsed.
"""

import streamlit as st
from typing import Dict


def render_notebook_overview_tab(parsed_data: Dict):
    """
    Render the Notebook Overview tab.

    Args:
        parsed_data: The parsed_data dict from session state.
                     Expects 'notebook_cell_sequence' and 'notebook_names' keys.
    """
    cell_sequence = parsed_data.get('notebook_cell_sequence', [])
    notebook_names = parsed_data.get('notebook_names', [])

    if not cell_sequence:
        st.info("No notebook cells found. Upload a Databricks notebook (.ipynb, .py, or .dbc) to see the overview.")
        return

    # Header
    if notebook_names:
        if len(notebook_names) == 1:
            st.markdown(f"### 📓 {notebook_names[0]}")
        else:
            st.markdown(f"### 📓 Notebooks: {len(notebook_names)} files")
            with st.expander("Notebooks in this archive"):
                for name in notebook_names:
                    st.markdown(f"- {name}")

    # Stats
    md_count = sum(1 for c in cell_sequence if c['type'] == 'md')
    sql_count = sum(1 for c in cell_sequence if c['type'] == 'sql')
    py_count = sum(1 for c in cell_sequence if c['type'] == 'python')

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Cells", len(cell_sequence))
    col2.metric("📝 Markdown", md_count)
    col3.metric("🗄️ SQL", sql_count)
    col4.metric("🐍 Python", py_count)

    st.markdown("---")

    # Filter controls
    col_filter, col_jump = st.columns([2, 2])
    with col_filter:
        show_types = st.multiselect(
            "Show cell types",
            options=["Markdown", "SQL", "Python"],
            default=["Markdown", "SQL", "Python"],
            key="nb_overview_filter"
        )

    type_map = {"Markdown": "md", "SQL": "sql", "Python": "python"}
    visible_types = {type_map[t] for t in show_types}

    # Track current notebook section for headers
    current_notebook = None

    for i, cell in enumerate(cell_sequence):
        ctype = cell.get('type', 'python')
        content = cell.get('content', '')
        notebook = cell.get('notebook', '')

        if ctype not in visible_types:
            continue

        # Show notebook separator when switching notebooks (for .dbc with multiple notebooks)
        if notebook and notebook != current_notebook:
            current_notebook = notebook
            if len(notebook_names) > 1:
                st.markdown(f"---\n#### 📄 {notebook}")

        if ctype == 'md':
            # Render markdown natively — shows headers, bold, lists, tables
            st.markdown(content)

        elif ctype == 'sql':
            with st.container():
                st.code(content, language='sql')

        elif ctype == 'python':
            with st.container():
                st.code(content, language='python')

    st.markdown("---")
    st.caption(f"Total: {len(cell_sequence)} cells | Markdown: {md_count} | SQL: {sql_count} | Python: {py_count}")
