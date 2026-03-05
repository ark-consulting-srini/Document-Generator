"""
Informatica Workflow Conversion Report Generator

Generates a comprehensive LLM-powered conversion report similar to
sample_outputs/Informatica_Workflow_Conversion_Report.pdf.

Sections:
1. Executive Summary
2. Workflow Architecture Overview
3. Stage-by-Stage Breakdown
4. Detailed Transformation Logic
5. Dimension Lookup Details
6. Update Strategy
7. Business Rules & Data Quality
8. Recommended dbt Project Structure
9. Source Column Mapping Reference
"""

import re
import pandas as pd
from typing import Dict, Tuple, Optional

from generators.prompts import create_conversion_report_prompt
from utils.platform_utils import call_llm


def _safe(val) -> str:
    """Return empty string for None/NaN, otherwise stripped string."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ''
    return str(val).strip()


def _prepare_report_context(lineage_df: pd.DataFrame,
                             sources_dict: Dict,
                             targets_dict: Dict,
                             mappings_dict: Dict,
                             workflow_data: Dict = None) -> str:
    """
    Build a structured context summary for the LLM prompt.

    Extracts key facts from parsed data to keep the prompt focused and
    within token limits.
    """
    parts = []

    # --- Source tables ---
    parts.append("## Source Tables")
    if sources_dict:
        for name, details in list(sources_dict.items())[:10]:
            if isinstance(details, dict):
                tbl_name = details.get('TABLE_NAME', details.get('name', name))
                col_count = len(details.get('columns', []))
                parts.append(f"- {tbl_name} ({col_count} columns)")
            else:
                parts.append(f"- {name}")
    parts.append("")

    # --- Target tables ---
    parts.append("## Target Tables")
    if targets_dict:
        for name, details in list(targets_dict.items())[:10]:
            if isinstance(details, dict):
                tbl_name = details.get('TABLE_NAME', details.get('name', name))
                col_count = len(details.get('columns', []))
                parts.append(f"- {tbl_name} ({col_count} columns)")
            else:
                parts.append(f"- {name}")
    parts.append("")

    # --- Lineage summary ---
    if lineage_df is not None and not lineage_df.empty:
        parts.append(f"## Lineage Summary ({len(lineage_df)} column mappings)")

        # Count by category
        lkp_count = 0
        direct_count = 0
        unconnected_count = 0
        for _, row in lineage_df.iterrows():
            src = _safe(row.get('Source_Table_INSERT')).upper()
            lkp = _safe(row.get('Lookup_Condition'))
            if src == 'UNCONNECTED':
                unconnected_count += 1
            elif lkp:
                lkp_count += 1
            else:
                direct_count += 1

        parts.append(f"- Direct mappings: {direct_count}")
        parts.append(f"- Lookup mappings: {lkp_count}")
        parts.append(f"- Unconnected/Generated: {unconnected_count}")
        parts.append("")

        # Sample lineage rows (first 20)
        parts.append("### Sample Mappings (first 20)")
        for i, (_, row) in enumerate(lineage_df.iterrows()):
            if i >= 20:
                parts.append(f"... and {len(lineage_df) - 20} more")
                break
            src_tbl = _safe(row.get('Source_Table_INSERT'))
            src_col = _safe(row.get('Source_Column_INSERT'))
            tgt_tbl = _safe(row.get('Target_Table'))
            tgt_col = _safe(row.get('Target_Column'))
            expr = _safe(row.get('Expression_Logic'))
            lkp = _safe(row.get('Lookup_Condition'))

            line = f"- {src_tbl}.{src_col} -> {tgt_tbl}.{tgt_col}"
            if expr:
                line += f" | Expr: {expr[:80]}"
            if lkp:
                line += f" | Lookup: {lkp[:80]}"
            parts.append(line)
        parts.append("")

    # --- Transformation instances ---
    if mappings_dict:
        parts.append("## Transformation Instances")
        for mapping_name, mapping_data in list(mappings_dict.items())[:5]:
            parts.append(f"\n### Mapping: {mapping_name}")
            instances = mapping_data.get('instances', [])
            if isinstance(instances, list):
                for inst in instances[:30]:
                    if isinstance(inst, dict):
                        iname = inst.get('INSTANCE_NAME', inst.get('name', ''))
                        itype = inst.get('TRANSFORMATION_TYPE', inst.get('type', ''))
                        parts.append(f"  - {iname} ({itype})")
        parts.append("")

    # --- Workflow data ---
    if workflow_data:
        parts.append("## Workflow Information")
        if isinstance(workflow_data, dict):
            wf_name = workflow_data.get('workflow_name', workflow_data.get('name', ''))
            if wf_name:
                parts.append(f"- Workflow: {wf_name}")
            sessions = workflow_data.get('sessions', [])
            if sessions:
                parts.append(f"- Sessions: {len(sessions)}")
                for sess in sessions[:10]:
                    if isinstance(sess, dict):
                        sname = sess.get('name', sess.get('SESSION_NAME', ''))
                        parts.append(f"  - {sname}")
        parts.append("")

    # --- Lookup conditions ---
    if lineage_df is not None:
        lkp_rows = lineage_df[lineage_df['Lookup_Condition'].notna() & (lineage_df['Lookup_Condition'] != '')]
        if len(lkp_rows) > 0:
            parts.append("## Lookup Conditions")
            seen = set()
            for _, row in lkp_rows.iterrows():
                lkp = _safe(row.get('Lookup_Condition'))
                tgt = _safe(row.get('Target_Column'))
                if lkp not in seen:
                    seen.add(lkp)
                    parts.append(f"- {tgt}: {lkp[:200]}")
            parts.append("")

    # --- Expression logic samples ---
    if lineage_df is not None:
        expr_rows = lineage_df[lineage_df['Expression_Logic'].notna() & (lineage_df['Expression_Logic'] != '')]
        if len(expr_rows) > 0:
            parts.append("## Expression Logic (samples)")
            for i, (_, row) in enumerate(expr_rows.iterrows()):
                if i >= 15:
                    break
                tgt = _safe(row.get('Target_Column'))
                expr = _safe(row.get('Expression_Logic'))
                parts.append(f"- {tgt}: {expr[:150]}")
            parts.append("")

    return '\n'.join(parts)


def generate_conversion_report(lineage_df: pd.DataFrame,
                                sources_dict: Dict,
                                targets_dict: Dict,
                                mappings_dict: Dict,
                                workflow_data: Dict,
                                model_name: str,
                                business_context: str = "") -> Tuple[Optional[str], Optional[str]]:
    """
    Generate a comprehensive Informatica Workflow Conversion Report via LLM.

    Args:
        lineage_df: Parsed lineage DataFrame
        sources_dict: Source definitions
        targets_dict: Target definitions
        mappings_dict: Mapping structures
        workflow_data: Workflow parser output
        model_name: LLM model name for call_llm()
        business_context: Optional business context

    Returns:
        Tuple of (markdown_report, error_message)
        If successful, error_message is None.
    """
    context = _prepare_report_context(
        lineage_df, sources_dict, targets_dict, mappings_dict, workflow_data
    )

    prompt = create_conversion_report_prompt(context, business_context)

    response, error = call_llm(model_name, prompt, max_tokens=8000)

    if error:
        return None, f"LLM error: {error}"
    if not response:
        return None, "LLM returned empty response"

    return response.strip(), None
