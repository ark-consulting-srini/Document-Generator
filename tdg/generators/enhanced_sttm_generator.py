"""
Enhanced STTM (Source-to-Target Mapping) Generator

Transforms the 22-column lineage DataFrame into a clean 10-column STTM
matching the reference format in sample_outputs/source_to_target_mapping.csv.

Columns produced:
  Target Table, Target Column, Target Data Type, Source Type,
  Source Table, Source Column, Transformation Logic,
  Lookup Table, Lookup Condition, Notes
"""

import re
import pandas as pd
from typing import Dict, Optional


def _safe(val) -> str:
    """Return empty string for None/NaN, otherwise stripped string."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ''
    return str(val).strip()


# ---------------------------------------------------------------------------
# Source-Type classification
# ---------------------------------------------------------------------------

def _classify_source_type(row: dict) -> str:
    """Classify a lineage row as Direct, Lookup, or Generated."""
    src_table = _safe(row.get('Source_Table_INSERT'))
    src_col = _safe(row.get('Source_Column_INSERT'))
    lkp_cond = _safe(row.get('Lookup_Condition'))
    expr = _safe(row.get('Expression_Logic'))
    trans = _safe(row.get('Transformations_INSERT'))

    # UNCONNECTED / system-generated
    if src_table.upper() == 'UNCONNECTED' or src_col.upper() == 'UNCONNECTED':
        return 'Generated'

    # Lookup-based columns (lookup condition present or transformation path
    # goes through a LKP_ transformation)
    if lkp_cond:
        return 'Lookup'
    if 'LKP_' in trans.upper():
        return 'Lookup'

    # Sequences, constants, system values
    if 'SYSTEM' in src_table.upper() or 'Sequence' in expr:
        return 'Generated'

    return 'Direct'


# ---------------------------------------------------------------------------
# Lookup table extraction
# ---------------------------------------------------------------------------

_LKP_TABLE_RE = re.compile(r'LKP_\w+', re.IGNORECASE)


def _extract_lookup_table(row: dict, targets_dict: Dict) -> str:
    """Extract the dimension/lookup table name from a lineage row."""
    lkp_cond = _safe(row.get('Lookup_Condition'))
    trans = _safe(row.get('Transformations_INSERT'))

    # Try to get the table name from the lookup condition text
    # In the parsed data, Lookup_Condition often contains the table name
    # or we can infer it from the LKP_ transformation name
    lkp_match = _LKP_TABLE_RE.search(trans) or _LKP_TABLE_RE.search(lkp_cond)
    if lkp_match:
        lkp_name = lkp_match.group(0)
        # Try to map LKP_<name> back to an actual table
        # e.g. LKP_PRFT_CTR → PROFIT_CENTER_DIM
        # The actual table name is often in the SQL override or lookup condition
        sql_ovr = _safe(row.get('SQL_Override'))
        for candidate in [sql_ovr, lkp_cond]:
            # Look for FROM <table> pattern
            from_match = re.search(r'FROM\s+(\w+)', candidate, re.IGNORECASE)
            if from_match:
                return from_match.group(1)

    # If Source_Table_INSERT is a lookup table (not the main source)
    src_table = _safe(row.get('Source_Table_INSERT'))
    if src_table and src_table.upper().endswith('_DIM'):
        return src_table

    return ''


# ---------------------------------------------------------------------------
# Notes generation
# ---------------------------------------------------------------------------

def _generate_notes(row: dict, source_type: str, lookup_table: str) -> str:
    """Generate contextual notes for a lineage row."""
    tgt_col = _safe(row.get('Target_Column'))
    tgt_col_upper = tgt_col.upper()

    if source_type == 'Lookup':
        src_col = _safe(row.get('Source_Column_INSERT'))
        if 'C_' == tgt_col_upper[:2]:
            return f"Current {_friendly_dim(lookup_table)} dimension key"
        elif 'H_' == tgt_col_upper[:2]:
            return f"Historical {_friendly_dim(lookup_table)} dimension key"
        if lookup_table:
            return f"FK lookup to {lookup_table}"
        return "Dimension lookup"

    if source_type == 'Generated':
        if any(k in tgt_col_upper for k in ['LOAD_DATE', 'INSERT_TS', 'UPDATE_TS', 'ETL_LOAD', 'ETL_UPDATE']):
            return "ETL load timestamp"
        if 'BATCH_ID' in tgt_col_upper:
            return "ETL batch identifier"
        return "System-generated / hardcoded value"

    # Direct — pattern-based notes
    if tgt_col_upper.startswith('NK_'):
        if 'DATE' in tgt_col_upper or 'DT' in tgt_col_upper:
            return _date_note(tgt_col)
        return "Natural key / business identifier"

    if tgt_col_upper.startswith('AMT_') or 'CASH_DISC' in tgt_col_upper:
        return _amount_note(tgt_col)

    if tgt_col_upper.endswith('_CD') or tgt_col_upper.endswith('_CODE'):
        return _code_note(tgt_col)

    if tgt_col_upper.endswith('_TXT') or tgt_col_upper in ('SGTXT', 'LN_ITEM_TXT'):
        return "Text field"

    if 'DATE' in tgt_col_upper or 'DT' in tgt_col_upper:
        return _date_note(tgt_col)

    return ''


def _friendly_dim(table_name: str) -> str:
    """Convert PROFIT_CENTER_DIM → profit center."""
    name = table_name.replace('_DIM', '').replace('_', ' ').lower()
    return name


_DATE_KEYWORDS = {
    'DOC': 'Document date', 'PSTG': 'Posting date', 'ENTRY': 'Entry date',
    'CLRNG': 'Clearing date', 'DUNN': 'Last dunning date',
    'NET_DUE': 'Net payment due date', 'CASH_DISC': 'Cash discount due date',
    'BSLN': 'Baseline date for payment', 'BANK_CHRG': 'Bank charges date',
}


def _date_note(col: str) -> str:
    col_upper = col.upper()
    for key, desc in _DATE_KEYWORDS.items():
        if key in col_upper:
            return desc
    return "Date field"


_AMT_KEYWORDS = {
    'LOCAL_CURR': 'Amount in local currency', 'DOC_CURR': 'Amount in document currency',
    'GRP_CURR': 'Amount in group currency', 'CASH_DISC_AMT': 'Cash discount amount',
    'CASH_DISC_BASE': 'Cash discount base amount', 'CASH_DISC_PCT': 'Cash discount percentage',
    'CASH_DISC_DAYS': 'Cash discount days',
}


def _amount_note(col: str) -> str:
    col_upper = col.upper()
    for key, desc in _AMT_KEYWORDS.items():
        if key in col_upper:
            return desc
    return "Monetary amount"


_CODE_KEYWORDS = {
    'DOC_TYPE': 'Document type code', 'PSTG_KEY': 'Posting key code',
    'DOC_STAT': 'Document status code', 'DUNN': 'Dunning code',
    'PYMT_MTHD': 'Payment method code', 'PYMT_TERMS': 'Payment terms code',
    'PYMT_BLOCK': 'Payment block code', 'TAX': 'Tax code',
    'CURR': 'Currency code', 'SP_GL': 'Special GL code',
}


def _code_note(col: str) -> str:
    col_upper = col.upper()
    for key, desc in _CODE_KEYWORDS.items():
        if key in col_upper:
            return desc
    return "Code field"


# ---------------------------------------------------------------------------
# Transformation logic cleanup
# ---------------------------------------------------------------------------

def _build_transformation_logic(row: dict) -> str:
    """Build clean transformation logic string from lineage row."""
    expr = _safe(row.get('Expression_Logic'))
    trans = _safe(row.get('Transformations_INSERT'))

    # If expression logic is present and meaningful, use it
    if expr and expr.upper() not in ('', 'N/A', 'NONE'):
        return expr

    # Fall back to transformations description
    if trans:
        return trans

    return 'Direct pass-through'


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------

def generate_enhanced_sttm(lineage_df: pd.DataFrame,
                           sources_dict: Dict,
                           targets_dict: Dict) -> pd.DataFrame:
    """
    Transform the parsed lineage DataFrame into 10-column Enhanced STTM.

    Args:
        lineage_df: DataFrame with 22 lineage columns from InformaticaLineageParser
        sources_dict: Dictionary of source definitions
        targets_dict: Dictionary of target definitions

    Returns:
        DataFrame with 10 columns matching the reference STTM format
    """
    if lineage_df is None or lineage_df.empty:
        return pd.DataFrame(columns=[
            'Target Table', 'Target Column', 'Target Data Type',
            'Source Type', 'Source Table', 'Source Column',
            'Transformation Logic', 'Lookup Table', 'Lookup Condition', 'Notes',
        ])

    rows = []
    for _, row in lineage_df.iterrows():
        row_dict = row.to_dict()

        tgt_table = _safe(row_dict.get('Target_Table'))
        tgt_col = _safe(row_dict.get('Target_Column'))
        raw_src_table = _safe(row_dict.get('Source_Table_INSERT'))
        raw_src_col = _safe(row_dict.get('Source_Column_INSERT'))

        # Skip self-referential rows (source table+column = target table+column)
        # These can arise from cross-mapping resolution when an intermediate
        # staging table is both source and target.
        if (raw_src_table and tgt_table
                and raw_src_table.upper() == tgt_table.upper()
                and raw_src_col.upper() == tgt_col.upper()):
            continue

        source_type = _classify_source_type(row_dict)
        lookup_table = ''

        if source_type == 'Lookup':
            lookup_table = _extract_lookup_table(row_dict, targets_dict)

        # Source table / column for Generated rows
        if source_type == 'Generated':
            src_table = 'N/A'
            src_col = 'N/A'
        else:
            src_table = raw_src_table
            src_col = raw_src_col

        rows.append({
            'Target Table': tgt_table,
            'Target Column': tgt_col,
            'Target Data Type': _safe(row_dict.get('Target_Datatype')),
            'Source Type': source_type,
            'Source Table': src_table,
            'Source Column': src_col,
            'Transformation Logic': _build_transformation_logic(row_dict),
            'Lookup Table': lookup_table,
            'Lookup Condition': _safe(row_dict.get('Lookup_Condition')),
            'Notes': _generate_notes(row_dict, source_type, lookup_table),
        })

    result = pd.DataFrame(rows)

    # Final deduplication on target table+column (keep first occurrence)
    if not result.empty:
        result = result.drop_duplicates(
            subset=['Target Table', 'Target Column'], keep='first'
        ).reset_index(drop=True)

    return result


def enhanced_sttm_to_csv(df: pd.DataFrame) -> str:
    """Export Enhanced STTM DataFrame to CSV string."""
    return df.to_csv(index=False)
