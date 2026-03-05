"""
Data Lineage Diagram Generator

Generates a Markdown document with 7 Mermaid diagrams + column lineage matrix
from parsed Informatica data, matching the reference format in
sample_outputs/DATA_LINEAGE_DIAGRAM.md.

All logic is programmatic — no LLM calls.
"""

import re
import pandas as pd
from typing import Dict, List, Tuple
from datetime import date


def _safe(val) -> str:
    """Return empty string for None/NaN, otherwise stripped string."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ''
    return str(val).strip()


# ═══════════════════════════════════════════════════════════════════════════
# Column classification helpers
# ═══════════════════════════════════════════════════════════════════════════

def _classify_target_columns(lineage_df: pd.DataFrame) -> Dict[str, List[dict]]:
    """Classify target columns into categories for diagram generation."""
    categories = {
        'lookup_fk': [],   # C_*/H_* dimension FK columns
        'natural_key': [], # NK_* columns (non-date)
        'date': [],        # Date columns
        'amount': [],      # AMT_* / monetary columns
        'code': [],        # *_CD code columns
        'text': [],        # Text columns
        'generated': [],   # UNCONNECTED / system-generated
    }

    for _, row in lineage_df.iterrows():
        rd = row.to_dict()
        tgt_col = _safe(rd.get('Target_Column')).upper()
        src_table = _safe(rd.get('Source_Table_INSERT')).upper()
        src_col = _safe(rd.get('Source_Column_INSERT'))
        lkp = _safe(rd.get('Lookup_Condition'))

        entry = {
            'target_col': _safe(rd.get('Target_Column')),
            'source_col': src_col,
            'source_table': _safe(rd.get('Source_Table_INSERT')),
            'target_dtype': _safe(rd.get('Target_Datatype')),
            'expression': _safe(rd.get('Expression_Logic')),
            'lookup_cond': lkp,
            'transformations': _safe(rd.get('Transformations_INSERT')),
        }

        if src_table == 'UNCONNECTED' or _safe(rd.get('Source_Column_INSERT')).upper() == 'UNCONNECTED':
            categories['generated'].append(entry)
        elif lkp or (tgt_col.startswith('C_') or tgt_col.startswith('H_')) and tgt_col.endswith('_ID'):
            categories['lookup_fk'].append(entry)
        elif tgt_col.startswith('NK_') and ('DATE' in tgt_col or 'DT' in tgt_col[-3:]):
            categories['date'].append(entry)
        elif tgt_col.startswith('NK_'):
            categories['natural_key'].append(entry)
        elif tgt_col.startswith('AMT_') or 'CASH_DISC' in tgt_col:
            categories['amount'].append(entry)
        elif tgt_col.endswith('_CD') or tgt_col.endswith('_CODE'):
            categories['code'].append(entry)
        elif any(kw in tgt_col for kw in ['TXT', 'SGTXT', 'REF_KEY', 'ASGN', 'DOC_NO']):
            categories['text'].append(entry)
        elif 'DATE' in tgt_col or tgt_col.endswith('_DT'):
            categories['date'].append(entry)
        else:
            # Default: treat as direct
            categories['text'].append(entry)

    return categories


def _extract_lookups(lineage_df: pd.DataFrame) -> List[dict]:
    """Extract unique lookup definitions from lineage data."""
    lookups = {}
    for _, row in lineage_df.iterrows():
        rd = row.to_dict()
        lkp_cond = _safe(rd.get('Lookup_Condition'))
        trans = _safe(rd.get('Transformations_INSERT'))
        tgt_col = _safe(rd.get('Target_Column'))

        if not lkp_cond and not (tgt_col.upper().startswith(('C_', 'H_')) and tgt_col.upper().endswith('_ID')):
            continue

        # Try to extract dim table name from SQL override or condition
        sql_ovr = _safe(rd.get('SQL_Override'))
        dim_table = ''
        for candidate in [sql_ovr, lkp_cond]:
            from_match = re.search(r'FROM\s+(\w+)', candidate, re.IGNORECASE)
            if from_match:
                dim_table = from_match.group(1)
                break

        if not dim_table:
            src_table = _safe(rd.get('Source_Table_INSERT'))
            if src_table.upper().endswith('_DIM'):
                dim_table = src_table

        if not dim_table:
            continue

        # Extract lookup key from condition
        lookup_key = ''
        key_match = re.search(r'(\w+)\s*=\s*IN_(\w+)', lkp_cond)
        if key_match:
            lookup_key = key_match.group(1)

        # Detect point-in-time
        pit = bool(re.search(r'VAL_FROM_DATE|VAL_TO_DATE', lkp_cond, re.IGNORECASE))

        # Source column that feeds this lookup
        src_col = _safe(rd.get('Source_Column_INSERT'))

        # Detect which source SAP field drives this lookup
        in_match = re.search(r'IN_(\w+)', lkp_cond)
        sap_field = in_match.group(1) if in_match else src_col

        if dim_table not in lookups:
            lookups[dim_table] = {
                'dim_table': dim_table,
                'lookup_key': lookup_key,
                'point_in_time': pit,
                'source_fields': set(),
                'target_cols': [],
            }
        lookups[dim_table]['source_fields'].add(sap_field)
        lookups[dim_table]['target_cols'].append(tgt_col)

    return list(lookups.values())


def _extract_transformations(mappings_dict: Dict) -> List[dict]:
    """Extract transformation instances from mappings_dict for pipeline diagram."""
    transforms = []
    if not mappings_dict:
        return transforms

    for mapping_name, mapping_data in mappings_dict.items():
        instances = mapping_data.get('instances', [])
        if isinstance(instances, list):
            for inst in instances:
                if isinstance(inst, dict):
                    transforms.append({
                        'name': inst.get('INSTANCE_NAME', inst.get('name', '')),
                        'type': inst.get('TRANSFORMATION_TYPE', inst.get('type', '')),
                        'mapping': mapping_name,
                    })
    return transforms


def _get_target_tables(targets_dict: Dict) -> List[str]:
    """Get list of target table names."""
    tables = []
    if targets_dict:
        for key in targets_dict:
            if isinstance(targets_dict[key], dict):
                name = targets_dict[key].get('TABLE_NAME', targets_dict[key].get('name', key))
            else:
                name = key
            tables.append(str(name))
    return tables


def _get_source_tables(sources_dict: Dict) -> List[str]:
    """Get list of source table names."""
    tables = []
    if sources_dict:
        for key in sources_dict:
            if isinstance(sources_dict[key], dict):
                name = sources_dict[key].get('TABLE_NAME', sources_dict[key].get('name', key))
            else:
                name = key
            tables.append(str(name))
    return tables


def _sanitize_id(name: str) -> str:
    """Sanitize a name for use as a Mermaid node ID."""
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)


# ═══════════════════════════════════════════════════════════════════════════
# Diagram generators
# ═══════════════════════════════════════════════════════════════════════════

def _diagram_1_pipeline_flow(lineage_df: pd.DataFrame, sources_dict: Dict,
                              targets_dict: Dict, mappings_dict: Dict) -> str:
    """Generate high-level pipeline flow diagram."""
    src_tables = _get_source_tables(sources_dict)
    tgt_tables = _get_target_tables(targets_dict)
    transforms = _extract_transformations(mappings_dict)

    # Count columns
    src_col_count = len(lineage_df) if lineage_df is not None else 0
    tgt_col_count = len(set(_safe(r.get('Target_Column')) for _, r in lineage_df.iterrows())) if lineage_df is not None else 0

    # Classify transforms by type
    expr_trans = [t for t in transforms if t['type'] in ('Expression', 'Expression Transformation')]
    flt_trans = [t for t in transforms if t['type'] in ('Filter', 'Filter Transformation')]
    agg_trans = [t for t in transforms if t['type'] in ('Aggregator', 'Aggregator Transformation')]
    lkp_trans = [t for t in transforms if t['type'] in ('Lookup Procedure', 'Lookup', 'Lookup Transformation')]
    rtr_trans = [t for t in transforms if t['type'] in ('Router', 'Router Transformation')]
    upd_trans = [t for t in transforms if t['type'] in ('Update Strategy', 'Update Strategy Transformation')]

    # Build source nodes
    src_nodes = []
    for tbl in src_tables[:3]:  # Limit to avoid huge diagrams
        src_nodes.append(f'        {_sanitize_id(tbl)}[("{tbl}<br/>{src_col_count} columns")]')

    lines = ['```mermaid', 'flowchart LR']
    lines.append('    subgraph SRC["SOURCE LAYER"]')
    lines.extend(src_nodes)
    lines.append('    end')
    lines.append('')

    # Staging subgraph
    stg_nodes = []
    if expr_trans:
        name = expr_trans[0]['name']
        stg_nodes.append(f'        EXP["Expression<br/>{name}<br/>Null handling"]')
    if flt_trans:
        name = flt_trans[0]['name']
        stg_nodes.append(f'        FLT["Filter<br/>{name}<br/>Remove blanks"]')
    if agg_trans:
        name = agg_trans[0]['name']
        stg_nodes.append(f'        AGG["Aggregator<br/>{name}<br/>Dedup by key"]')

    if stg_nodes:
        lines.append('    subgraph STG["STAGING (Bronze)"]')
        lines.extend(stg_nodes)
        lines.append('    end')
        lines.append('')

    # Enrichment subgraph
    if lkp_trans:
        lkp_count = len(lkp_trans)
        fk_count = len([r for _, r in lineage_df.iterrows()
                        if _safe(r.get('Target_Column')).upper().startswith(('C_', 'H_'))])
        lines.append('    subgraph ENR["ENRICHMENT"]')
        lines.append(f'        LKP["{lkp_count} Dimension<br/>Lookups<br/>{fk_count} FK columns"]')
        lines.append('    end')
        lines.append('')

    # Target subgraph
    tgt_nodes = []
    if rtr_trans or upd_trans:
        tgt_nodes.append('        RTR["Router<br/>Insert vs Update"]')
    for tbl in tgt_tables[:2]:
        tgt_nodes.append(f'        {_sanitize_id(tbl)}[("{tbl}<br/>{tgt_col_count} columns")]')

    lines.append('    subgraph TGT["TARGET (Silver)"]')
    lines.extend(tgt_nodes)
    lines.append('    end')
    lines.append('')

    # Edges — simplified chain
    chain = []
    if src_tables:
        chain.append(_sanitize_id(src_tables[0]))
    if expr_trans:
        chain.append('EXP')
    if flt_trans:
        chain.append('FLT')
    if agg_trans:
        chain.append('AGG')
    if lkp_trans:
        chain.append('LKP')
    if rtr_trans or upd_trans:
        chain.append('RTR')
    if tgt_tables:
        chain.append(_sanitize_id(tgt_tables[0]))

    if chain:
        lines.append('    ' + ' --> '.join(chain))

    lines.append('')
    lines.append('    style SRC fill:#e1f5fe')
    if stg_nodes:
        lines.append('    style STG fill:#fff3e0')
    if lkp_trans:
        lines.append('    style ENR fill:#f3e5f5')
    lines.append('    style TGT fill:#e8f5e9')
    lines.append('```')

    return '\n'.join(lines)


def _diagram_2_dimension_lookups(lineage_df: pd.DataFrame) -> str:
    """Generate dimension lookup data flow diagram."""
    lookups = _extract_lookups(lineage_df)
    if not lookups:
        return '_No dimension lookups detected._'

    lines = ['```mermaid', 'flowchart TB']

    # Source record subgraph
    all_source_fields = set()
    for lkp in lookups:
        all_source_fields.update(lkp['source_fields'])

    lines.append('    subgraph SOURCE["Source Record"]')
    for field in sorted(all_source_fields):
        lines.append(f'        S_{_sanitize_id(field)}["{field}"]')
    lines.append('    end')
    lines.append('')

    # Dimension tables subgraph
    lines.append('    subgraph DIMS["Dimension Tables"]')
    for lkp in lookups:
        dim_id = _sanitize_id(lkp['dim_table'])
        lines.append(f'        D_{dim_id}[("{lkp["dim_table"]}")]')
    lines.append('    end')
    lines.append('')

    # Target FK columns subgraph
    lines.append('    subgraph TARGET["Target FK Columns"]')
    for lkp in lookups:
        for col in lkp['target_cols']:
            lines.append(f'        T_{_sanitize_id(col)}["{col}"]')
    lines.append('    end')
    lines.append('')

    # Edges: source → dim → target
    for lkp in lookups:
        dim_id = _sanitize_id(lkp['dim_table'])
        src_ids = ' & '.join(f'S_{_sanitize_id(f)}' for f in sorted(lkp['source_fields']))
        tgt_ids = ' & '.join(f'T_{_sanitize_id(c)}' for c in lkp['target_cols'])
        if src_ids and tgt_ids:
            lines.append(f'    {src_ids} --> D_{dim_id} --> {tgt_ids}')

    lines.append('')
    lines.append('    style SOURCE fill:#e3f2fd')
    lines.append('    style DIMS fill:#fce4ec')
    lines.append('    style TARGET fill:#e8f5e9')
    lines.append('```')

    return '\n'.join(lines)


def _diagram_3_column_categories(lineage_df: pd.DataFrame, sources_dict: Dict,
                                  targets_dict: Dict) -> str:
    """Generate column category lineage diagram."""
    cats = _classify_target_columns(lineage_df)
    src_tables = _get_source_tables(sources_dict)
    tgt_tables = _get_target_tables(targets_dict)

    src_name = src_tables[0] if src_tables else 'SOURCE'
    tgt_name = tgt_tables[0] if tgt_tables else 'TARGET'

    # Collect source columns by category for display
    nk_src = [c['source_col'] for c in cats['natural_key'] if c['source_col']][:4]
    dt_src = [c['source_col'] for c in cats['date'] if c['source_col']][:4]
    cd_src = [c['source_col'] for c in cats['code'] if c['source_col']][:4]
    amt_src = [c['source_col'] for c in cats['amount'] if c['source_col']][:4]
    txt_src = [c['source_col'] for c in cats['text'] if c['source_col']][:4]

    lines = ['```mermaid', 'flowchart LR']

    lines.append(f'    subgraph SRC["Source: {src_name}"]')
    lines.append('        direction TB')
    if nk_src:
        lines.append(f'        S1["Keys<br/>{", ".join(nk_src[:2])}<br/>{", ".join(nk_src[2:4])}"]')
    if dt_src:
        lines.append(f'        S2["Dates<br/>{", ".join(dt_src[:2])}<br/>{", ".join(dt_src[2:4])}..."]')
    if cd_src:
        lines.append(f'        S3["Codes<br/>{", ".join(cd_src[:2])}<br/>{", ".join(cd_src[2:4])}..."]')
    if amt_src:
        lines.append(f'        S4["Amounts<br/>{", ".join(amt_src[:2])}<br/>{", ".join(amt_src[2:4])}..."]')
    if txt_src:
        lines.append(f'        S5["Text<br/>{", ".join(txt_src[:2])}<br/>{", ".join(txt_src[2:4])}..."]')
    lines.append('    end')
    lines.append('')

    lines.append('    subgraph TRANS["Transformations"]')
    lines.append('        direction TB')
    lines.append('        T1["COALESCE<br/>Null to Empty/0"]')
    lines.append('        T2["CAST<br/>String to Int/Date"]')
    lines.append('        T3["Lookup<br/>NK to FK"]')
    lines.append('    end')
    lines.append('')

    lines.append(f'    subgraph TGT["Target: {tgt_name}"]')
    lines.append('        direction TB')
    lines.append(f'        D1["NK_* Columns<br/>{len(cats["natural_key"])} natural keys"]')
    lines.append(f'        D2["Date Columns<br/>{len(cats["date"])} date columns"]')
    lines.append(f'        D3["*_CD Columns<br/>{len(cats["code"])} code columns"]')
    lines.append(f'        D4["AMT_* Columns<br/>{len(cats["amount"])} amount columns"]')
    lines.append(f'        D5["Text Columns<br/>{len(cats["text"])} text columns"]')
    lines.append(f'        D6["C_*/H_* IDs<br/>{len(cats["lookup_fk"])} dimension FKs"]')
    lines.append('    end')
    lines.append('')

    # Edges
    if nk_src:
        lines.append('    S1 --> T1 --> D1')
    if dt_src:
        lines.append('    S2 --> T2 --> D2')
    if cd_src:
        lines.append('    S3 --> T1 --> D3')
    if amt_src:
        lines.append('    S4 --> T1 --> D4')
    if txt_src:
        lines.append('    S5 --> T1 --> D5')
    if cats['lookup_fk']:
        # Keys feed into lookups
        if nk_src:
            lines.append('    S1 --> T3 --> D6')

    lines.append('')
    lines.append('    style SRC fill:#fff3e0')
    lines.append('    style TRANS fill:#f3e5f5')
    lines.append('    style TGT fill:#e8f5e9')
    lines.append('```')

    return '\n'.join(lines)


def _diagram_4_point_in_time(lineage_df: pd.DataFrame) -> str:
    """Generate point-in-time lookup logic diagram."""
    lookups = _extract_lookups(lineage_df)
    pit_lookups = [lkp for lkp in lookups if lkp['point_in_time']]

    if not pit_lookups:
        return '_No point-in-time lookups detected._'

    # Use the first PIT lookup as the example
    lkp = pit_lookups[0]
    dim = lkp['dim_table']
    key = lkp['lookup_key'] or 'KEY'
    src_fields = sorted(lkp['source_fields'])
    primary_field = src_fields[0] if src_fields else 'SOURCE_KEY'

    lines = ['```mermaid', 'flowchart TB']

    lines.append('    subgraph INPUT["Lookup Input"]')
    lines.append(f'        PKEY["{primary_field}<br/>({_friendly_name(key)})"]')
    lines.append('        BUDAT["BUDAT<br/>(Posting Date)"]')
    lines.append('        TYPE["TYPE_CD<br/>(C or H)"]')
    lines.append('    end')
    lines.append('')

    lines.append(f'    subgraph DIM["{dim}"]')
    lines.append(f'        DIM_REC["Records with:<br/>{key}<br/>VAL_FROM_DATE<br/>VAL_TO_DATE<br/>TYPE_CD"]')
    lines.append('    end')
    lines.append('')

    lines.append('    subgraph COND["Lookup Condition"]')
    lines.append(f'        C1["{key} = {primary_field}"]')
    lines.append('        C2["TYPE_CD = C or H"]')
    lines.append('        C3["VAL_FROM_DATE <= BUDAT"]')
    lines.append('        C4["VAL_TO_DATE >= BUDAT"]')
    lines.append('    end')
    lines.append('')

    # Find C_ and H_ target columns for this lookup
    c_cols = [c for c in lkp['target_cols'] if c.upper().startswith('C_')]
    h_cols = [c for c in lkp['target_cols'] if c.upper().startswith('H_')]

    lines.append('    subgraph OUTPUT["Lookup Output"]')
    if c_cols:
        lines.append(f'        OUT_C["{c_cols[0]}<br/>(Current)"]')
    if h_cols:
        lines.append(f'        OUT_H["{h_cols[0]}<br/>(Historical)"]')
    lines.append('    end')
    lines.append('')

    lines.append('    PKEY --> C1')
    lines.append('    TYPE --> C2')
    lines.append('    BUDAT --> C3 & C4')
    lines.append('    DIM_REC --> C1 & C2 & C3 & C4')
    out_ids = []
    if c_cols:
        out_ids.append('OUT_C')
    if h_cols:
        out_ids.append('OUT_H')
    if out_ids:
        lines.append(f'    C1 & C2 & C3 & C4 --> {" & ".join(out_ids)}')

    lines.append('')
    lines.append('    style INPUT fill:#e3f2fd')
    lines.append('    style DIM fill:#fce4ec')
    lines.append('    style COND fill:#fff9c4')
    lines.append('    style OUTPUT fill:#e8f5e9')
    lines.append('```')

    return '\n'.join(lines)


def _friendly_name(col: str) -> str:
    """Convert column name to friendly label."""
    return col.replace('_', ' ').title()


def _diagram_5_update_strategy(lineage_df: pd.DataFrame, targets_dict: Dict,
                                mappings_dict: Dict) -> str:
    """Generate update strategy flow diagram."""
    transforms = _extract_transformations(mappings_dict)
    rtr_trans = [t for t in transforms if t['type'] in ('Router', 'Router Transformation')]
    upd_trans = [t for t in transforms if t['type'] in ('Update Strategy', 'Update Strategy Transformation')]

    if not rtr_trans and not upd_trans:
        return '_No router/update strategy detected._'

    tgt_tables = _get_target_tables(targets_dict)
    tgt_name = tgt_tables[0] if tgt_tables else 'TARGET'

    # Try to detect aggregation keys from natural key columns
    nk_cols = []
    for _, row in lineage_df.iterrows():
        tgt_col = _safe(row.get('Target_Column')).upper()
        if tgt_col.startswith('NK_') and 'DATE' not in tgt_col:
            src_col = _safe(row.get('Source_Column_INSERT'))
            if src_col:
                nk_cols.append(src_col)
    key_display = ', '.join(nk_cols[:4]) if nk_cols else 'key columns'

    lines = ['```mermaid', 'flowchart TB']

    lines.append('    subgraph AGG["After Aggregation"]')
    lines.append(f'        REC["Aggregated Record<br/>({key_display})"]')
    lines.append('    end')
    lines.append('')

    lines.append('    subgraph LKP_EXIST["Existence Check"]')
    lines.append(f'        LKP["LKP_{tgt_name}<br/>Check if key exists"]')
    lines.append('    end')
    lines.append('')

    lines.append('    subgraph ROUTER["Router Logic"]')
    lines.append('        CHK{{"Record Exists?"}}')
    lines.append('        INS["INSERT Path"]')
    lines.append('        UPD["UPDATE Path"]')
    lines.append('    end')
    lines.append('')

    lines.append(f'    subgraph TARGET["{tgt_name}"]')
    lines.append('        TGT_INS["New Records<br/>(INSERT)"]')
    lines.append('        TGT_UPD["Existing Records<br/>(UPDATE)"]')
    lines.append('    end')
    lines.append('')

    lines.append('    REC --> LKP_EXIST --> CHK')
    lines.append('    CHK -->|"No (NULL)"| INS --> TGT_INS')
    lines.append('    CHK -->|"Yes (Found)"| UPD --> TGT_UPD')
    lines.append('')
    lines.append('    style AGG fill:#e3f2fd')
    lines.append('    style LKP_EXIST fill:#fff9c4')
    lines.append('    style ROUTER fill:#f3e5f5')
    lines.append('    style TARGET fill:#e8f5e9')
    lines.append('```')

    return '\n'.join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# Section 6: Column Lineage Matrix (Markdown tables, not Mermaid)
# ═══════════════════════════════════════════════════════════════════════════

def _section_6_column_matrix(lineage_df: pd.DataFrame) -> str:
    """Generate column lineage matrix as Markdown tables."""
    cats = _classify_target_columns(lineage_df)
    lookups = _extract_lookups(lineage_df)
    lookup_map = {lkp['dim_table']: lkp for lkp in lookups}

    parts = []

    # --- Dimension Foreign Keys ---
    fk_rows = cats['lookup_fk']
    if fk_rows:
        parts.append(f'### Dimension Foreign Keys ({len(fk_rows)} columns)')
        parts.append('')
        parts.append('| Target Column | Source Column | Lookup Table | Lookup Key | Point-in-Time |')
        parts.append('|--------------|---------------|--------------|------------|---------------|')
        for entry in fk_rows:
            tgt = entry['target_col']
            src = entry['source_col']
            lkp_cond = entry['lookup_cond']

            # Find matching lookup table
            lkp_table = ''
            lkp_key = ''
            pit = 'No'
            for dim, info in lookup_map.items():
                if tgt in info['target_cols']:
                    lkp_table = dim
                    lkp_key = info['lookup_key']
                    if info['point_in_time']:
                        pit = 'Yes (BUDAT)'
                    break

            parts.append(f'| {tgt} | {src} | {lkp_table} | {lkp_key} | {pit} |')
        parts.append('')

    # --- Natural Keys ---
    nk_rows = cats['natural_key']
    if nk_rows:
        parts.append(f'### Natural Keys ({len(nk_rows)} columns)')
        parts.append('')
        parts.append('| Target Column | Source Column | Transformation |')
        parts.append('|--------------|---------------|----------------|')
        for entry in nk_rows:
            expr = entry['expression'] or 'Direct'
            parts.append(f'| {entry["target_col"]} | {entry["source_col"]} | {expr} |')
        parts.append('')

    # --- Date Columns ---
    dt_rows = cats['date']
    if dt_rows:
        parts.append(f'### Date Columns ({len(dt_rows)} columns)')
        parts.append('')
        parts.append('| Target Column | Source Column | Description |')
        parts.append('|--------------|---------------|-------------|')
        for entry in dt_rows:
            desc = _date_description(entry['target_col'])
            parts.append(f'| {entry["target_col"]} | {entry["source_col"]} | {desc} |')
        parts.append('')

    # --- Amount Columns ---
    amt_rows = cats['amount']
    if amt_rows:
        parts.append(f'### Amount Columns ({len(amt_rows)} columns)')
        parts.append('')
        parts.append('| Target Column | Source Column | Transformation |')
        parts.append('|--------------|---------------|----------------|')
        for entry in amt_rows:
            expr = entry['expression'] or 'Direct'
            parts.append(f'| {entry["target_col"]} | {entry["source_col"]} | {expr} |')
        parts.append('')

    return '\n'.join(parts)


def _date_description(col: str) -> str:
    """Generate description for a date column."""
    col_upper = col.upper()
    descs = {
        'DOC_DATE': 'Document date', 'PSTG_DATE': 'Posting date',
        'ENTRY_DATE': 'Entry/CPU date', 'CLRNG_DATE': 'Clearing date',
        'DUNN_DATE': 'Last dunning date', 'NET_DUE': 'Net payment due date',
        'CASH_DISC_TERMS_1': 'Cash discount 1 due date',
        'CASH_DISC_TERMS_2': 'Cash discount 2 due date',
        'BSLN_DATE': 'Baseline date', 'BANK_CHRG': 'Bank charges date',
    }
    for key, desc in descs.items():
        if key in col_upper:
            return desc
    return 'Date field'


# ═══════════════════════════════════════════════════════════════════════════
# Section 7: dbt Model Lineage
# ═══════════════════════════════════════════════════════════════════════════

def _diagram_7_dbt_lineage(lineage_df: pd.DataFrame, sources_dict: Dict,
                            targets_dict: Dict) -> str:
    """Generate dbt model lineage diagram."""
    src_tables = _get_source_tables(sources_dict)
    tgt_tables = _get_target_tables(targets_dict)
    lookups = _extract_lookups(lineage_df)
    dim_tables = [lkp['dim_table'] for lkp in lookups]

    src_display = src_tables[0] if src_tables else 'SOURCE'
    tgt_display = tgt_tables[0] if tgt_tables else 'TARGET'

    lines = ['```mermaid', 'flowchart TB']

    lines.append('    subgraph SOURCES["Sources"]')
    lines.append(f'        SRC_FILE[("{src_display}<br/>Flat File")]')
    if dim_tables:
        dim_sample = '<br/>'.join(dim_tables[:3])
        if len(dim_tables) > 3:
            dim_sample += '<br/>etc.'
        lines.append(f'        SRC_DIMS[("{len(dim_tables)} Dimension Tables<br/>{dim_sample}")]')
    lines.append('    end')
    lines.append('')

    lines.append('    subgraph BRONZE["Bronze Layer"]')
    stg_name = f'stg_{tgt_display.lower()}'
    lines.append(f'        STG["{stg_name}<br/>- Null handling<br/>- Type casting<br/>- Deduplication"]')
    lines.append('    end')
    lines.append('')

    lines.append('    subgraph SILVER["Silver Layer"]')
    fact_name = f'fact_{tgt_display.lower()}'
    join_desc = f'{len(dim_tables)} Dimension joins' if dim_tables else 'Transform logic'
    pit_count = sum(1 for lkp in lookups if lkp['point_in_time'])
    pit_desc = f'<br/>- {pit_count} Point-in-time lookups' if pit_count else ''
    lines.append(f'        FACT["{fact_name}<br/>- {join_desc}{pit_desc}<br/>- Incremental merge"]')
    lines.append('    end')
    lines.append('')

    lines.append('    SRC_FILE --> STG')
    lines.append('    STG --> FACT')
    if dim_tables:
        lines.append('    SRC_DIMS --> FACT')
    lines.append('')
    lines.append('    style SOURCES fill:#e3f2fd')
    lines.append('    style BRONZE fill:#fff3e0')
    lines.append('    style SILVER fill:#e8f5e9')
    lines.append('```')

    return '\n'.join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# Main entry point
# ═══════════════════════════════════════════════════════════════════════════

def generate_lineage_diagrams(lineage_df: pd.DataFrame,
                               sources_dict: Dict,
                               targets_dict: Dict,
                               mappings_dict: Dict = None) -> str:
    """
    Generate a complete data lineage diagram document in Markdown with Mermaid.

    Args:
        lineage_df: Parsed lineage DataFrame
        sources_dict: Source definitions
        targets_dict: Target definitions
        mappings_dict: Mapping structures (optional, for transform details)

    Returns:
        Markdown string with 7 sections
    """
    if lineage_df is None or lineage_df.empty:
        return '# Data Lineage Diagram\n\n_No lineage data available._'

    tgt_tables = _get_target_tables(targets_dict)
    tgt_name = tgt_tables[0] if tgt_tables else 'Target'
    friendly_name = tgt_name.replace('_', ' ').title()

    today = date.today().isoformat()

    sections = []

    # Header
    sections.append(f'# {friendly_name} - Data Lineage Diagram')
    sections.append('')
    sections.append('## Complete Data Flow Visualization')
    sections.append('')
    sections.append(f'This document provides visual data lineage diagrams showing how data flows from source to target in the {friendly_name} ETL process.')
    sections.append('')
    sections.append('---')
    sections.append('')

    # Section 1
    sections.append('## 1. High-Level Pipeline Flow')
    sections.append('')
    sections.append(_diagram_1_pipeline_flow(lineage_df, sources_dict, targets_dict, mappings_dict or {}))
    sections.append('')
    sections.append('---')
    sections.append('')

    # Section 2
    sections.append('## 2. Dimension Lookup Data Flow')
    sections.append('')
    sections.append(_diagram_2_dimension_lookups(lineage_df))
    sections.append('')
    sections.append('---')
    sections.append('')

    # Section 3
    sections.append('## 3. Column Category Lineage')
    sections.append('')
    sections.append(_diagram_3_column_categories(lineage_df, sources_dict, targets_dict))
    sections.append('')
    sections.append('---')
    sections.append('')

    # Section 4
    sections.append('## 4. Point-in-Time Lookup Logic')
    sections.append('')
    sections.append(_diagram_4_point_in_time(lineage_df))
    sections.append('')
    sections.append('---')
    sections.append('')

    # Section 5
    sections.append('## 5. Update Strategy Flow')
    sections.append('')
    sections.append(_diagram_5_update_strategy(lineage_df, targets_dict, mappings_dict or {}))
    sections.append('')
    sections.append('---')
    sections.append('')

    # Section 6
    sections.append('## 6. Complete Column Lineage Matrix')
    sections.append('')
    sections.append(_section_6_column_matrix(lineage_df))
    sections.append('')
    sections.append('---')
    sections.append('')

    # Section 7
    sections.append('## 7. dbt Model Lineage (Converted)')
    sections.append('')
    sections.append(_diagram_7_dbt_lineage(lineage_df, sources_dict, targets_dict))
    sections.append('')
    sections.append('---')
    sections.append('')

    # Footer
    sections.append(f'*Generated: {today} | Source: Informatica PowerCenter*')

    return '\n'.join(sections)
