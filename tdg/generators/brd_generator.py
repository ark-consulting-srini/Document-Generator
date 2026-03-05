"""
BRD Generator Module for T - BRD STTM Generator

Functions for preparing lineage summaries and generating BRD content.
"""

import pandas as pd
from typing import Dict, List, Optional


def prepare_lineage_summary(df_lineage: pd.DataFrame, 
                           df_sources: pd.DataFrame = None, 
                           df_targets: pd.DataFrame = None, 
                           max_rows: int = None) -> str:
    """
    Prepare a comprehensive summary of the lineage for LLM consumption.
    
    Args:
        df_lineage: DataFrame with lineage data
        df_sources: DataFrame with source definitions (optional)
        df_targets: DataFrame with target definitions (optional)
        max_rows: Optional limit on rows (None = all rows)
        
    Returns:
        Formatted summary string
    """
    summary = []
    
    # Target table info
    target_tables = df_lineage['Target_Table'].unique()
    summary.append(f"TARGET TABLES: {', '.join([t for t in target_tables if t])}")
    
    # Source tables info
    source_tables = df_lineage['Source_Table_INSERT'].unique()
    source_tables = [s for s in source_tables if s and s not in 
                    ['Derived', 'Hardcoded', 'SYSTEM', 'UNCONNECTED', 'SEQUENCE_GENERATOR', 'Lookup/Expression']]
    summary.append(f"SOURCE TABLES: {', '.join(source_tables)}")
    
    # Lookup tables
    lookup_tables = [s for s in df_lineage['Source_Table_INSERT'].unique() 
                    if s and s not in source_tables and s not in 
                    ['Derived', 'Hardcoded', 'SYSTEM', 'UNCONNECTED', 'SEQUENCE_GENERATOR', 'Lookup/Expression']]
    if lookup_tables:
        summary.append(f"LOOKUP TABLES: {', '.join(lookup_tables)}")
    
    # SQL Overrides
    if 'SQL_Override' in df_lineage.columns:
        sql_overrides = df_lineage['SQL_Override'].dropna().unique()
        sql_overrides = [s for s in sql_overrides if s and len(str(s).strip()) > 0]
        if sql_overrides:
            summary.append(f"\nSQL OVERRIDES:")
            for sql in sql_overrides[:5]:
                sql_str = str(sql)
                summary.append(f"  {sql_str[:200]}{'...' if len(sql_str) > 200 else ''}")
    
    # Lookup Conditions
    if 'Lookup_Condition' in df_lineage.columns:
        lookup_conditions = df_lineage['Lookup_Condition'].dropna().unique()
        lookup_conditions = [c for c in lookup_conditions if c and len(str(c).strip()) > 0]
        if lookup_conditions:
            summary.append(f"\nLOOKUP CONDITIONS:")
            for cond in lookup_conditions:
                summary.append(f"  {cond}")
    
    # Field mappings
    summary.append("\nFIELD MAPPINGS (Source -> Target):")
    rows_to_process = df_lineage if max_rows is None else df_lineage.head(max_rows)
    
    for idx, row in rows_to_process.iterrows():
        src_table = row.get('Source_Table_INSERT', '') or ''
        src_col = row.get('Source_Column_INSERT', '') or ''
        tgt_table = row.get('Target_Table', '') or ''
        tgt_col = row.get('Target_Column', '') or ''
        expr = row.get('Expression_Logic', '') or ''
        lkp_cond = row.get('Lookup_Condition', '') or ''
        transformations = row.get('Transformations_INSERT', '') or ''
        
        mapping_line = f"  {src_table}.{src_col} -> {tgt_table}.{tgt_col}"

        if expr:
            expr_str = str(expr)
            mapping_line += f" | Expr: {expr_str[:100]}{'...' if len(expr_str) > 100 else ''}"

        if lkp_cond:
            mapping_line += f" | LookupOn: {lkp_cond}"

        if transformations:
            trans_str = str(transformations)
            mapping_line += f" | Via: {trans_str[:80]}{'...' if len(trans_str) > 80 else ''}"

        # Cross-mapping path (shows chained mapping trace when applicable)
        cross_path = row.get('Cross_Mapping_Path', '') or ''
        if cross_path:
            mapping_line += f" | MappingPath: {cross_path}"

        summary.append(mapping_line)
    
    if max_rows and len(df_lineage) > max_rows:
        summary.append(f"  ... and {len(df_lineage) - max_rows} more mappings")
    
    # Statistics
    summary.append(f"\nSUMMARY STATISTICS:")
    summary.append(f"  Total Field Mappings: {len(df_lineage)}")
    summary.append(f"  Source Tables: {len(source_tables)}")
    summary.append(f"  Target Tables: {len(target_tables)}")
    
    derived_count = len(df_lineage[df_lineage['Source_Table_INSERT'] == 'Derived'])
    hardcoded_count = len(df_lineage[df_lineage['Source_Table_INSERT'] == 'Hardcoded'])
    system_count = len(df_lineage[df_lineage['Source_Table_INSERT'] == 'SYSTEM'])
    
    if derived_count:
        summary.append(f"  Derived Fields: {derived_count}")
    if hardcoded_count:
        summary.append(f"  Hardcoded Fields: {hardcoded_count}")
    if system_count:
        summary.append(f"  System Fields (SYSDATE etc): {system_count}")
    
    return "\n".join(summary)


def prepare_raw_xml_summary(sources_dict: Dict, 
                           targets_dict: Dict, 
                           mappings_dict: Dict, 
                           optimized: bool = False) -> str:
    """
    Prepare a summary directly from the parsed XML structure.
    
    Args:
        sources_dict: Dictionary of source tables and fields
        targets_dict: Dictionary of target tables and fields
        mappings_dict: Dictionary of mapping structures
        optimized: If True, uses smart sampling to reduce size
        
    Returns:
        Formatted summary string
    """
    summary = []
    
    summary.append("=" * 60)
    if optimized:
        summary.append("RAW XML STRUCTURE SUMMARY (OPTIMIZED)")
    else:
        summary.append("RAW XML STRUCTURE SUMMARY (COMPLETE)")
    summary.append("=" * 60)
    
    max_fields = 50 if optimized else None
    max_expr = 15 if optimized else None
    max_sql = 500 if optimized else None
    
    # SOURCE DEFINITIONS
    summary.append("\n### SOURCE DEFINITIONS ###")
    for source_name, source_info in sources_dict.items():
        fields = source_info.get('fields', {})
        selected = _get_important_fields(fields, max_fields) if optimized else fields
        
        field_count = f"({len(fields)} fields"
        if optimized and len(fields) > len(selected):
            field_count += f", showing {len(selected)} key fields)"
        else:
            field_count += ")"
        
        summary.append(f"\nSOURCE: {source_name} {field_count}")
        
        for field_name, field_info in selected.items():
            dtype = field_info.get('datatype', '')
            precision = field_info.get('precision', '')
            scale = field_info.get('scale', '')
            key = field_info.get('keytype', '')
            
            key_str = ""
            if key and 'PRIMARY' in str(key).upper():
                key_str = " [PK]"
            elif key and 'FOREIGN' in str(key).upper():
                key_str = " [FK]"
            
            dtype_str = dtype
            if precision:
                dtype_str += f"({precision}"
                if scale:
                    dtype_str += f",{scale}"
                dtype_str += ")"
            
            summary.append(f"  - {field_name}: {dtype_str}{key_str}")
        
        if optimized and len(fields) > len(selected):
            summary.append(f"  ... and {len(fields) - len(selected)} more fields")
    
    # TARGET DEFINITIONS
    summary.append("\n### TARGET DEFINITIONS ###")
    for target_name, target_info in targets_dict.items():
        fields = target_info.get('fields', {})
        selected = _get_important_fields(fields, max_fields) if optimized else fields
        
        field_count = f"({len(fields)} fields"
        if optimized and len(fields) > len(selected):
            field_count += f", showing {len(selected)} key fields)"
        else:
            field_count += ")"
        
        summary.append(f"\nTARGET: {target_name} {field_count}")
        
        for field_name, field_info in selected.items():
            dtype = field_info.get('datatype', '')
            precision = field_info.get('precision', '')
            scale = field_info.get('scale', '')
            key = field_info.get('keytype', '')
            
            key_str = ""
            if key and 'PRIMARY' in str(key).upper():
                key_str = " [PK]"
            elif key and 'FOREIGN' in str(key).upper():
                key_str = " [FK]"
            
            dtype_str = dtype
            if precision:
                dtype_str += f"({precision}"
                if scale:
                    dtype_str += f",{scale}"
                dtype_str += ")"
            
            summary.append(f"  - {field_name}: {dtype_str}{key_str}")
        
        if optimized and len(fields) > len(selected):
            summary.append(f"  ... and {len(fields) - len(selected)} more fields")
    
    # MAPPING STRUCTURE
    summary.append("\n### MAPPING STRUCTURE ###")
    for mapping_name, mapping_info in mappings_dict.items():
        summary.append(f"\nMAPPING: {mapping_name}")
        
        instances = mapping_info.get('instances', {})
        source_instances = [n for n, i in instances.items() if i.get('instance_type') == 'SOURCE']
        target_instances = [n for n, i in instances.items() if i.get('instance_type') == 'TARGET']
        
        summary.append(f"  Source Instances: {', '.join(source_instances)}")
        summary.append(f"  Target Instances: {', '.join(target_instances)}")
        
        transformations = mapping_info.get('transformations', {})
        summary.append(f"  Transformations ({len(transformations)}):")
        
        for trans_name, trans_info in transformations.items():
            trans_type = trans_info.get('type', 'Unknown')
            summary.append(f"    - {trans_name} ({trans_type})")
            
            # SQL Query
            sql_query = trans_info.get('sql_query', '')
            if sql_query:
                if max_sql and len(sql_query) > max_sql:
                    summary.append(f"      SQL Query: {sql_query[:max_sql]}...")
                else:
                    summary.append(f"      SQL Query: {sql_query}")
            
            # Lookup condition
            lookup_cond = trans_info.get('lookup_condition', '')
            if lookup_cond:
                summary.append(f"      Lookup Condition: {lookup_cond}")
            
            # Expressions
            fields_with_expr = [(f, i) for f, i in trans_info.get('fields', {}).items() 
                               if i.get('expression') and i.get('expression') != f]
            if fields_with_expr:
                selected_expr = _get_important_expressions(fields_with_expr, max_expr) if optimized else fields_with_expr
                
                expr_count = f"({len(fields_with_expr)}"
                if optimized and len(fields_with_expr) > len(selected_expr):
                    expr_count += f", showing {len(selected_expr)} key):"
                else:
                    expr_count += "):"
                
                summary.append(f"      Expressions {expr_count}")
                
                for field_name, field_info in selected_expr:
                    expr = field_info.get('expression', '')
                    if len(expr) > 200:
                        summary.append(f"        {field_name} = {expr[:200]}...")
                    else:
                        summary.append(f"        {field_name} = {expr}")
        
        connectors = mapping_info.get('connectors', [])
        summary.append(f"  Total Connectors: {len(connectors)}")
    
    summary.append("\n" + "=" * 60)
    summary.append(f"SUMMARY: {len(sources_dict)} sources, {len(targets_dict)} targets, {len(mappings_dict)} mappings")
    summary.append("=" * 60)
    
    return "\n".join(summary)


def _get_important_fields(fields_dict: Dict, max_fields: int = None) -> Dict:
    """Select important fields prioritizing PKs and key-like names."""
    if max_fields is None or len(fields_dict) <= max_fields:
        return fields_dict
    
    important = {}
    remaining = {}
    
    for field_name, field_info in fields_dict.items():
        key = field_info.get('keytype', '')
        if key and 'PRIMARY' in str(key).upper():
            important[field_name] = field_info
        elif key and 'FOREIGN' in str(key).upper():
            important[field_name] = field_info
        elif any(kw in field_name.upper() for kw in ['_ID', '_KEY', '_CD', '_CODE', '_NO', '_NUM']):
            important[field_name] = field_info
        else:
            remaining[field_name] = field_info
    
    if len(important) >= max_fields:
        return dict(list(important.items())[:max_fields])
    
    slots_left = max_fields - len(important)
    remaining_list = list(remaining.items())
    
    if len(remaining_list) > slots_left:
        step = len(remaining_list) / slots_left
        sampled = [remaining_list[int(i * step)] for i in range(slots_left)]
    else:
        sampled = remaining_list
    
    important.update(dict(sampled))
    return important


def _get_important_expressions(fields_with_expr: List, max_expr: int = None) -> List:
    """Select important expressions prioritizing complex ones."""
    if max_expr is None or len(fields_with_expr) <= max_expr:
        return fields_with_expr
    
    complex_expr = []
    simple_expr = []
    
    for field_name, field_info in fields_with_expr:
        expr = field_info.get('expression', '')
        if any(kw in expr.upper() for kw in ['IIF', 'DECODE', 'CASE', '||', 'LOOKUP', 'ERROR']):
            complex_expr.append((field_name, field_info))
        elif any(op in expr for op in ['/', '*', '+', '-']):
            complex_expr.append((field_name, field_info))
        else:
            simple_expr.append((field_name, field_info))
    
    result = complex_expr[:max_expr]
    if len(result) < max_expr:
        result.extend(simple_expr[:max_expr - len(result)])
    
    return result


def generate_sample_requirements(df_lineage: pd.DataFrame, 
                                 business_context: str = "") -> str:
    """
    Generate sample requirements when Snowflake is not available.
    
    Args:
        df_lineage: Lineage DataFrame
        business_context: Optional business context
        
    Returns:
        Sample BRD content
    """
    target_tables = df_lineage['Target_Table'].unique()
    source_tables = [s for s in df_lineage['Source_Table_INSERT'].unique() 
                    if s and s not in ['Derived', 'Hardcoded', 'SYSTEM', 'UNCONNECTED', 'SEQUENCE_GENERATOR']]
    
    sample = f"""## 1. Overview

This ETL process loads data from {len(source_tables)} source table(s) into the {', '.join(target_tables)} target table(s).
A total of {len(df_lineage)} field mappings have been identified.

{f"**Business Context:** {business_context}" if business_context else ""}

## 2. Business Functional Requirements (BRD)

- **BR-1**: Load data from source system into {', '.join(target_tables)} target table
- **BR-2**: Transform and map {len(df_lineage)} fields from source to target
- **BR-3**: Apply lookup transformations for reference data enrichment
- **BR-4**: Handle derived and calculated fields using expression logic
- **BR-5**: Validate all mandatory fields are populated before loading
- **BR-6**: Log all failed records for review and reprocessing
- **BR-7**: Support incremental data loading where applicable

## 3. Technical Detailed Steps

1. Initialize ETL process and log start time
2. Extract data from source tables: {', '.join(source_tables[:5])}{'...' if len(source_tables) > 5 else ''}
3. Apply source qualifier transformations
4. Execute lookup transformations for reference data
5. Apply expression transformations for calculated fields
6. Map transformed data to target structure
7. Validate data quality rules
8. Load data into target table: {', '.join(target_tables)}
9. Log completion status and record counts

## 4. Source System Description

The following source tables are involved:
{chr(10).join([f"- **{s}**" for s in source_tables[:10]])}

## 5. Target System Description

Data is loaded into:
{chr(10).join([f"- **{t}**" for t in target_tables])}

## 6. Data Quality Requirements

- **DQ-1**: All mandatory fields must be populated
- **DQ-2**: Foreign key relationships must be validated
- **DQ-3**: Data type conversions must be handled appropriately

## 7. Error Handling

- Failed records should be logged for review
- The process should continue processing valid records

---
*Note: This is a sample output. Connect to Snowflake for AI-generated requirements.*
"""
    return sample
