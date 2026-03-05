"""
Data Model Generator Module for T - BRD STTM Generator

Generates Graphviz data model diagrams from parsed XML structures.
"""

from typing import Dict, Tuple, Optional
import pandas as pd

from config.settings import GRAPHVIZ_AVAILABLE


def generate_data_model(sources: Dict, targets: Dict, 
                       lineage_df: pd.DataFrame, 
                       model_type: str = 'logical') -> Tuple[Optional[object], Optional[str]]:
    """
    Generate a Graphviz data model diagram.
    
    Args:
        sources: Dictionary of source table definitions
        targets: Dictionary of target table definitions
        lineage_df: DataFrame with lineage data
        model_type: 'conceptual' (entities only), 'logical' (entities + columns), 
                   'physical' (entities + columns + datatypes)
    
    Returns:
        Tuple of (graphviz.Digraph object, error message)
    """
    if not GRAPHVIZ_AVAILABLE:
        return None, "graphviz library not installed. Run: pip install graphviz"
    
    try:
        import graphviz
    except ImportError:
        return None, "graphviz library not installed"
    
    dot = graphviz.Digraph(comment=f'{model_type.title()} Data Model')
    dot.attr(rankdir='LR')  # Left to right layout
    
    if model_type == 'conceptual':
        dot.attr('node', shape='box', style='filled', fillcolor='lightblue', 
                fontname='Helvetica', fontsize='12')
    else:
        dot.attr('node', shape='none', fontname='Helvetica')
    
    nodes_added = set()
    edges_added = set()
    
    # Get actual source/target table names
    actual_source_tables = set(sources.keys())
    actual_target_tables = set(targets.keys())
    
    # Collect source-to-target edges
    source_to_target_edges = set()
    
    if lineage_df is not None and len(lineage_df) > 0:
        for _, row in lineage_df.iterrows():
            src_table = row.get('Source_Table_INSERT', '')
            tgt_table = row.get('Target_Table', '')
            
            if not src_table or not tgt_table:
                continue
            
            src_clean = src_table.replace('Shortcut_to_', '')
            tgt_clean = tgt_table.replace('Shortcut_to_', '')
            
            # Only include actual source tables
            if src_clean in actual_source_tables:
                source_to_target_edges.add((src_clean, tgt_clean))
    
    sources_with_connections = set(edge[0] for edge in source_to_target_edges)
    targets_with_connections = set(edge[1] for edge in source_to_target_edges)
    
    # Add source table nodes
    for source_name in sources_with_connections:
        if source_name not in nodes_added and source_name in sources:
            if model_type == 'conceptual':
                dot.node(source_name, source_name, fillcolor='#E3F2FD')
            else:
                label = _create_table_label(
                    source_name, 
                    sources[source_name].get('fields', {}),
                    '#E3F2FD',
                    show_datatypes=(model_type == 'physical')
                )
                dot.node(source_name, label)
            nodes_added.add(source_name)
    
    # Add target table nodes
    for target_name in targets_with_connections:
        if target_name not in nodes_added and target_name in targets:
            if model_type == 'conceptual':
                dot.node(target_name, target_name, fillcolor='#E8F5E9')
            else:
                label = _create_table_label(
                    target_name,
                    targets[target_name].get('fields', {}),
                    '#E8F5E9',
                    show_datatypes=(model_type == 'physical')
                )
                dot.node(target_name, label)
            nodes_added.add(target_name)
    
    # Add edges
    for src_table, tgt_table in source_to_target_edges:
        edge_key = f"{src_table}->{tgt_table}"
        if edge_key not in edges_added:
            dot.edge(src_table, tgt_table, style='solid', color='#555555')
            edges_added.add(edge_key)
    
    return dot, None


def _create_table_label(table_name: str, fields_dict: Dict, 
                        color: str, show_datatypes: bool = False) -> str:
    """
    Create HTML table label for logical/physical model nodes.
    
    Args:
        table_name: Name of the table
        fields_dict: Dictionary of field definitions
        color: Background color
        show_datatypes: Whether to show datatypes
        
    Returns:
        HTML label string for Graphviz
    """
    # Sort fields - PKs first
    sorted_fields = sorted(
        fields_dict.items(),
        key=lambda x: (
            0 if 'PRIMARY' in str(x[1].get('keytype', '')).upper() else 1,
            x[0]
        )
    )
    
    rows = ""
    for field_name, field_info in sorted_fields[:30]:  # Limit to 30 fields
        pk_icon = "🔑 " if 'PRIMARY' in str(field_info.get('keytype', '')).upper() else ""
        fk_icon = "🔗 " if 'FOREIGN' in str(field_info.get('keytype', '')).upper() else ""
        icon = pk_icon or fk_icon
        
        if show_datatypes:
            dtype = field_info.get('datatype', '')
            precision = field_info.get('precision', '')
            dtype_str = f" : {dtype}"
            if precision:
                dtype_str += f"({precision})"
            rows += f'<TR><TD ALIGN="LEFT" PORT="{field_name}">{icon}{field_name}{dtype_str}</TD></TR>'
        else:
            rows += f'<TR><TD ALIGN="LEFT" PORT="{field_name}">{icon}{field_name}</TD></TR>'
    
    if len(sorted_fields) > 30:
        rows += f'<TR><TD ALIGN="LEFT">... +{len(sorted_fields) - 30} more fields</TD></TR>'
    
    label = f'''<<TABLE BORDER="1" CELLBORDER="0" CELLSPACING="0" BGCOLOR="{color}">
        <TR><TD COLSPAN="1" BGCOLOR="{color}"><B>{table_name}</B></TD></TR>
        <HR/>
        {rows}
    </TABLE>>'''
    
    return label


def export_data_model(dot, format: str = 'svg') -> bytes:
    """
    Export data model to specified format.
    
    Args:
        dot: Graphviz Digraph object
        format: Output format ('svg', 'png', 'pdf')
        
    Returns:
        Bytes of the rendered image
    """
    if dot is None:
        return None
    
    try:
        return dot.pipe(format=format)
    except Exception:
        return None
