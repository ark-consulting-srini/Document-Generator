"""
Common helper utilities for T - BRD STTM Generator
"""

import re
from typing import Optional


def format_datatype(datatype: str, precision: str = '', scale: str = None, nullable: str = None) -> str:
    """
    Format datatype with precision and nullable info into a single string.
    
    Handles Informatica XML datatypes that may contain placeholders like:
    - NUMBER(P,S) -> NUMBER(10,2) if precision=10 and scale=2
    - VARCHAR2(P) -> VARCHAR2(50) if precision=50
    
    Args:
        datatype: Base datatype name
        precision: Precision value
        scale: Scale value (optional)
        nullable: Nullable indicator
        
    Returns:
        Formatted datatype string like "VARCHAR2(50) NOTNULL"
    """
    if not datatype:
        return ''
    
    result = datatype.upper()
    
    # Check if datatype already has placeholders like (P,S) or (P)
    if '(P,S)' in result or '(P, S)' in result:
        # Replace P,S placeholders with actual values
        if precision and scale:
            result = result.replace('(P,S)', f'({precision},{scale})')
            result = result.replace('(P, S)', f'({precision},{scale})')
        elif precision:
            result = result.replace('(P,S)', f'({precision})')
            result = result.replace('(P, S)', f'({precision})')
        else:
            # Remove placeholder if no precision/scale provided
            result = result.replace('(P,S)', '')
            result = result.replace('(P, S)', '')
    elif '(P)' in result:
        # Replace P placeholder with actual precision
        if precision:
            result = result.replace('(P)', f'({precision})')
        else:
            result = result.replace('(P)', '')
    elif precision:
        # No placeholder exists, append precision/scale if datatype doesn't already have parentheses
        if '(' not in result:
            if scale:
                result += f"({precision},{scale})"
            else:
                result += f"({precision})"
    
    # Add NOTNULL only if not nullable
    if nullable and str(nullable).upper() in ['NOTNULL', 'NOT NULL', 'N', 'NO', '0']:
        result += ' NOTNULL'
    
    return result


def format_key_type(keytype: str) -> str:
    """
    Format key type - only show if PRIMARY KEY.
    
    Args:
        keytype: Key type from XML
        
    Returns:
        'PK' for primary key, '' otherwise
    """
    if keytype and 'PRIMARY' in str(keytype).upper():
        return 'PK'
    return ''


def format_business_name(technical_name: str) -> str:
    """
    Convert technical name to business-friendly format.
    Example: SAP_ASSET_POSTING_DOC_HDR -> Sap Asset Posting Doc Hdr
    
    Args:
        technical_name: Technical/database column name
        
    Returns:
        Business-friendly formatted name
    """
    if not technical_name:
        return ''
    
    # Replace underscores with spaces and title case
    words = technical_name.replace('_', ' ').split()
    
    # Handle common abbreviations
    abbreviations = {
        'SAP', 'ODS', 'DW', 'ETL', 'ID', 'NO', 'AMT', 'QTY', 'PCT', 
        'HDR', 'DTL', 'TRX', 'DOC', 'CD', 'DT', 'IND', 'NBR', 'NUM'
    }
    
    formatted_words = []
    for word in words:
        if word.upper() in abbreviations:
            formatted_words.append(word.upper())
        else:
            formatted_words.append(word.title())
    
    return ' '.join(formatted_words)


def normalize_field_name(field_name: str) -> str:
    """
    Normalize field name by removing suffixes and prefixes.
    
    Args:
        field_name: Original field name
        
    Returns:
        Normalized field name
    """
    # Remove trailing digits
    base = re.sub(r'\d+$', '', field_name)
    
    # Remove common prefixes
    for prefix in ['OUT_', 'IN_', 'SRC_', 'TGT_', 'lkp_', 'var_', 'o_', 'i_']:
        if base.lower().startswith(prefix.lower()):
            base = base[len(prefix):]
    
    return base


def clean_table_name(name: str) -> str:
    """
    Clean table name by removing shortcuts and prefixes.
    
    Args:
        name: Original table/instance name
        
    Returns:
        Cleaned table name
    """
    if not name:
        return ''
    
    # Remove common shortcuts
    cleaned = name.replace('Shortcut_to_', '')
    cleaned = cleaned.replace('SQ_', '')
    
    return cleaned


def is_special_source(source_name: str) -> bool:
    """
    Check if source name is a special/derived type (not an actual table).
    
    Args:
        source_name: Source table name
        
    Returns:
        True if special source type
    """
    special_sources = {
        'Derived', 'Hardcoded', 'SYSTEM', 'UNCONNECTED', 
        'SEQUENCE_GENERATOR', 'Lookup/Expression'
    }
    return source_name in special_sources


def truncate_text(text: str, max_length: int = 100, suffix: str = '...') -> str:
    """
    Truncate text to maximum length with suffix.
    
    Args:
        text: Text to truncate
        max_length: Maximum length
        suffix: Suffix to add if truncated
        
    Returns:
        Truncated text
    """
    if not text or len(text) <= max_length:
        return text or ''
    return text[:max_length - len(suffix)] + suffix


def estimate_tokens(text: str) -> int:
    """
    Estimate number of tokens in text (rough approximation: ~4 chars per token).
    
    Args:
        text: Text to estimate tokens for
        
    Returns:
        Estimated token count
    """
    if not text:
        return 0
    return len(text) // 4


def clean_sql_identifier(name: str) -> str:
    """
    Clean a name to be a valid SQL identifier.
    
    Args:
        name: Original name
        
    Returns:
        Valid SQL identifier
    """
    if not name:
        return ''
    
    # Replace spaces and special characters with underscore
    cleaned = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    
    # Remove multiple consecutive underscores
    cleaned = re.sub(r'_+', '_', cleaned)
    
    # Remove leading/trailing underscores
    cleaned = cleaned.strip('_')
    
    # Ensure doesn't start with a number
    if cleaned and cleaned[0].isdigit():
        cleaned = '_' + cleaned
    
    return cleaned.upper()


def parse_expression_fields(expression: str) -> list:
    """
    Extract field references from an Informatica expression.
    
    Args:
        expression: Informatica expression string
        
    Returns:
        List of field names referenced in the expression
    """
    if not expression:
        return []
    
    # SQL keywords and functions to exclude
    sql_keywords = {
        'IIF', 'ISNULL', 'NULL', 'SYSDATE', 'AND', 'OR', 'NOT', 'THEN', 'ELSE',
        'END', 'CASE', 'WHEN', 'TRUE', 'FALSE', 'VAR', 'DECODE', 'TO_DATE',
        'TO_CHAR', 'TO_DECIMAL', 'LTRIM', 'RTRIM', 'SUBSTR', 'INSTR', 'LENGTH',
        'ERROR', 'ABORT', 'LOWER', 'UPPER', 'MIN', 'MAX', 'AVG', 'SUM', 'COUNT',
        'SESSSTARTTIME', 'TRUNC', 'ROUND', 'DATE_DIFF', 'ADD_TO_DATE', 'GET_DATE_PART',
        'YYYY', 'MM', 'DD', 'HH24', 'MI', 'SS', 'MS', 'US', 'NS', 'NVL', 'NVL2',
        'LOOKUP', 'REG_EXTRACT', 'REG_REPLACE', 'LPAD', 'RPAD', 'REPLACE'
    }
    
    # Extract potential field names (words that look like identifiers)
    potential_fields = re.findall(r'\b([A-Z][A-Z0-9_]+)\b', expression.upper())
    
    # Filter out keywords
    fields = [f for f in potential_fields if f not in sql_keywords]
    
    return list(set(fields))
