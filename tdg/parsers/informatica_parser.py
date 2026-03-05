"""
Informatica XML Parser — Technical Document Generator (TDG)

Parses Informatica PowerCenter XML mapping files and extracts:
- Source and target table definitions
- Transformation logic and expressions
- Field-level lineage (source-to-target mapping)
- Lookup conditions and SQL overrides
"""

import xml.etree.ElementTree as ET
from collections import defaultdict
import re

from parsers.base_parser import BaseParser


class InformaticaLineageParser(BaseParser):
    """Parse Informatica PowerCenter XML mapping files and generate lineage + all component data."""

    @classmethod
    def can_parse(cls, filename: str, file_content: bytes = None) -> bool:
        """Accept mapping XMLs only — workflow XMLs are handled separately."""
        if file_content is None:
            return True
        content = file_content if isinstance(file_content, str) else file_content.decode("utf-8", errors="replace")
        # Workflow XMLs have a <WORKFLOW element; mapping XMLs do not
        return "<WORKFLOW" not in content[:4000]

    def __init__(self, xml_content, filename: str = "unknown"):
        super().__init__(xml_content, filename)
        self.tree = ET.ElementTree(ET.fromstring(self.file_content))
        self.root = self.tree.getroot()
        
    def parse_all(self):
        """Parse all components from XML"""
        # Parse sources
        for source in self.root.findall('.//SOURCE'):
            source_name = source.get('NAME')
            self.sources[source_name] = {
                'name': source_name,
                'fields': {}
            }
            
            for field in source.findall('SOURCEFIELD'):
                field_name = field.get('NAME')
                self.sources[source_name]['fields'][field_name] = {
                    'datatype': field.get('DATATYPE'),
                    'precision': field.get('PRECISION', ''),
                    'scale': field.get('SCALE', ''),
                    'nullable': field.get('NULLABLE'),
                    'keytype': field.get('KEYTYPE')
                }
                
                self.sources_data.append({
                    'Source_Name': source_name,
                    'Field_Name': field_name,
                    'Datatype': field.get('DATATYPE', ''),
                    'Precision': field.get('PRECISION', ''),
                    'Scale': field.get('SCALE', ''),
                    'Nullable': field.get('NULLABLE', ''),
                    'Key_Type': field.get('KEYTYPE', ''),
                    'Business_Name': field.get('BUSINESSNAME', ''),
                    'Description': field.get('DESCRIPTION', '')
                })
        
        # Parse targets
        for target in self.root.findall('.//TARGET'):
            target_name = target.get('NAME')
            self.targets[target_name] = {
                'name': target_name,
                'fields': {}
            }
            
            for field in target.findall('TARGETFIELD'):
                field_name = field.get('NAME')
                self.targets[target_name]['fields'][field_name] = {
                    'datatype': field.get('DATATYPE'),
                    'precision': field.get('PRECISION', ''),
                    'scale': field.get('SCALE', ''),
                    'nullable': field.get('NULLABLE'),
                    'keytype': field.get('KEYTYPE')
                }
                
                self.targets_data.append({
                    'Target_Name': target_name,
                    'Field_Name': field_name,
                    'Datatype': field.get('DATATYPE', ''),
                    'Precision': field.get('PRECISION', ''),
                    'Scale': field.get('SCALE', ''),
                    'Nullable': field.get('NULLABLE', ''),
                    'Key_Type': field.get('KEYTYPE', ''),
                    'Business_Name': field.get('BUSINESSNAME', ''),
                    'Description': field.get('DESCRIPTION', '')
                })
        
        # Parse mappings
        for mapping in self.root.findall('.//MAPPING'):
            mapping_name = mapping.get('NAME')
            self.mappings[mapping_name] = {
                'name': mapping_name,
                'instances': {},
                'target_instances': {},
                'source_instances': {},
                'transformations': {},
                'connectors': [],
                'expression_logic': {},
                'sq_field_sources': {},
                'field_expressions': {},  # Track expressions for field derivation
                'transformation_fields': {}  # Track all fields per transformation
            }
            
            # Parse instances
            for instance in mapping.findall('.//INSTANCE'):
                inst_name = instance.get('NAME')
                trans_name = instance.get('TRANSFORMATION_NAME', '')
                inst_type = instance.get('TYPE', '')
                
                self.mappings[mapping_name]['instances'][inst_name] = {
                    'transformation_name': trans_name,
                    'instance_type': inst_type
                }
                
                self.instances_data.append({
                    'Mapping_Name': mapping_name,
                    'Instance_Name': inst_name,
                    'Transformation_Name': trans_name,
                    'Instance_Type': inst_type,
                    'Description': instance.get('DESCRIPTION', '')
                })
                
                if inst_type == 'TARGET':
                    actual_target = trans_name.replace('Shortcut_to_', '')
                    self.mappings[mapping_name]['target_instances'][inst_name] = actual_target
                
                if inst_type == 'SOURCE':
                    actual_source = trans_name.replace('Shortcut_to_', '')
                    self.mappings[mapping_name]['source_instances'][inst_name] = actual_source
            
            # Parse transformations and their fields
            for trans in mapping.findall('.//TRANSFORMATION'):
                trans_name = trans.get('NAME')
                trans_type = trans.get('TYPE')
                
                trans_info = {
                    'name': trans_name,
                    'type': trans_type,
                    'fields': {},
                    'input_fields': [],
                    'output_fields': [],
                    'lookup_condition': '',
                    'lookup_sql': '',
                    'lookup_table': '',
                    'sql_query': '',  # For Source Qualifier SQL Query
                    'user_defined_join': '',  # For Source Qualifier joins
                    'source_filter': '',  # For Source Qualifier filter
                    'table_attributes': {}
                }
                
                # Parse TABLEATTRIBUTE elements (contains SQL, conditions, etc.)
                for attr in trans.findall('TABLEATTRIBUTE'):
                    attr_name = attr.get('NAME', '').lower()
                    attr_value = attr.get('VALUE', '')
                    trans_info['table_attributes'][attr_name] = attr_value
                    
                    # Capture specific attributes based on transformation type
                    # Lookup attributes
                    if 'lookup condition' in attr_name or 'lookup_condition' in attr_name:
                        trans_info['lookup_condition'] = attr_value
                    elif 'lookup sql override' in attr_name:
                        trans_info['lookup_sql'] = attr_value
                    elif 'lookup table name' in attr_name:
                        trans_info['lookup_table'] = attr_value
                    
                    # Source Qualifier attributes
                    elif 'sql query' in attr_name:
                        trans_info['sql_query'] = attr_value
                    elif 'user defined join' in attr_name:
                        trans_info['user_defined_join'] = attr_value
                    elif 'source filter' in attr_name:
                        trans_info['source_filter'] = attr_value
                    
                    # Generic SQL override (works for multiple transformation types)
                    elif 'sql override' in attr_name or 'sql' in attr_name:
                        if not trans_info['sql_query'] and not trans_info['lookup_sql']:
                            trans_info['sql_query'] = attr_value
                
                # Track fields with their port types
                for field in trans.findall('TRANSFORMFIELD'):
                    field_name = field.get('NAME')
                    port_type = field.get('PORTTYPE', '')
                    expression = field.get('EXPRESSION', '')
                    
                    trans_info['fields'][field_name] = {
                        'datatype': field.get('DATATYPE'),
                        'expression': expression,
                        'porttype': port_type
                    }
                    
                    # Track input/output fields
                    if 'INPUT' in port_type.upper():
                        trans_info['input_fields'].append(field_name)
                    if 'OUTPUT' in port_type.upper():
                        trans_info['output_fields'].append(field_name)
                    
                    # Store field expressions for tracing
                    if expression and expression != field_name:
                        key = f"{trans_name}::{field_name}"
                        self.mappings[mapping_name]['field_expressions'][key] = expression
                        
                        # Also store in expression_logic for the lineage record
                        base_name = re.sub(r'\d+$', '', field_name)
                        if base_name not in self.mappings[mapping_name]['expression_logic']:
                            self.mappings[mapping_name]['expression_logic'][base_name] = []
                        self.mappings[mapping_name]['expression_logic'][base_name].append({
                            'transformation': trans_name,
                            'expression': expression
                        })
                    
                    # Add to transformations reference data
                    self.transformations_data.append({
                        'Mapping_Name': mapping_name,
                        'Transformation_Name': trans_name,
                        'Transformation_Type': trans_type,
                        'Field_Name': field_name,
                        'Datatype': field.get('DATATYPE', ''),
                        'Precision': field.get('PRECISION', ''),
                        'Scale': field.get('SCALE', ''),
                        'Port_Type': port_type,
                        'Expression': expression or '',
                        'Description': field.get('DESCRIPTION', '')
                    })
                
                self.mappings[mapping_name]['transformations'][trans_name] = trans_info
                self.mappings[mapping_name]['transformation_fields'][trans_name] = trans_info['fields']
            
            # Parse connectors
            for conn in mapping.findall('.//CONNECTOR'):
                conn_data = {
                    'from_instance': conn.get('FROMINSTANCE'),
                    'from_field': conn.get('FROMFIELD'),
                    'to_instance': conn.get('TOINSTANCE'),
                    'to_field': conn.get('TOFIELD')
                }
                self.mappings[mapping_name]['connectors'].append(conn_data)
                
                self.connectors_data.append({
                    'Mapping_Name': mapping_name,
                    'From_Instance': conn.get('FROMINSTANCE'),
                    'From_Field': conn.get('FROMFIELD'),
                    'From_Instance_Type': conn.get('FROMINSTANCETYPE', ''),
                    'To_Instance': conn.get('TOINSTANCE'),
                    'To_Field': conn.get('TOFIELD'),
                    'To_Instance_Type': conn.get('TOINSTANCETYPE', '')
                })
            
            # Build SQ field to source mapping
            self.build_sq_source_mapping(mapping_name, mapping)
    
    def build_sq_source_mapping(self, mapping_name, mapping_elem):
        """
        Map Source Qualifier fields to actual source tables.
        Also tracks HDR vs DTL distinction based on field naming.
        
        Convention in joined HDR/DTL mappings:
        - Fields without suffix -> from HDR (header/document table)
        - Fields with numeric suffix (e.g., FIELD1) -> from DTL (line/detail table)
        """
        sq_name = None
        
        for trans in mapping_elem.findall('.//TRANSFORMATION[@TYPE="Source Qualifier"]'):
            sq_name = trans.get('NAME')
            break
        
        if not sq_name:
            return
        
        # Track all SQ field to source mappings
        # Also build a map of fields that exist in BOTH HDR and DTL
        hdr_fields = {}  # field_name -> source info
        dtl_fields = {}  # field_name -> source info
        
        for conn in mapping_elem.findall('.//CONNECTOR'):
            if conn.get('TOINSTANCE') == sq_name:
                from_inst = conn.get('FROMINSTANCE')
                to_field = conn.get('TOFIELD')
                from_field = conn.get('FROMFIELD')
                
                source_inst = self.mappings[mapping_name]['instances'].get(from_inst, {})
                if source_inst.get('instance_type') == 'SOURCE':
                    source_table = source_inst.get('transformation_name', '').replace('Shortcut_to_', '')
                    
                    source_info = {
                        'source_table': source_table,
                        'source_field': from_field,
                        'is_hdr': 'HDR' in source_table.upper() or 'DOC' in source_table.upper(),
                        'is_dtl': 'DTL' in source_table.upper() or 'LINE' in source_table.upper()
                    }
                    
                    self.mappings[mapping_name]['sq_field_sources'][to_field] = source_info
                    
                    # Track HDR vs DTL separately for fields that exist in both
                    if source_info['is_hdr']:
                        hdr_fields[from_field] = source_info
                    elif source_info['is_dtl']:
                        dtl_fields[from_field] = source_info
        
        # Store the HDR/DTL field maps for later use
        self.mappings[mapping_name]['hdr_fields'] = hdr_fields
        self.mappings[mapping_name]['dtl_fields'] = dtl_fields
        
        # Identify fields that exist in BOTH HDR and DTL (these have different INSERT vs UPDATE sources)
        common_fields = set(hdr_fields.keys()) & set(dtl_fields.keys())
        self.mappings[mapping_name]['dual_source_fields'] = common_fields
    
    def normalize_field_name(self, field_name):
        """Normalize field name by removing suffixes and prefixes"""
        # Remove trailing digits
        base = re.sub(r'\d+$', '', field_name)
        
        # Remove common prefixes
        for prefix in ['OUT_', 'IN_', 'SRC_', 'TGT_', 'lkp_', 'var_', 'o_', 'i_']:
            if base.lower().startswith(prefix.lower()):
                base = base[len(prefix):]
        
        return base
    
    def fuzzy_match_field(self, output_field, input_fields, strict=False):
        """
        Enhanced fuzzy match field names with multiple strategies.
        This is critical for handling Router transformations and field name variations.
        """
        if not input_fields:
            return None
        
        # Strategy 1: Exact match
        if output_field in input_fields:
            return output_field
        
        # Strategy 2: Remove trailing digits (Router adds 1, 2, 3 for output groups)
        base = re.sub(r'\d+$', '', output_field)
        if base in input_fields:
            return base
        
        # Strategy 3: Remove common prefixes
        prefixes = ['OUT_', 'IN_', 'SRC_', 'TGT_', 'lkp_', 'var_', 'o_', 'i_']
        for prefix in prefixes:
            if output_field.lower().startswith(prefix.lower()):
                without_prefix = output_field[len(prefix):]
                if without_prefix in input_fields:
                    return without_prefix
                # Also try removing trailing digits from the prefix-stripped version
                base_no_prefix = re.sub(r'\d+$', '', without_prefix)
                if base_no_prefix in input_fields:
                    return base_no_prefix
        
        # Strategy 4: Handle special naming conventions
        # asset_key_out -> ASSET_KEY
        if output_field.lower().endswith('_out'):
            without_out = output_field[:-4]
            if without_out in input_fields:
                return without_out
            if without_out.upper() in input_fields:
                return without_out.upper()
        
        # Strategy 5: Case-insensitive matching
        output_lower = output_field.lower()
        for inp in input_fields:
            if inp.lower() == output_lower:
                return inp
        
        # Strategy 6: Normalized matching (remove all suffixes and compare)
        output_normalized = self.normalize_field_name(output_field).lower()
        for inp in input_fields:
            if self.normalize_field_name(inp).lower() == output_normalized:
                return inp
        
        if strict:
            return None
        
        # Strategy 7: Containment matching (for fields like REC_OBJ_1_NO -> RECEIVER_OBJ_1_NO)
        for inp in input_fields:
            # Check if core part matches
            inp_normalized = self.normalize_field_name(inp).lower()
            if inp_normalized in output_normalized or output_normalized in inp_normalized:
                if len(min(inp_normalized, output_normalized)) > 3:
                    return inp
        
        # Strategy 8: Partial matching with significant overlap
        output_parts = set(re.split(r'[_\d]+', output_field.lower()))
        output_parts.discard('')
        for inp in input_fields:
            inp_parts = set(re.split(r'[_\d]+', inp.lower()))
            inp_parts.discard('')
            # If significant overlap in parts
            common = output_parts & inp_parts
            if len(common) >= 2 or (len(common) == 1 and len(list(common)[0]) > 5):
                return inp
        
        return None
    
    def find_field_source_from_expression(self, expression, mapping_name):
        """Extract source field references from an expression and trace to actual sources"""
        if not expression:
            return None
        
        expr_stripped = expression.strip()
        
        # ENHANCEMENT: Check for hardcoded/literal values FIRST
        # Quoted string literals (e.g., 'PAL', 'N/A', 'SAP')
        if expr_stripped.startswith("'") and expr_stripped.endswith("'"):
            return {'source_table': 'Hardcoded', 'source_field': expr_stripped}
        
        # Numeric literals (e.g., -1, 0, 100.5)
        if re.match(r'^-?\d+(\.\d+)?$', expr_stripped):
            return {'source_table': 'Hardcoded', 'source_field': expr_stripped}
        
        # Check for SYSDATE
        if 'SYSDATE' in expression.upper():
            return {'source_table': 'SYSTEM', 'source_field': 'SYSDATE'}
        
        # Check for concatenation (derived field)
        if '||' in expression:
            return {'source_table': 'Derived', 'source_field': f"Expression: {expression[:80]}"}
        
        # Check for division/calculation patterns (like CUBE_ADJUSTED_WGT)
        if any(op in expression for op in ['/', '*', '+', '-']) and not expression.startswith(expression.split()[0] if expression.split() else ''):
            # Extract field names from the expression
            potential_fields = re.findall(r'\b(in_[A-Z][A-Z0-9_]*|[A-Z][A-Z0-9_]*)\b', expression)
            if potential_fields:
                return {'source_table': 'Derived', 'source_field': f"Expression: {expression[:100]}"}
        
        mapping = self.mappings[mapping_name]
        
        # Look for field references in the expression
        # Common patterns: IIF(ISNULL(FIELD), default, FIELD), FIELD, :LKP.FIELD
        
        # Extract potential field names from expression
        potential_fields = re.findall(r'\b([A-Z][A-Z0-9_]+)\b', expression.upper())
        
        # Filter out SQL keywords and functions
        sql_keywords = {
            'IIF', 'ISNULL', 'NULL', 'SYSDATE', 'AND', 'OR', 'NOT', 'THEN', 'ELSE', 
            'END', 'CASE', 'WHEN', 'TRUE', 'FALSE', 'VAR', 'DECODE', 'TO_DATE', 
            'TO_CHAR', 'TO_DECIMAL', 'LTRIM', 'RTRIM', 'SUBSTR', 'INSTR', 'LENGTH',
            'ERROR', 'ABORT', 'LOWER', 'UPPER', 'MIN', 'MAX', 'AVG', 'SUM', 'COUNT',
            'SESSSTARTTIME', 'TRUNC', 'ROUND', 'DATE_DIFF', 'ADD_TO_DATE', 'GET_DATE_PART',
            'YYYY', 'MM', 'DD', 'HH24', 'MI', 'SS', 'MS', 'US', 'NS'
        }
        potential_fields = [f for f in potential_fields if f not in sql_keywords]
        
        # Check if any of these are source fields
        for field in potential_fields:
            for source_name, source_info in self.sources.items():
                if field in source_info['fields']:
                    return {'source_table': source_name, 'source_field': field}
        
        # Check SQ field sources
        for field in potential_fields:
            if field in mapping['sq_field_sources']:
                return mapping['sq_field_sources'][field]
        
        # Check for lookup references (fields starting with lkp_ or in_)
        for field in potential_fields:
            if field.lower().startswith('lkp_') or field.lower().startswith('in_'):
                return {'source_table': 'Lookup/Expression', 'source_field': field}
        
        # If expression contains calculations, report as derived
        if any(op in expression for op in ['/', '*', '+', '-']):
            return {'source_table': 'Derived', 'source_field': f"Expression: {expression[:100]}"}
        
        return None
    
    def build_lineage(self, progress_callback=None):
        """Build complete lineage with enhanced tracing for INSERT vs UPDATE paths"""
        all_lineage = []
        
        total_mappings = len(self.mappings)
        
        for idx, (mapping_name, mapping_info) in enumerate(self.mappings.items()):
            if progress_callback:
                progress_callback(idx + 1, total_mappings, mapping_name)
            
            # Build backward connections map
            backward_conn = defaultdict(list)
            for conn in mapping_info['connectors']:
                to_key = f"{conn['to_instance']}::{conn['to_field']}"
                backward_conn[to_key].append(conn)
            
            # Build forward connections map (for alternative tracing)
            forward_conn = defaultdict(list)
            for conn in mapping_info['connectors']:
                from_key = f"{conn['from_instance']}::{conn['from_field']}"
                forward_conn[from_key].append(conn)
            
            # Identify INSERT and UPDATE target instances
            insert_instances = set()
            update_instances = set()
            
            for inst_name in mapping_info['target_instances'].keys():
                if 'insert' in inst_name.lower():
                    insert_instances.add(inst_name)
                elif 'update' in inst_name.lower():
                    update_instances.add(inst_name)
                else:
                    # If no clear distinction, add to both
                    insert_instances.add(inst_name)
                    update_instances.add(inst_name)
            
            # ENHANCEMENT: Track which target fields have connectors
            # This is used to detect UNCONNECTED fields
            connected_target_fields = set()
            
            # Get ALL target fields (from connectors going to target instances)
            # Track INSERT and UPDATE paths separately
            target_field_paths = defaultdict(lambda: {'insert': [], 'update': []})
            
            for conn in mapping_info['connectors']:
                if conn['to_instance'] in mapping_info['target_instances']:
                    target_field = conn['to_field']
                    connected_target_fields.add(target_field)  # Track connected fields
                    
                    path_info = {
                        'from_instance': conn['from_instance'],
                        'from_field': conn['from_field'],
                        'to_instance': conn['to_instance']
                    }
                    
                    if conn['to_instance'] in insert_instances:
                        target_field_paths[target_field]['insert'].append(path_info)
                    if conn['to_instance'] in update_instances:
                        target_field_paths[target_field]['update'].append(path_info)
            
            # Trace each target field
            for target_field, paths in target_field_paths.items():
                insert_lineage = None
                update_lineage = None
                
                # Trace INSERT paths
                for path_info in paths['insert']:
                    result = self.trace_path_enhanced(
                        path_info['from_instance'],
                        path_info['from_field'],
                        backward_conn,
                        mapping_name,
                        visited=set()
                    )
                    if result:
                        insert_lineage = result
                        break
                
                # Trace UPDATE paths separately
                for path_info in paths['update']:
                    result = self.trace_path_enhanced(
                        path_info['from_instance'],
                        path_info['from_field'],
                        backward_conn,
                        mapping_name,
                        visited=set()
                    )
                    if result:
                        update_lineage = result
                        break
                
                # Create lineage record if we found any path
                if insert_lineage or update_lineage:
                    lineage_record = self.create_lineage_record(
                        mapping_name,
                        target_field,
                        insert_lineage,
                        update_lineage,
                        backward_conn
                    )
                    all_lineage.append(lineage_record)
            
            # ENHANCEMENT: Detect and report UNCONNECTED fields
            # These are fields defined in the target table but have no connectors pointing to them
            for inst_name, target_name in mapping_info['target_instances'].items():
                if target_name in self.targets:
                    all_target_fields = set(self.targets[target_name]['fields'].keys())
                    unconnected_fields = all_target_fields - connected_target_fields
                    
                    for field_name in unconnected_fields:
                        field_meta = self.targets[target_name]['fields'][field_name]
                        # Format target datatype
                        target_dtype = self.format_datatype(
                            field_meta.get('datatype', ''),
                            field_meta.get('precision', ''),
                            field_meta.get('scale', ''),
                            field_meta.get('nullable', '')
                        )
                        target_key = self.format_key_type(field_meta.get('keytype', ''))
                        
                        # Get business names for target
                        target_table_business_name = self.get_table_business_name(target_name, 'target')
                        target_column_business_name = self.get_column_business_name(target_name, field_name, 'target')
                        
                        all_lineage.append({
                            'Mapping_Name': mapping_name,
                            'Source_Table_INSERT': 'UNCONNECTED',
                            'Source_Table_Business_Name': '',
                            'Source_Column_INSERT': 'UNCONNECTED',
                            'Source_Column_Business_Name': '',
                            'Source_Table_UPDATE': '',
                            'Source_Column_UPDATE': '',
                            'Source_Datatype': '',
                            'Source_Key': '',
                            'Target_Table': target_name,
                            'Target_Table_Business_Name': target_table_business_name,
                            'Target_Column': field_name,
                            'Target_Column_Business_Name': target_column_business_name,
                            'Target_Datatype': target_dtype,
                            'Target_Key': target_key,
                            'Expression_Logic': '',
                            'Lookup_Condition': '',
                            'SQL_Override': '',
                            'Transformations_INSERT': '',
                            'Transformations_UPDATE': ''
                        })
                        # Mark as connected to avoid duplicates from multiple target instances
                        connected_target_fields.add(field_name)
        
        return all_lineage
    
    def get_lookup_source_table(self, instance_name, trans_name):
        """
        Extract the actual source table name from a lookup transformation.
        Naming convention: lkp_<table_name> or Shortcut_to_lkp_<table_name>
        """
        # Get the base name
        name = trans_name if trans_name else instance_name
        name = name.replace('Shortcut_to_', '')
        
        # Remove trailing digits (e.g., lkp_sap_setl_ord_dist_rules1 -> lkp_sap_setl_ord_dist_rules)
        name = re.sub(r'\d+$', '', name)
        
        # Extract table name after lkp_ prefix
        if name.lower().startswith('lkp_'):
            table_name = name[4:]  # Remove 'lkp_' prefix
            # Convert to uppercase for consistency with source table naming
            return table_name.upper()
        
        return name.upper()
    
    def trace_input_field_source(self, instance, input_field, backward_conn, mapping_name):
        """
        Trace an input field (e.g., in_EAW_NUMR_CONV_BASE_UNIT) back to its source.
        Returns a string like "MATL_UOM_MAST_ODS.NUMR_CONV_BASE_UNIT" or None.
        """
        mapping = self.mappings[mapping_name]
        
        # Build the lookup key for this input field
        lookup_key = f"{instance}::{input_field}"
        
        if lookup_key in backward_conn:
            for conn in backward_conn[lookup_key]:
                from_inst = conn['from_instance']
                from_field = conn['from_field']
                
                # Check if from_instance is a lookup
                inst_info = mapping['instances'].get(from_inst, {})
                trans_name = inst_info.get('transformation_name', '')
                trans_info = mapping['transformations'].get(from_inst, {})
                trans_type = trans_info.get('type', '')
                
                if 'lkp_' in from_inst.lower() or 'Lookup' in trans_type:
                    # Get the lookup table name
                    lookup_table = self.get_lookup_source_table(from_inst, trans_name)
                    return f"{lookup_table}.{from_field}"
                
                # Check if it's a source
                if from_inst in mapping['source_instances']:
                    source_name = mapping['source_instances'][from_inst]
                    return f"{source_name}.{from_field}"
                
                # Check if it's a Source Qualifier
                if 'SQ_' in from_inst or trans_type == 'Source Qualifier':
                    sq_source = mapping['sq_field_sources'].get(from_field)
                    if sq_source:
                        return f"{sq_source['source_table']}.{sq_source['source_field']}"
                
                # Otherwise return the immediate source
                return f"{from_inst}.{from_field}"
        
        return None
    
    def determine_hdr_vs_dtl_source(self, field_name, mapping_name):
        """
        Determine if a field comes from HDR or DTL source based on SQ field naming convention.
        
        In Informatica mappings with joined HDR/DTL sources:
        - Fields WITHOUT numeric suffix in SQ -> from HDR (header/doc table)
        - Fields WITH numeric suffix (e.g., _NO1, _DATE1) in SQ -> from DTL (line/detail table)
        
        Returns: 'HDR', 'DTL', or None if cannot determine
        """
        mapping = self.mappings.get(mapping_name, {})
        sq_field_sources = mapping.get('sq_field_sources', {})
        
        # Check if field has a numeric suffix pattern suggesting DTL origin
        # Pattern: field ends with digit that's part of the suffix (not the field name)
        base_field = re.sub(r'(\d+)$', '', field_name)
        
        # If the field name ends with a number AND there's a non-suffixed version in sources
        if base_field != field_name:
            # This field has a numeric suffix - likely from DTL
            # Verify by checking if base field exists (would be HDR version)
            if base_field in sq_field_sources:
                return 'DTL'
        
        # Check the actual source from sq_field_sources
        if field_name in sq_field_sources:
            source_table = sq_field_sources[field_name].get('source_table', '')
            if 'DTL' in source_table.upper() or 'LINE' in source_table.upper() or 'DETAIL' in source_table.upper():
                return 'DTL'
            elif 'HDR' in source_table.upper() or 'DOC' in source_table.upper() or 'HEADER' in source_table.upper():
                return 'HDR'
        
        return None
    
    def trace_path_enhanced(self, instance, field, backward_conn, mapping_name, depth=0, max_depth=25, visited=None):
        """
        Enhanced backward tracing with proper handling of:
        - Router transformations with numbered output ports
        - Lookup transformations (return lookup table as source)
        - SYSDATE and system-generated values
        - Derived/concatenated fields
        - Expression-based field derivations
        - Field name variations (RECEIVER_OBJ -> REC_OBJ, etc.)
        - Hardcoded/literal values (e.g., 'PAL', 'N/A', -1)
        - Sequence generators
        """
        if visited is None:
            visited = set()
        
        if depth > max_depth:
            return None
        
        visit_key = f"{instance}::{field}::{depth}"
        if visit_key in visited:
            return None
        visited.add(visit_key)
        
        mapping = self.mappings[mapping_name]
        
        # Check if we've reached a source instance
        if instance in mapping['source_instances']:
            source_name = mapping['source_instances'][instance]
            return {
                'source_table': source_name,
                'source_field': field,
                'transformations': []
            }
        
        # Get transformation info for this instance
        trans_info = mapping['transformations'].get(instance, {})
        trans_type = trans_info.get('type', '')
        inst_info = mapping['instances'].get(instance, {})
        trans_name = inst_info.get('transformation_name', '')
        
        # ENHANCEMENT: Check for Sequence Generator
        if 'Sequence' in trans_type or 'seq_' in instance.lower() or 'Sequence' in instance:
            return {
                'source_table': 'SEQUENCE_GENERATOR',
                'source_field': f"{instance}.{field}",
                'transformations': [f"{instance} (Sequence)"]
            }
        
        # ENHANCEMENT: Check for hardcoded/literal values early
        trans_fields = mapping.get('transformation_fields', {}).get(instance, {})
        if field in trans_fields:
            field_info = trans_fields[field]
            expression = field_info.get('expression', '')
            if expression:
                expr_stripped = expression.strip()
                
                # Check for quoted string literals (e.g., 'PAL', 'N/A', 'SAP')
                if expr_stripped.startswith("'") and expr_stripped.endswith("'"):
                    return {
                        'source_table': 'Hardcoded',
                        'source_field': expr_stripped,
                        'transformations': [f"{instance} (Hardcoded)"]
                    }
                
                # Check for numeric literals (e.g., -1, 0, 100.5)
                if re.match(r'^-?\d+(\.\d+)?$', expr_stripped):
                    return {
                        'source_table': 'Hardcoded',
                        'source_field': expr_stripped,
                        'transformations': [f"{instance} (Hardcoded)"]
                    }
        
        # Check if this is a Lookup transformation - these are SOURCE of data
        if trans_type == 'Lookup Procedure' or 'lkp_' in instance.lower() or 'Shortcut_to_lkp_' in trans_name:
            # Get the actual lookup source table name from naming convention
            lookup_table = self.get_lookup_source_table(instance, trans_name)
            
            # Check if this field is a LOOKUP/OUTPUT port (meaning it comes FROM the lookup table)
            field_info = trans_info.get('fields', {}).get(field, {})
            port_type = field_info.get('porttype', '').upper()
            
            if 'LOOKUP' in port_type or ('OUTPUT' in port_type and 'INPUT' not in port_type):
                return {
                    'source_table': lookup_table,
                    'source_field': field,
                    'transformations': [f"{instance} (Lookup)"]
                }
        
        # Check if this is a Source Qualifier
        if trans_name.startswith('SQ_') or trans_type == 'Source Qualifier':
            # First try exact match
            sq_source = mapping['sq_field_sources'].get(field)
            if sq_source:
                return {
                    'source_table': sq_source['source_table'],
                    'source_field': sq_source['source_field'],
                    'transformations': []
                }
            
            # Try fuzzy matching for SQ fields
            sq_fields = list(mapping['sq_field_sources'].keys())
            matched_sq = self.fuzzy_match_field(field, sq_fields)
            if matched_sq:
                sq_source = mapping['sq_field_sources'][matched_sq]
                return {
                    'source_table': sq_source['source_table'],
                    'source_field': sq_source['source_field'],
                    'transformations': []
                }
            
            # For fields with numeric suffix (e.g., ACCT_LINE_ITEM_NO1), 
            # check if this indicates DTL source
            base_field = re.sub(r'(\d+)$', '', field)
            if base_field != field and base_field in mapping['sq_field_sources']:
                # Field has suffix - check if there's a suffixed version that maps to DTL
                suffixed_field = f"{base_field}1"
                if suffixed_field in mapping['sq_field_sources']:
                    sq_source = mapping['sq_field_sources'][suffixed_field]
                    return {
                        'source_table': sq_source['source_table'],
                        'source_field': sq_source['source_field'],
                        'transformations': []
                    }
                # Otherwise use the base field (HDR)
                sq_source = mapping['sq_field_sources'][base_field]
                return {
                    'source_table': sq_source['source_table'],
                    'source_field': sq_source['source_field'],
                    'transformations': []
                }
        
        # Check for SYSDATE or system-generated values in expressions
        trans_fields = mapping.get('transformation_fields', {}).get(instance, {})
        if field in trans_fields:
            field_info = trans_fields[field]
            expression = field_info.get('expression', '')
            port_type = field_info.get('porttype', '').upper()
            
            if expression:
                # Check for SYSDATE
                if 'SYSDATE' in expression.upper():
                    return {
                        'source_table': 'SYSTEM',
                        'source_field': 'SYSDATE',
                        'transformations': [f"{instance} (Expression: {expression[:50]})"]
                    }
                
                # Check for concatenation/derived fields
                if '||' in expression:
                    return {
                        'source_table': 'Derived',
                        'source_field': f"Expression: {expression[:100]}",
                        'transformations': [f"{instance} (Expression)"]
                    }
                
                # Check for calculations (division, multiplication, etc.) - like CUBE_ADJUSTED_WGT
                if any(op in expression for op in ['/', '*', '+', '-']) and not expression.startswith(field):
                    # This is a calculated field - extract input field references
                    input_refs = re.findall(r'\b(in_[A-Za-z][A-Za-z0-9_]*|var_[A-Za-z][A-Za-z0-9_]*)\b', expression)
                    if input_refs:
                        # ENHANCEMENT: Trace the input fields back to their sources
                        source_details = []
                        for input_ref in input_refs[:3]:  # Limit to first 3 inputs
                            # Try to find where this input comes from via connectors
                            input_source = self.trace_input_field_source(instance, input_ref, backward_conn, mapping_name)
                            if input_source:
                                source_details.append(f"{input_ref} <- {input_source}")
                            else:
                                source_details.append(input_ref)
                        
                        return {
                            'source_table': 'Derived',
                            'source_field': f"Calculation: {'; '.join(source_details)}",
                            'transformations': [f"{instance} (Expression: {expression[:60]})"]
                        }
                
                # Check for local variable references (var_*)
                var_refs = re.findall(r'\bvar_([A-Za-z][A-Za-z0-9_]*)\b', expression)
                if var_refs and expression.startswith('var_'):
                    # This output uses a local variable - the variable contains the logic
                    # Look for the variable definition in same transformation
                    var_name = f"var_{var_refs[0]}" if var_refs else None
                    if var_name and var_name in trans_fields:
                        var_expr = trans_fields[var_name].get('expression', '')
                        if var_expr:
                            return {
                                'source_table': 'Derived',
                                'source_field': f"Expression: {var_expr[:100]}",
                                'transformations': [f"{instance} (Expression)"]
                            }
            
            # For OUTPUT ports with no expression but fed by lookups, trace the input
            if 'OUTPUT' in port_type and (not expression or expression == field):
                # Check if there's a corresponding input field
                input_field = f"in_{field}" if not field.startswith('in_') else field
                if input_field in trans_fields:
                    # This output just passes through an input - update field to trace
                    field = input_field
        
        # Build the lookup key
        start_key = f"{instance}::{field}"
        
        # Try to find connection with exact match first
        if start_key in backward_conn:
            for prev_conn in backward_conn[start_key]:
                result = self.trace_path_enhanced(
                    prev_conn['from_instance'],
                    prev_conn['from_field'],
                    backward_conn,
                    mapping_name,
                    depth + 1,
                    max_depth,
                    visited.copy()
                )
                if result:
                    # Add transformation to the chain if it's meaningful
                    if trans_type and trans_type not in ['Source Qualifier', 'Source Definition']:
                        result['transformations'].append(f"{instance} ({trans_type})")
                    return result
        
        # If exact match fails, try fuzzy matching
        possible_keys = [k for k in backward_conn.keys() if k.startswith(f"{instance}::")]
        if possible_keys:
            possible_fields = [k.split('::', 1)[1] for k in possible_keys]
            matched = self.fuzzy_match_field(field, possible_fields)
            
            if matched:
                matched_key = f"{instance}::{matched}"
                for prev_conn in backward_conn[matched_key]:
                    result = self.trace_path_enhanced(
                        prev_conn['from_instance'],
                        prev_conn['from_field'],
                        backward_conn,
                        mapping_name,
                        depth + 1,
                        max_depth,
                        visited.copy()
                    )
                    if result:
                        if trans_type and trans_type not in ['Source Qualifier', 'Source Definition']:
                            result['transformations'].append(f"{instance} ({trans_type})")
                        return result
        
        # Check for expression-based derivation with source field references
        expr_key = f"{instance}::{field}"
        if expr_key in mapping['field_expressions']:
            expression = mapping['field_expressions'][expr_key]
            # Check for SYSDATE first
            if 'SYSDATE' in expression.upper():
                return {
                    'source_table': 'SYSTEM',
                    'source_field': 'SYSDATE',
                    'transformations': [f"{instance} (Expression)"]
                }
            source_from_expr = self.find_field_source_from_expression(expression, mapping_name)
            if source_from_expr:
                return {
                    'source_table': source_from_expr['source_table'],
                    'source_field': source_from_expr['source_field'],
                    'transformations': [f"{instance} (Expression)"]
                }
        
        # For fields that come from lookups via Shortcut instances
        if 'Shortcut_to_lkp_' in instance or 'Shortcut_to_lkp_' in trans_name:
            lookup_table = self.get_lookup_source_table(instance, trans_name)
            return {
                'source_table': lookup_table,
                'source_field': field,
                'transformations': [f"{instance} (Lookup)"]
            }
        
        return None
    
    def format_datatype(self, datatype, precision, scale=None, nullable=None):
        """Format datatype with precision and nullable info into a single string like VARCHAR2(50) NOTNULL
        
        Handles Informatica XML datatypes that may contain placeholders like:
        - NUMBER(P,S) -> NUMBER(10,2) if precision=10 and scale=2
        - VARCHAR2(P) -> VARCHAR2(50) if precision=50
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
    
    def format_key_type(self, keytype):
        """Format key type - only show if PRIMARY KEY"""
        if keytype and 'PRIMARY' in str(keytype).upper():
            return 'PK'
        return ''
    
    def get_lookup_join_condition(self, mapping_name, lookup_instance):
        """Extract lookup join condition from transformation"""
        mapping = self.mappings[mapping_name]
        trans_info = mapping['transformations'].get(lookup_instance, {})
        
        # First priority: explicitly captured lookup_condition from TABLEATTRIBUTE
        if trans_info.get('lookup_condition'):
            return trans_info['lookup_condition']
        
        # Second priority: check table_attributes dictionary
        table_attrs = trans_info.get('table_attributes', {})
        for key, value in table_attrs.items():
            if 'condition' in key.lower() and value:
                return value
        
        # Third priority: Build condition from input fields (lookup keys)
        fields = trans_info.get('fields', {})
        input_fields = [f for f, info in fields.items() 
                       if 'INPUT' in str(info.get('porttype', '')).upper() 
                       and 'OUTPUT' not in str(info.get('porttype', '')).upper()
                       and 'LOOKUP' not in str(info.get('porttype', '')).upper()]
        
        if input_fields:
            # Format as condition: IN_FIELD = FIELD (typical lookup join pattern)
            conditions = []
            for inp_field in input_fields[:5]:  # Limit to 5 fields
                # Try to match input field to lookup field
                base_name = inp_field.replace('IN_', '').replace('in_', '')
                conditions.append(f"{inp_field} = {base_name}")
            return ' AND '.join(conditions)
        
        return ''
    
    def get_lookup_sql(self, mapping_name, lookup_instance):
        """Extract lookup SQL override from transformation"""
        mapping = self.mappings[mapping_name]
        trans_info = mapping['transformations'].get(lookup_instance, {})
        
        # Check for lookup_sql
        if trans_info.get('lookup_sql'):
            return trans_info['lookup_sql']
        
        # Check table_attributes
        table_attrs = trans_info.get('table_attributes', {})
        for key, value in table_attrs.items():
            if 'sql' in key.lower() and value:
                return value
        
        return ''
    
    def get_transformation_sql(self, mapping_name, trans_instance):
        """
        Get SQL from any transformation type:
        - Source Qualifier: SQL Query, User Defined Join, Source Filter
        - Lookup: Lookup SQL Override
        - SQL Transformation: SQL Query
        - Stored Procedure: SQL
        """
        mapping = self.mappings[mapping_name]
        trans_info = mapping['transformations'].get(trans_instance, {})
        
        if not trans_info:
            return ''
        
        # Priority order for SQL
        sql_parts = []
        
        # 1. SQL Query (Source Qualifier, SQL Transformation)
        if trans_info.get('sql_query'):
            sql_parts.append(trans_info['sql_query'])
        
        # 2. Lookup SQL Override
        if trans_info.get('lookup_sql'):
            sql_parts.append(trans_info['lookup_sql'])
        
        # 3. User Defined Join (Source Qualifier)
        if trans_info.get('user_defined_join'):
            sql_parts.append(f"JOIN: {trans_info['user_defined_join']}")
        
        # 4. Source Filter (Source Qualifier WHERE clause)
        if trans_info.get('source_filter'):
            sql_parts.append(f"FILTER: {trans_info['source_filter']}")
        
        # 5. Check table_attributes for any SQL-related attribute
        if not sql_parts:
            table_attrs = trans_info.get('table_attributes', {})
            for key, value in table_attrs.items():
                if value and ('sql' in key.lower() or 'query' in key.lower()):
                    sql_parts.append(value)
                    break
        
        return ' | '.join(sql_parts) if sql_parts else ''
    
    def get_source_qualifier_sql(self, mapping_name):
        """Get SQL from the Source Qualifier transformation in a mapping"""
        mapping = self.mappings[mapping_name]
        
        for trans_name, trans_info in mapping['transformations'].items():
            if trans_info.get('type') == 'Source Qualifier' or trans_name.startswith('SQ_'):
                sql = self.get_transformation_sql(mapping_name, trans_name)
                if sql:
                    return sql
        
        return ''
    
    def trace_derived_field_parent(self, field_name, mapping_name, backward_conn, depth=0, max_depth=5):
        """Trace where an in_COLUMN_NAME field actually comes from"""
        if depth > max_depth:
            return field_name
        
        mapping = self.mappings[mapping_name]
        
        # Look for connectors that feed this field
        for key, conns in backward_conn.items():
            if f"::{field_name}" in key:
                for conn in conns:
                    from_inst = conn['from_instance']
                    from_field = conn['from_field']
                    
                    # Check if from_inst is a lookup
                    if 'lkp_' in from_inst.lower():
                        lookup_table = self.get_lookup_source_table(from_inst, from_inst)
                        return f"{from_field} (from {lookup_table})"
                    
                    # Check if from_inst is a source
                    if from_inst in mapping['source_instances']:
                        source_name = mapping['source_instances'][from_inst]
                        return f"{from_field} (from {source_name})"
                    
                    # Check SQ
                    inst_info = mapping['instances'].get(from_inst, {})
                    trans_name = inst_info.get('transformation_name', '')
                    if trans_name.startswith('SQ_'):
                        sq_source = mapping['sq_field_sources'].get(from_field)
                        if sq_source:
                            return f"{sq_source['source_field']} (from {sq_source['source_table']})"
        
        return field_name
    
    def create_lineage_record(self, mapping_name, target_field, insert_lineage, update_lineage, backward_conn=None):
        """Create lineage record with all metadata"""
        mapping = self.mappings[mapping_name]
        
        # Get target table info
        target_table = None
        for inst_name, tgt_name in mapping['target_instances'].items():
            target_table = tgt_name
            break
        
        # Get target field metadata
        target_meta = {}
        if target_table and target_table in self.targets:
            if target_field in self.targets[target_table]['fields']:
                target_meta = self.targets[target_table]['fields'][target_field]
        
        # Get source info from lineage
        source_path = insert_lineage or update_lineage
        source_meta = {}
        
        if source_path:
            src_table = source_path['source_table']
            src_field = source_path['source_field']
            
            if src_table in self.sources and src_field in self.sources[src_table]['fields']:
                source_meta = self.sources[src_table]['fields'][src_field]
        
        # Get expression logic
        expr_logic = ''
        if target_field in mapping['expression_logic']:
            expr_list = mapping['expression_logic'][target_field]
            expr_logic = ' | '.join([f"{e['transformation']}: {e['expression']}" for e in expr_list])
        else:
            normalized = self.normalize_field_name(target_field)
            if normalized in mapping['expression_logic']:
                expr_list = mapping['expression_logic'][normalized]
                expr_logic = ' | '.join([f"{e['transformation']}: {e['expression']}" for e in expr_list])
        
        # Determine INSERT values
        insert_table = insert_lineage['source_table'] if insert_lineage else ''
        insert_column = insert_lineage['source_field'] if insert_lineage else ''
        insert_transformations = ' <- '.join(reversed(insert_lineage['transformations'])) if insert_lineage else ''
        
        # ENHANCEMENT: For derived fields with in_COLUMN_NAME, trace to parent source
        if insert_column and (insert_column.startswith('in_') or 'Calculation from:' in insert_column):
            if backward_conn:
                # Extract the field names and trace them
                if insert_column.startswith('in_'):
                    parent_info = self.trace_derived_field_parent(insert_column, mapping_name, backward_conn)
                    if parent_info != insert_column:
                        insert_column = f"{insert_column} <- {parent_info}"
                elif 'Calculation from:' in insert_column:
                    # Parse the calculation fields and trace each
                    calc_fields = re.findall(r'in_[A-Za-z][A-Za-z0-9_]*', insert_column)
                    traced_fields = []
                    for cf in calc_fields[:2]:  # Trace first 2 fields
                        parent_info = self.trace_derived_field_parent(cf, mapping_name, backward_conn)
                        traced_fields.append(parent_info)
                    if traced_fields:
                        insert_column = f"Derived: {' / '.join(traced_fields)}"
        
        # Determine UPDATE values
        update_table = update_lineage['source_table'] if update_lineage else ''
        update_column = update_lineage['source_field'] if update_lineage else ''
        update_transformations = ' <- '.join(reversed(update_lineage['transformations'])) if update_lineage else ''
        
        # SPECIAL HANDLING: For fields that exist in BOTH HDR and DTL sources,
        # apply business logic: INSERT uses HDR, UPDATE uses DTL
        dual_source_fields = mapping.get('dual_source_fields', set())
        hdr_fields = mapping.get('hdr_fields', {})
        dtl_fields = mapping.get('dtl_fields', {})
        
        # Check if this target field corresponds to a dual-source field
        base_field = re.sub(r'\d+$', '', target_field)
        source_field_to_check = insert_column if insert_column else update_column
        base_source_field = re.sub(r'\d+$', '', source_field_to_check) if source_field_to_check else ''
        
        if base_source_field in dual_source_fields:
            if base_source_field in hdr_fields:
                hdr_info = hdr_fields[base_source_field]
                insert_table = hdr_info['source_table']
                insert_column = hdr_info['source_field']
            
            if base_source_field in dtl_fields:
                dtl_info = dtl_fields[base_source_field]
                update_table = dtl_info['source_table']
                update_column = dtl_info['source_field']
        
        # ENHANCEMENT: Blank out UPDATE columns if same as INSERT
        if insert_table == update_table and insert_column == update_column:
            update_table = ''
            update_column = ''
        
        if insert_transformations == update_transformations:
            update_transformations = ''
        
        # Get lookup join condition and SQL if this came from a lookup
        lookup_condition = ''
        transformation_sql = ''
        lookup_instance_name = ''
        
        if insert_lineage and insert_lineage.get('transformations'):
            for trans in insert_lineage['transformations']:
                if 'Lookup' in trans:
                    # Extract instance name from transformation string
                    inst_match = re.match(r'([^\s(]+)', trans)
                    if inst_match:
                        lookup_instance_name = inst_match.group(1)
                        lookup_condition = self.get_lookup_join_condition(mapping_name, lookup_instance_name)
                        transformation_sql = self.get_transformation_sql(mapping_name, lookup_instance_name)
                    break
        
        # Also check if source table is from a lookup (even if transformation chain doesn't show it)
        if not lookup_condition and insert_table:
            # Check if insert_table matches any lookup transformation
            for trans_name, trans_info in mapping['transformations'].items():
                if trans_info.get('type') == 'Lookup Procedure' or 'lkp_' in trans_name.lower():
                    lkp_table = self.get_lookup_source_table(trans_name, trans_name)
                    if lkp_table and lkp_table.upper() == insert_table.upper():
                        lookup_condition = self.get_lookup_join_condition(mapping_name, trans_name)
                        transformation_sql = self.get_transformation_sql(mapping_name, trans_name)
                        break
        
        # If no lookup SQL, check for Source Qualifier SQL (applies to all fields from SQ)
        if not transformation_sql:
            sq_sql = self.get_source_qualifier_sql(mapping_name)
            if sq_sql:
                transformation_sql = sq_sql

        # Extract Source Qualifier join and filter conditions
        source_join_condition = ''
        source_filter_condition = ''
        for trans_name, trans_info in mapping['transformations'].items():
            if 'source qualifier' in trans_info.get('type', '').lower():
                if trans_info.get('user_defined_join'):
                    source_join_condition = trans_info['user_defined_join']
                if trans_info.get('source_filter'):
                    source_filter_condition = trans_info['source_filter']
                break

        # Format consolidated columns
        source_datatype = self.format_datatype(
            source_meta.get('datatype', ''),
            source_meta.get('precision', ''),
            source_meta.get('scale', ''),
            source_meta.get('nullable', '')
        )
        
        target_datatype = self.format_datatype(
            target_meta.get('datatype', ''),
            target_meta.get('precision', ''),
            target_meta.get('scale', ''),
            target_meta.get('nullable', '')
        )
        
        source_key = self.format_key_type(source_meta.get('keytype', ''))
        target_key = self.format_key_type(target_meta.get('keytype', ''))
        
        # Get business names for source and target
        source_table_business_name = self.get_table_business_name(insert_table, 'source')
        source_column_business_name = self.get_column_business_name(insert_table, insert_column, 'source')
        target_table_business_name = self.get_table_business_name(target_table, 'target')
        target_column_business_name = self.get_column_business_name(target_table, target_field, 'target')
        
        return {
            'Mapping_Name': mapping_name,
            'Source_Table_INSERT': insert_table,
            'Source_Table_Business_Name': source_table_business_name,
            'Source_Column_INSERT': insert_column,
            'Source_Column_Business_Name': source_column_business_name,
            'Source_Table_UPDATE': update_table,
            'Source_Column_UPDATE': update_column,
            'Source_Datatype': source_datatype,
            'Source_Key': source_key,
            'Target_Table': target_table or '',
            'Target_Table_Business_Name': target_table_business_name,
            'Target_Column': target_field,
            'Target_Column_Business_Name': target_column_business_name,
            'Target_Datatype': target_datatype,
            'Target_Key': target_key,
            'Expression_Logic': expr_logic or '',
            'Lookup_Condition': lookup_condition,
            'SQL_Override': transformation_sql or '',
            'Source_Join_Condition': source_join_condition,
            'Source_Filter': source_filter_condition,
            'Transformations_INSERT': insert_transformations,
            'Transformations_UPDATE': update_transformations
        }
    
    def get_table_business_name(self, table_name, table_type='source'):
        """Get business name for a table. Returns empty string if not found."""
        if not table_name or table_name in ['Derived', 'Hardcoded', 'SYSTEM', 'UNCONNECTED', 'SEQUENCE_GENERATOR', 'Lookup/Expression']:
            return ''
        
        # Check in sources or targets based on type
        if table_type == 'source' and table_name in self.sources:
            # Source tables don't typically have business names at table level in Informatica
            # But we can derive from the table name or return empty
            return self.format_business_name(table_name)
        elif table_type == 'target' and table_name in self.targets:
            return self.format_business_name(table_name)
        
        return ''
    
    def get_column_business_name(self, table_name, column_name, table_type='source'):
        """Get business name for a column from the parsed XML metadata."""
        if not table_name or not column_name:
            return ''
        
        # Skip special source types
        if table_name in ['Derived', 'Hardcoded', 'SYSTEM', 'UNCONNECTED', 'SEQUENCE_GENERATOR', 'Lookup/Expression']:
            return ''
        
        # Clean column name (remove any tracing info like "in_XXX <- YYY")
        clean_column = column_name.split('<-')[0].strip() if '<-' in column_name else column_name
        clean_column = clean_column.split('(')[0].strip() if '(' in clean_column else clean_column
        
        # Look up in sources_data or targets_data
        if table_type == 'source':
            for src in self.sources_data:
                if src.get('Source_Name') == table_name and src.get('Field_Name') == clean_column:
                    business_name = src.get('Business_Name', '')
                    if business_name:
                        return business_name
            # If no business name in XML, derive from column name
            return self.format_business_name(clean_column)
        else:
            for tgt in self.targets_data:
                if tgt.get('Target_Name') == table_name and tgt.get('Field_Name') == clean_column:
                    business_name = tgt.get('Business_Name', '')
                    if business_name:
                        return business_name
            # If no business name in XML, derive from column name
            return self.format_business_name(clean_column)
    
    def format_business_name(self, technical_name):
        """Convert technical name to business-friendly format.
        Example: SAP_ASSET_POSTING_DOC_HDR -> Sap Asset Posting Doc Hdr
        """
        if not technical_name:
            return ''
        
        # Replace underscores with spaces and title case
        words = technical_name.replace('_', ' ').split()
        # Handle common abbreviations
        abbreviations = {'SAP', 'ODS', 'DW', 'ETL', 'ID', 'NO', 'AMT', 'QTY', 'PCT', 'HDR', 'DTL', 'TRX', 'DOC'}
        
        formatted_words = []
        for word in words:
            if word.upper() in abbreviations:
                formatted_words.append(word.upper())
            else:
                formatted_words.append(word.title())
        
        return ' '.join(formatted_words)


