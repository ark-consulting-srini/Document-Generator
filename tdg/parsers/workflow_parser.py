"""
Informatica Workflow XML Parser for T - TDD Generator

Parses Informatica PowerCenter workflow XML files and extracts:
- Workflow orchestration metadata (name, description, scheduling)
- Session definitions and their mapping references
- Task execution DAG (dependency graph via WORKFLOWLINK)
- Database connections and configuration
- Pre/Post SQL statements
- Command tasks (shell scripts, file operations)
- Email notification configuration
- Error handling and recovery settings
- File source/target configurations
"""

import xml.etree.ElementTree as ET
from collections import defaultdict


class InformaticaWorkflowParser:
    """Parse Informatica workflow XML files and extract orchestration metadata."""

    def __init__(self, xml_content):
        self.tree = ET.ElementTree(ET.fromstring(xml_content))
        self.root = self.tree.getroot()

        # Extracted data
        self.workflows = {}
        self.sessions = {}
        self.reusable_tasks = {}
        self.configs = {}

        # Flattened data for DataFrames
        self.workflow_data = []
        self.session_data = []
        self.task_data = []
        self.link_data = []
        self.connection_data = []
        self.command_data = []

    def parse_all(self, progress_callback=None):
        """Parse all workflow components from XML."""
        if progress_callback:
            progress_callback(0.1, "Parsing session configurations...")
        self._parse_configs()

        if progress_callback:
            progress_callback(0.2, "Parsing reusable tasks...")
        self._parse_reusable_tasks()

        if progress_callback:
            progress_callback(0.4, "Parsing sessions...")
        self._parse_sessions()

        if progress_callback:
            progress_callback(0.6, "Parsing workflows...")
        self._parse_workflows()

        if progress_callback:
            progress_callback(0.8, "Building execution DAG...")
        self._build_dag()

        if progress_callback:
            progress_callback(1.0, "Workflow parsing complete")

    # ------------------------------------------------------------------ #
    #  Config parsing
    # ------------------------------------------------------------------ #
    def _parse_configs(self):
        """Parse CONFIG elements (session configuration objects)."""
        for config in self.root.findall('.//CONFIG'):
            name = config.get('NAME', '')
            attrs = {}
            for attr in config.findall('ATTRIBUTE'):
                attr_name = attr.get('NAME', '')
                attr_value = attr.get('VALUE', '')
                if attr_name and attr_value:
                    attrs[attr_name] = attr_value

            self.configs[name] = {
                'name': name,
                'description': config.get('DESCRIPTION', ''),
                'is_default': config.get('ISDEFAULT', 'NO'),
                'attributes': attrs,
            }

    # ------------------------------------------------------------------ #
    #  Reusable task parsing (email, command, etc.)
    # ------------------------------------------------------------------ #
    def _parse_reusable_tasks(self):
        """Parse TASK elements that are reusable (defined outside WORKFLOW)."""
        for folder in self.root.findall('.//FOLDER'):
            for task in folder.findall('TASK'):
                name = task.get('NAME', '')
                task_type = task.get('TYPE', '')
                reusable = task.get('REUSABLE', 'NO')
                if reusable != 'YES':
                    continue

                attrs = {}
                for attr in task.findall('ATTRIBUTE'):
                    attrs[attr.get('NAME', '')] = attr.get('VALUE', '')

                self.reusable_tasks[name] = {
                    'name': name,
                    'type': task_type,
                    'description': task.get('DESCRIPTION', ''),
                    'attributes': attrs,
                }

    # ------------------------------------------------------------------ #
    #  Session parsing
    # ------------------------------------------------------------------ #
    def _parse_sessions(self):
        """Parse SESSION elements with transformation instances, connections, etc."""
        # Search for SESSION elements anywhere in the document — they can be
        # direct children of FOLDER or nested inside WORKFLOW elements.
        for session in self.root.findall('.//SESSION'):
                sess_name = session.get('NAME', '')
                mapping_name = session.get('MAPPINGNAME', '')

                # Transformation instances within the session
                transformations = []
                for ti in session.findall('SESSTRANSFORMATIONINST'):
                    trans = {
                        'instance_name': ti.get('SINSTANCENAME', ''),
                        'transformation_name': ti.get('TRANSFORMATIONNAME', ''),
                        'transformation_type': ti.get('TRANSFORMATIONTYPE', ''),
                        'pipeline': ti.get('PIPELINE', ''),
                        'stage': ti.get('STAGE', ''),
                        'is_repartition_point': ti.get('ISREPARTITIONPOINT', 'NO'),
                        'partition_type': ti.get('PARTITIONTYPE', ''),
                    }

                    # Attributes on the transformation instance
                    ti_attrs = {}
                    for attr in ti.findall('ATTRIBUTE'):
                        ti_attrs[attr.get('NAME', '')] = attr.get('VALUE', '')
                    trans['attributes'] = ti_attrs

                    # Flat file config
                    ff = ti.find('FLATFILE')
                    if ff is not None:
                        trans['flatfile'] = {
                            'delimited': ff.get('DELIMITED', ''),
                            'delimiters': ff.get('DELIMITERS', ''),
                            'quote_character': ff.get('QUOTE_CHARACTER', ''),
                            'null_character': ff.get('NULL_CHARACTER', ''),
                            'row_delimiter': ff.get('ROWDELIMITER', ''),
                            'skip_rows': ff.get('SKIPROWS', '0'),
                        }

                    transformations.append(trans)

                # Connection references
                connections = []
                for se in session.findall('SESSIONEXTENSION'):
                    se_info = {
                        'name': se.get('NAME', ''),
                        'instance_name': se.get('SINSTANCENAME', ''),
                        'type': se.get('TYPE', ''),
                        'subtype': se.get('SUBTYPE', ''),
                        'transformation_type': se.get('TRANSFORMATIONTYPE', ''),
                    }

                    for cr in se.findall('CONNECTIONREFERENCE'):
                        conn = {
                            'session': sess_name,
                            'extension_name': se.get('NAME', ''),
                            'extension_type': se.get('TYPE', ''),
                            'instance_name': se.get('SINSTANCENAME', ''),
                            'connection_name': cr.get('CONNECTIONNAME', ''),
                            'connection_type': cr.get('CONNECTIONTYPE', ''),
                            'connection_subtype': cr.get('CONNECTIONSUBTYPE', ''),
                        }
                        connections.append(conn)
                        self.connection_data.append(conn)

                    # Extract target load settings
                    se_attrs = {}
                    for attr in se.findall('ATTRIBUTE'):
                        se_attrs[attr.get('NAME', '')] = attr.get('VALUE', '')
                    se_info['attributes'] = se_attrs
                    connections.append(se_info)

                # Session-level attributes
                sess_attrs = {}
                for attr in session.findall('ATTRIBUTE'):
                    sess_attrs[attr.get('NAME', '')] = attr.get('VALUE', '')

                # Email on failure
                failure_email = ''
                for sc in session.findall('SESSIONCOMPONENT'):
                    if sc.get('TYPE', '') == 'Failure Email':
                        failure_email = sc.get('REFOBJECTNAME', '')

                # Pre/Post SQL
                pre_sql_list = []
                post_sql_list = []
                for ti in session.findall('SESSTRANSFORMATIONINST'):
                    for attr in ti.findall('ATTRIBUTE'):
                        if attr.get('NAME') == 'Pre SQL':
                            val = attr.get('VALUE', '')
                            if val:
                                pre_sql_list.append({
                                    'instance': ti.get('SINSTANCENAME', ''),
                                    'sql': val,
                                })
                        elif attr.get('NAME') == 'Post SQL':
                            val = attr.get('VALUE', '')
                            if val:
                                post_sql_list.append({
                                    'instance': ti.get('SINSTANCENAME', ''),
                                    'sql': val,
                                })

                self.sessions[sess_name] = {
                    'name': sess_name,
                    'mapping_name': mapping_name,
                    'description': session.get('DESCRIPTION', ''),
                    'is_valid': session.get('ISVALID', ''),
                    'sort_order': session.get('SORTORDER', ''),
                    'transformations': transformations,
                    'connections': connections,
                    'attributes': sess_attrs,
                    'failure_email': failure_email,
                    'pre_sql': pre_sql_list,
                    'post_sql': post_sql_list,
                }

                # Build flat session record
                self.session_data.append({
                    'Session_Name': sess_name,
                    'Mapping_Name': mapping_name,
                    'Description': session.get('DESCRIPTION', ''),
                    'Is_Valid': session.get('ISVALID', ''),
                    'Treat_Source_Rows_As': sess_attrs.get('Treat source rows as', ''),
                    'Commit_Type': sess_attrs.get('Commit Type', ''),
                    'Commit_Interval': sess_attrs.get('Commit Interval', ''),
                    'Recovery_Strategy': sess_attrs.get('Recovery Strategy', ''),
                    'Parameter_File': sess_attrs.get('Parameter Filename', ''),
                    'Session_Log': sess_attrs.get('Session Log File Name', ''),
                    'Failure_Email': failure_email,
                    'Pre_SQL': '; '.join(p['sql'] for p in pre_sql_list) if pre_sql_list else '',
                    'Post_SQL': '; '.join(p['sql'] for p in post_sql_list) if post_sql_list else '',
                    'Num_Transformations': len(transformations),
                    'Num_Connections': sum(1 for c in connections if 'connection_name' in c),
                })

    # ------------------------------------------------------------------ #
    #  Workflow parsing
    # ------------------------------------------------------------------ #
    def _parse_workflows(self):
        """Parse WORKFLOW elements with tasks, links, and variables."""
        for wf in self.root.findall('.//WORKFLOW'):
            wf_name = wf.get('NAME', '')
            wf_desc = wf.get('DESCRIPTION', '')

            # Scheduler
            scheduler_info = {}
            sched = wf.find('SCHEDULER')
            if sched is not None:
                si = sched.find('SCHEDULEINFO')
                if si is not None:
                    scheduler_info = {
                        'type': si.get('SCHEDULETYPE', 'ONDEMAND'),
                        'start_date': si.get('STARTDATE', ''),
                        'end_date': si.get('ENDDATE', ''),
                        'repeat_interval': si.get('REPEATINTERVAL', ''),
                    }

            # Inline tasks (Start, Command, etc.)
            tasks = {}
            for task in wf.findall('TASK'):
                t_name = task.get('NAME', '')
                t_type = task.get('TYPE', '')

                task_info = {
                    'name': t_name,
                    'type': t_type,
                    'description': task.get('DESCRIPTION', ''),
                    'reusable': task.get('REUSABLE', 'NO'),
                }

                # Command task value pairs
                commands = []
                for vp in task.findall('VALUEPAIR'):
                    cmd_val = vp.get('VALUE', '')
                    if cmd_val:
                        cmd = {
                            'workflow': wf_name,
                            'task': t_name,
                            'exec_order': vp.get('EXECORDER', ''),
                            'command_name': vp.get('NAME', ''),
                            'command': cmd_val,
                        }
                        commands.append(cmd)
                        self.command_data.append(cmd)
                task_info['commands'] = commands

                # Task attributes
                t_attrs = {}
                for attr in task.findall('ATTRIBUTE'):
                    t_attrs[attr.get('NAME', '')] = attr.get('VALUE', '')
                task_info['attributes'] = t_attrs

                tasks[t_name] = task_info

            # Task instances (references to sessions or tasks)
            task_instances = {}
            for ti in wf.findall('TASKINSTANCE'):
                ti_name = ti.get('NAME', '')
                ti_info = {
                    'name': ti_name,
                    'task_name': ti.get('TASKNAME', ''),
                    'task_type': ti.get('TASKTYPE', ''),
                    'is_enabled': ti.get('ISENABLED', 'YES'),
                    'fail_parent': ti.get('FAIL_PARENT_IF_INSTANCE_FAILS', 'NO'),
                    'treat_input_as_and': ti.get('TREAT_INPUTLINK_AS_AND', 'NO'),
                    'description': ti.get('DESCRIPTION', ''),
                    'reusable': ti.get('REUSABLE', 'NO'),
                }

                # Instance-level attribute overrides
                ti_attrs = {}
                for attr in ti.findall('ATTRIBUTE'):
                    ti_attrs[attr.get('NAME', '')] = attr.get('VALUE', '')
                ti_info['attributes'] = ti_attrs

                # Session extension overrides (e.g., source filename)
                overrides = []
                for se in ti.findall('SESSIONEXTENSION'):
                    for attr in se.findall('ATTRIBUTE'):
                        overrides.append({
                            'extension': se.get('NAME', ''),
                            'instance': se.get('SINSTANCENAME', ''),
                            'attribute': attr.get('NAME', ''),
                            'value': attr.get('VALUE', ''),
                        })
                ti_info['overrides'] = overrides

                # Failure email
                for sc in ti.findall('SESSIONCOMPONENT'):
                    if sc.get('TYPE', '') == 'Failure Email':
                        ti_info['failure_email'] = sc.get('REFOBJECTNAME', '')

                task_instances[ti_name] = ti_info

                # Flat record for DataFrame
                self.task_data.append({
                    'Workflow': wf_name,
                    'Task_Instance': ti_name,
                    'Task_Name': ti.get('TASKNAME', ''),
                    'Task_Type': ti.get('TASKTYPE', ''),
                    'Is_Enabled': ti.get('ISENABLED', 'YES'),
                    'Fail_Parent': ti.get('FAIL_PARENT_IF_INSTANCE_FAILS', 'NO'),
                    'Reusable': ti.get('REUSABLE', 'NO'),
                    'Mapping_Name': self.sessions.get(ti.get('TASKNAME', ''), {}).get('mapping_name', ''),
                })

            # Workflow links (execution DAG)
            links = []
            for link in wf.findall('WORKFLOWLINK'):
                l = {
                    'workflow': wf_name,
                    'from_task': link.get('FROMTASK', ''),
                    'to_task': link.get('TOTASK', ''),
                    'condition': link.get('CONDITION', ''),
                }
                links.append(l)
                self.link_data.append(l)

            # Workflow variables (user-defined only)
            user_variables = []
            for wv in wf.findall('WORKFLOWVARIABLE'):
                if wv.get('USERDEFINED', 'NO') == 'YES':
                    user_variables.append({
                        'name': wv.get('NAME', ''),
                        'datatype': wv.get('DATATYPE', ''),
                        'default_value': wv.get('DEFAULTVALUE', ''),
                        'is_persistent': wv.get('ISPERSISTENT', 'NO'),
                        'description': wv.get('DESCRIPTION', ''),
                    })

            # Workflow attributes
            wf_attrs = {}
            for attr in wf.findall('ATTRIBUTE'):
                wf_attrs[attr.get('NAME', '')] = attr.get('VALUE', '')

            self.workflows[wf_name] = {
                'name': wf_name,
                'description': wf_desc,
                'is_valid': wf.get('ISVALID', ''),
                'is_enabled': wf.get('ISENABLED', 'YES'),
                'server': wf.get('SERVERNAME', ''),
                'scheduler': scheduler_info,
                'tasks': tasks,
                'task_instances': task_instances,
                'links': links,
                'user_variables': user_variables,
                'attributes': wf_attrs,
            }

            # Flat workflow record
            self.workflow_data.append({
                'Workflow_Name': wf_name,
                'Description': wf_desc,
                'Is_Valid': wf.get('ISVALID', ''),
                'Is_Enabled': wf.get('ISENABLED', 'YES'),
                'Server': wf.get('SERVERNAME', ''),
                'Schedule_Type': scheduler_info.get('type', 'ONDEMAND'),
                'Num_Sessions': sum(1 for ti in task_instances.values() if ti['task_type'] == 'Session'),
                'Num_Commands': sum(1 for ti in task_instances.values() if ti['task_type'] == 'Command'),
                'Num_Links': len(links),
                'Parameter_File': wf_attrs.get('Parameter Filename', ''),
                'Log_File': wf_attrs.get('Workflow Log File Name', ''),
            })

    # ------------------------------------------------------------------ #
    #  DAG builder
    # ------------------------------------------------------------------ #
    def _build_dag(self):
        """Build execution DAG from workflow links for each workflow."""
        for wf_name, wf in self.workflows.items():
            # adjacency list
            forward = defaultdict(list)
            backward = defaultdict(list)
            for link in wf['links']:
                forward[link['from_task']].append({
                    'to': link['to_task'],
                    'condition': link['condition'],
                })
                backward[link['to_task']].append({
                    'from': link['from_task'],
                    'condition': link['condition'],
                })

            wf['dag_forward'] = dict(forward)
            wf['dag_backward'] = dict(backward)

            # Topological order (BFS-based)
            in_degree = defaultdict(int)
            all_tasks = set()
            for link in wf['links']:
                all_tasks.add(link['from_task'])
                all_tasks.add(link['to_task'])
                in_degree[link['to_task']] += 1

            queue = [t for t in all_tasks if in_degree[t] == 0]
            topo_order = []
            while queue:
                node = queue.pop(0)
                topo_order.append(node)
                for edge in forward.get(node, []):
                    in_degree[edge['to']] -= 1
                    if in_degree[edge['to']] == 0:
                        queue.append(edge['to'])

            wf['execution_order'] = topo_order

    # ------------------------------------------------------------------ #
    #  Summary generation for LLM prompts
    # ------------------------------------------------------------------ #
    def generate_workflow_summary(self):
        """Generate a text summary of all workflows for LLM processing."""
        lines = []

        # Repository / folder context
        repo = self.root.find('.//REPOSITORY')
        if repo is not None:
            lines.append(f"REPOSITORY: {repo.get('NAME', '')}")
        folder = self.root.find('.//FOLDER')
        if folder is not None:
            lines.append(f"FOLDER: {folder.get('NAME', '')} — {folder.get('DESCRIPTION', '')}")
        lines.append('')

        for wf_name, wf in self.workflows.items():
            lines.append(f"{'='*60}")
            lines.append(f"WORKFLOW: {wf_name}")
            lines.append(f"Description: {wf['description']}")
            lines.append(f"Server: {wf['server']}")
            lines.append(f"Schedule: {wf['scheduler'].get('type', 'ONDEMAND')}")
            lines.append(f"Valid: {wf['is_valid']}  |  Enabled: {wf['is_enabled']}")
            lines.append('')

            # Execution order
            lines.append("EXECUTION ORDER:")
            for i, task in enumerate(wf.get('execution_order', []), 1):
                ti = wf['task_instances'].get(task, {})
                task_type = ti.get('task_type', 'Unknown')
                mapping = ''
                if task_type == 'Session':
                    sess = self.sessions.get(ti.get('task_name', ''), {})
                    mapping = f" → mapping: {sess.get('mapping_name', '?')}"
                enabled = '(DISABLED)' if ti.get('is_enabled') == 'NO' else ''
                lines.append(f"  {i}. {task} [{task_type}]{mapping} {enabled}")
            lines.append('')

            # Dependencies (links with conditions)
            lines.append("TASK DEPENDENCIES:")
            for link in wf['links']:
                cond = f" IF {link['condition']}" if link['condition'] else ''
                lines.append(f"  {link['from_task']} → {link['to_task']}{cond}")
            lines.append('')

            # Command tasks
            has_commands = False
            for t_name, t_info in wf['tasks'].items():
                if t_info['type'] == 'Command' and t_info.get('commands'):
                    if not has_commands:
                        lines.append("COMMAND TASKS:")
                        has_commands = True
                    lines.append(f"  {t_name}:")
                    for cmd in t_info['commands']:
                        lines.append(f"    [{cmd.get('exec_order', '')}] {cmd.get('command_name', '')}: {cmd['command']}")
            if has_commands:
                lines.append('')

        # Sessions detail
        lines.append(f"{'='*60}")
        lines.append("SESSION DETAILS:")
        lines.append('')
        for sess_name, sess in self.sessions.items():
            lines.append(f"SESSION: {sess_name}")
            lines.append(f"  Mapping: {sess['mapping_name']}")
            lines.append(f"  Description: {sess['description']}")
            lines.append(f"  Source Rows Treatment: {sess['attributes'].get('Treat source rows as', '')}")
            lines.append(f"  Recovery Strategy: {sess['attributes'].get('Recovery Strategy', '')}")

            if sess['failure_email']:
                email_task = self.reusable_tasks.get(sess['failure_email'], {})
                email_addr = email_task.get('attributes', {}).get('Email User Name', '')
                lines.append(f"  Failure Email: {email_addr}")

            if sess['pre_sql']:
                lines.append(f"  Pre SQL:")
                for ps in sess['pre_sql']:
                    lines.append(f"    [{ps['instance']}] {ps['sql']}")

            if sess['post_sql']:
                lines.append(f"  Post SQL:")
                for ps in sess['post_sql']:
                    lines.append(f"    [{ps['instance']}] {ps['sql']}")

            # Connections
            db_connections = set()
            for conn in self.connection_data:
                if conn['session'] == sess_name and conn['connection_name']:
                    db_connections.add(f"{conn['connection_name']} ({conn['connection_type']}/{conn['connection_subtype']})")
            if db_connections:
                lines.append(f"  Connections: {', '.join(sorted(db_connections))}")

            # Source file configs
            for ti in sess['transformations']:
                if ti.get('flatfile'):
                    ff = ti['flatfile']
                    lines.append(f"  File Source [{ti['instance_name']}]: delimited={ff['delimited']}, delimiter='{ff['delimiters']}', quote={ff['quote_character']}")

            # Lookup tables
            for ti in sess['transformations']:
                if ti['transformation_type'] == 'Lookup Procedure':
                    lkp_table = ti['attributes'].get('Lookup table name', '')
                    lkp_sql = ti['attributes'].get('Lookup Sql Override', '')
                    if lkp_table:
                        lines.append(f"  Lookup [{ti['instance_name']}]: {lkp_table}")
                    if lkp_sql:
                        lines.append(f"    SQL Override: {lkp_sql[:200]}")

            # Target load settings
            for conn_info in sess['connections']:
                if isinstance(conn_info, dict) and conn_info.get('type') == 'WRITER':
                    attrs = conn_info.get('attributes', {})
                    insert = attrs.get('Insert', '')
                    update = attrs.get('Update as Update', '')
                    upsert = attrs.get('Update else Insert', '')
                    truncate = attrs.get('Truncate target table option', '')
                    prefix = attrs.get('Table Name Prefix', '')
                    if any([insert, update, upsert]):
                        inst = conn_info.get('instance_name', '')
                        lines.append(f"  Target [{inst}]: Insert={insert}, Update={update}, Upsert={upsert}, Truncate={truncate}")

            lines.append('')

        return '\n'.join(lines)

    # ------------------------------------------------------------------ #
    #  Utility: detect XML type
    # ------------------------------------------------------------------ #
    @staticmethod
    def is_workflow_xml(xml_content):
        """Check if XML content contains workflow definitions (not just mappings)."""
        try:
            root = ET.fromstring(xml_content)
            return len(root.findall('.//WORKFLOW')) > 0
        except ET.ParseError:
            return False

    @staticmethod
    def get_xml_type(xml_content):
        """Detect the type of Informatica XML: 'workflow', 'mapping', or 'unknown'."""
        try:
            root = ET.fromstring(xml_content)
            has_workflow = len(root.findall('.//WORKFLOW')) > 0
            has_mapping = len(root.findall('.//MAPPING')) > 0

            if has_workflow:
                return 'workflow'
            elif has_mapping:
                return 'mapping'
            else:
                return 'unknown'
        except ET.ParseError:
            return 'unknown'

    def get_referenced_mappings(self):
        """Get list of mapping names referenced by sessions in this workflow."""
        return list(set(
            sess['mapping_name']
            for sess in self.sessions.values()
            if sess['mapping_name']
        ))

    def generate_unified_summary(self, mappings_dict=None, sources_dict=None, targets_dict=None):
        """Generate a workflow summary enriched with mapping details for each session.

        When mappings_dict is provided, each session section is augmented with:
        - Source/target table names and key fields
        - Transformation names and types
        - Key expressions and lookup SQL overrides
        - Load strategy details

        Falls back to generate_workflow_summary() when no mapping data is available.
        """
        if not mappings_dict:
            return self.generate_workflow_summary()

        mappings_dict = mappings_dict or {}
        sources_dict = sources_dict or {}
        targets_dict = targets_dict or {}

        lines = []

        # Repository / folder context
        repo = self.root.find('.//REPOSITORY')
        if repo is not None:
            lines.append(f"REPOSITORY: {repo.get('NAME', '')}")
        folder = self.root.find('.//FOLDER')
        if folder is not None:
            lines.append(f"FOLDER: {folder.get('NAME', '')} — {folder.get('DESCRIPTION', '')}")
        lines.append('')

        for wf_name, wf in self.workflows.items():
            lines.append(f"{'='*60}")
            lines.append(f"WORKFLOW: {wf_name}")
            lines.append(f"Description: {wf['description']}")
            lines.append(f"Server: {wf['server']}")
            lines.append(f"Schedule: {wf['scheduler'].get('type', 'ONDEMAND')}")
            lines.append(f"Valid: {wf['is_valid']}  |  Enabled: {wf['is_enabled']}")
            lines.append('')

            # Execution order
            lines.append("EXECUTION ORDER:")
            for i, task in enumerate(wf.get('execution_order', []), 1):
                ti = wf['task_instances'].get(task, {})
                task_type = ti.get('task_type', 'Unknown')
                mapping = ''
                if task_type == 'Session':
                    sess = self.sessions.get(ti.get('task_name', ''), {})
                    mapping = f" → mapping: {sess.get('mapping_name', '?')}"
                enabled = '(DISABLED)' if ti.get('is_enabled') == 'NO' else ''
                lines.append(f"  {i}. {task} [{task_type}]{mapping} {enabled}")
            lines.append('')

            # Dependencies (links with conditions)
            lines.append("TASK DEPENDENCIES:")
            for link in wf['links']:
                cond = f" IF {link['condition']}" if link['condition'] else ''
                lines.append(f"  {link['from_task']} → {link['to_task']}{cond}")
            lines.append('')

            # Command tasks
            has_commands = False
            for t_name, t_info in wf['tasks'].items():
                if t_info['type'] == 'Command' and t_info.get('commands'):
                    if not has_commands:
                        lines.append("COMMAND TASKS:")
                        has_commands = True
                    lines.append(f"  {t_name}:")
                    for cmd in t_info['commands']:
                        lines.append(f"    [{cmd.get('exec_order', '')}] {cmd.get('command_name', '')}: {cmd['command']}")
            if has_commands:
                lines.append('')

        # Sessions detail with mapping enrichment
        lines.append(f"{'='*60}")
        lines.append("SESSION DETAILS (ENRICHED WITH MAPPING DATA):")
        lines.append('')
        for sess_name, sess in self.sessions.items():
            mapping_name = sess['mapping_name']
            mapping_data = mappings_dict.get(mapping_name)

            lines.append(f"SESSION: {sess_name}")
            lines.append(f"  Mapping: {mapping_name}")
            if not mapping_data:
                lines.append(f"  ⚠ Mapping XML not uploaded — details unavailable")
            lines.append(f"  Description: {sess['description']}")
            lines.append(f"  Source Rows Treatment: {sess['attributes'].get('Treat source rows as', '')}")
            lines.append(f"  Recovery Strategy: {sess['attributes'].get('Recovery Strategy', '')}")

            if sess['failure_email']:
                email_task = self.reusable_tasks.get(sess['failure_email'], {})
                email_addr = email_task.get('attributes', {}).get('Email User Name', '')
                lines.append(f"  Failure Email: {email_addr}")

            if sess['pre_sql']:
                lines.append(f"  Pre SQL:")
                for ps in sess['pre_sql']:
                    lines.append(f"    [{ps['instance']}] {ps['sql']}")

            if sess['post_sql']:
                lines.append(f"  Post SQL:")
                for ps in sess['post_sql']:
                    lines.append(f"    [{ps['instance']}] {ps['sql']}")

            # Connections
            db_connections = set()
            for conn in self.connection_data:
                if conn['session'] == sess_name and conn['connection_name']:
                    db_connections.add(f"{conn['connection_name']} ({conn['connection_type']}/{conn['connection_subtype']})")
            if db_connections:
                lines.append(f"  Connections: {', '.join(sorted(db_connections))}")

            # Source file configs
            for ti in sess['transformations']:
                if ti.get('flatfile'):
                    ff = ti['flatfile']
                    lines.append(f"  File Source [{ti['instance_name']}]: delimited={ff['delimited']}, delimiter='{ff['delimiters']}', quote={ff['quote_character']}")

            # Lookup tables (from session)
            for ti in sess['transformations']:
                if ti['transformation_type'] == 'Lookup Procedure':
                    lkp_table = ti['attributes'].get('Lookup table name', '')
                    lkp_sql = ti['attributes'].get('Lookup Sql Override', '')
                    if lkp_table:
                        lines.append(f"  Lookup [{ti['instance_name']}]: {lkp_table}")
                    if lkp_sql:
                        lines.append(f"    SQL Override: {lkp_sql[:200]}")

            # Target load settings
            for conn_info in sess['connections']:
                if isinstance(conn_info, dict) and conn_info.get('type') == 'WRITER':
                    attrs = conn_info.get('attributes', {})
                    insert = attrs.get('Insert', '')
                    update = attrs.get('Update as Update', '')
                    upsert = attrs.get('Update else Insert', '')
                    truncate = attrs.get('Truncate target table option', '')
                    if any([insert, update, upsert]):
                        inst = conn_info.get('instance_name', '')
                        lines.append(f"  Target [{inst}]: Insert={insert}, Update={update}, Upsert={upsert}, Truncate={truncate}")

            # ---- Enrichment from mapping data ----
            if mapping_data:
                lines.append('')
                lines.append(f"  --- Mapping Detail: {mapping_name} ---")

                # Sources
                src_instances = mapping_data.get('source_instances', {})
                if src_instances:
                    src_tables = sorted(set(src_instances.values()))
                    lines.append(f"  Source Tables: {', '.join(src_tables)}")
                    for src_table in src_tables:
                        src_def = sources_dict.get(src_table, {})
                        if src_def:
                            key_fields = [f for f, info in src_def.get('fields', {}).items()
                                          if info.get('keytype')]
                            if key_fields:
                                lines.append(f"    {src_table} keys: {', '.join(key_fields)}")

                # Targets
                tgt_instances = mapping_data.get('target_instances', {})
                if tgt_instances:
                    tgt_tables = sorted(set(tgt_instances.values()))
                    lines.append(f"  Target Tables: {', '.join(tgt_tables)}")
                    for tgt_table in tgt_tables:
                        tgt_def = targets_dict.get(tgt_table, {})
                        if tgt_def:
                            key_fields = [f for f, info in tgt_def.get('fields', {}).items()
                                          if info.get('keytype')]
                            if key_fields:
                                lines.append(f"    {tgt_table} keys: {', '.join(key_fields)}")

                # Transformations summary by type
                transforms = mapping_data.get('transformations', {})
                if transforms:
                    type_counts = defaultdict(list)
                    for t_name, t_info in transforms.items():
                        type_counts[t_info.get('type', 'Unknown')].append(t_name)

                    lines.append(f"  Transformations ({len(transforms)} total):")
                    for t_type, names in sorted(type_counts.items()):
                        lines.append(f"    {t_type} ({len(names)}): {', '.join(names)}")

                    # Key expressions from Expression transforms
                    for t_name, t_info in transforms.items():
                        if t_info.get('type') in ('Expression', 'expression'):
                            expr_fields = []
                            for f_name, f_info in t_info.get('fields', {}).items():
                                expr = f_info.get('expression', '')
                                if expr and f_info.get('porttype', '') in ('OUTPUT', 'OUTPUT/INPUT'):
                                    expr_fields.append(f"{f_name} = {expr[:150]}")
                            if expr_fields:
                                lines.append(f"  Expression [{t_name}]:")
                                for ef in expr_fields[:15]:
                                    lines.append(f"    {ef}")

                    # Lookup details from mapping
                    for t_name, t_info in transforms.items():
                        if 'lookup' in t_info.get('type', '').lower():
                            lkp_tbl = t_info.get('lookup_table', '')
                            lkp_cond = t_info.get('lookup_condition', '')
                            lkp_sql = t_info.get('lookup_sql', '')
                            if lkp_tbl:
                                lines.append(f"  Lookup [{t_name}]: table={lkp_tbl}")
                            if lkp_cond:
                                lines.append(f"    Condition: {lkp_cond[:200]}")
                            if lkp_sql:
                                lines.append(f"    SQL Override: {lkp_sql[:300]}")

                    # Source Qualifier SQL / filters
                    for t_name, t_info in transforms.items():
                        if 'source qualifier' in t_info.get('type', '').lower():
                            sq_sql = t_info.get('sql_query', '')
                            sq_join = t_info.get('user_defined_join', '')
                            sq_filter = t_info.get('source_filter', '')
                            if sq_sql:
                                lines.append(f"  Source Qualifier [{t_name}] SQL: {sq_sql[:300]}")
                            if sq_join:
                                lines.append(f"    Join: {sq_join[:200]}")
                            if sq_filter:
                                lines.append(f"    Filter: {sq_filter[:200]}")

            lines.append('')

        return '\n'.join(lines)
