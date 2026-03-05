"""
Databricks Notebook Parser — Technical Document Generator (TDG)

Parses Databricks notebooks in three formats:
  - .ipynb  : Jupyter notebook JSON
  - .py     : Databricks Python export (# MAGIC markers)
  - .dbc    : Databricks archive (ZIP of JSON notebooks)

Extracts:
  - Markdown cells → notebook_context (pre-fills TDD business context)
  - SQL cells      → column-level lineage via SQLLineageParser / sqlglot
  - Python cells   → table-level lineage via PySpark pattern matching
  - cell_sequence  → ordered cell list for Notebook Overview tab
"""

import io
import json
import os
import re
import zipfile
from typing import List, Dict, Optional

from parsers.base_parser import BaseParser

# Regex patterns for PySpark table references
_SRC_PATTERNS = [
    re.compile(r'spark\.read\.table\(\s*["\']([^"\']+)["\']', re.IGNORECASE),
    re.compile(r'spark\.table\(\s*["\']([^"\']+)["\']', re.IGNORECASE),
    re.compile(r'spark\.read\.(?:format\([^)]+\)\.)?load\(\s*["\']([^"\']+)["\']', re.IGNORECASE),
    re.compile(r'\.read\(\s*["\']([^"\']+)["\']', re.IGNORECASE),
]
_TGT_PATTERNS = [
    re.compile(r'\.write\.saveAsTable\(\s*["\']([^"\']+)["\']', re.IGNORECASE),
    re.compile(r'\.write\.insertInto\(\s*["\']([^"\']+)["\']', re.IGNORECASE),
    re.compile(r'\.write\.(?:format\([^)]+\)\.)?save\(\s*["\']([^"\']+)["\']', re.IGNORECASE),
]
_INLINE_SQL = re.compile(
    r'spark\.sql\(\s*(?:f?"""(.*?)"""|f?\'\'\'(.*?)\'\'\'|f?"([^"]+)"|f?\'([^\']+)\')\s*\)',
    re.DOTALL | re.IGNORECASE,
)


def _strip_magic_prefix(line: str) -> str:
    """Remove leading '# MAGIC ' prefix from a Databricks .py export line."""
    return re.sub(r'^#\s*MAGIC\s?', '', line)


def _extract_title(md_text: str) -> str:
    """Extract first heading from markdown text, or return empty string."""
    for line in md_text.splitlines():
        line = line.strip()
        if line.startswith('#'):
            return line.lstrip('#').strip()
    return ''


class DatabricksNotebookParser(BaseParser):
    """
    Parser for Databricks notebooks (.ipynb, .py, .dbc).

    In addition to the standard BaseParser attributes, this parser populates:
      notebook_context  : str  — all markdown joined as plain text (for TDD context)
      markdown_sections : list — [{title, content}]
      sql_cells         : list — [sql_string, ...]
      python_cells      : list — [python_string, ...]
      cell_sequence     : list — [{type:'md'|'sql'|'python', content, title}]
      notebooks         : list — notebook names (multiple for .dbc)
    """

    def __init__(self, file_content, filename: str = 'unknown'):
        # BaseParser handles bytes→str, sets self.file_content and self.filename
        # For .dbc we need the raw bytes — keep a reference before decoding
        self._raw_bytes = file_content if isinstance(file_content, bytes) else file_content.encode('utf-8')
        super().__init__(file_content, filename)

        self.notebook_context: str = ''
        self.markdown_sections: List[Dict] = []
        self.sql_cells: List[str] = []
        self.python_cells: List[str] = []
        self.cell_sequence: List[Dict] = []
        self.notebooks: List[str] = []

    @classmethod
    def can_parse(cls, filename: str, file_content: bytes = None) -> bool:
        ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
        if ext in ('ipynb', 'dbc'):
            return True
        if ext == 'py' and file_content:
            sample = file_content[:3000].decode('utf-8', errors='replace')
            return ('# Databricks notebook source' in sample or '# MAGIC' in sample)
        return False

    # ------------------------------------------------------------------
    # BaseParser contract
    # ------------------------------------------------------------------

    def parse_all(self) -> None:
        ext = self.filename.rsplit('.', 1)[-1].lower() if '.' in self.filename else ''
        raw_cells: List[Dict] = []

        if ext == 'ipynb':
            raw_cells = self._parse_ipynb()
        elif ext == 'dbc':
            raw_cells = self._parse_dbc()
        elif ext == 'py':
            raw_cells = self._parse_py()

        self._process_cells(raw_cells)
        self._build_sources_targets()

    def build_lineage(self, **kwargs) -> list:
        records = []

        # SQL cells → reuse SQLLineageParser for column-level lineage
        try:
            from parsers.sql_parser import SQLLineageParser
            for i, sql in enumerate(self.sql_cells):
                if not sql.strip():
                    continue
                cell_name = f"{self.mapping_name}_sql_cell_{i + 1}"
                sub = SQLLineageParser(sql.encode('utf-8'), filename=cell_name + '.sql')
                sub.parse_all()
                cell_records = sub.build_lineage()
                records.extend(cell_records)
        except Exception:
            pass  # degrade gracefully — SQL lineage optional

        # Python cells → table-level lineage via regex
        for i, py_code in enumerate(self.python_cells):
            cell_records = self._extract_pyspark_lineage(py_code, i)
            records.extend(cell_records)

        return records

    # ------------------------------------------------------------------
    # Format-specific parsers — return list of {type, content} dicts
    # ------------------------------------------------------------------

    def _parse_ipynb(self) -> List[Dict]:
        """Parse Jupyter .ipynb JSON format."""
        try:
            nb = json.loads(self.file_content)
        except json.JSONDecodeError:
            return []

        self.notebooks.append(self.mapping_name)
        raw_cells = []

        for cell in nb.get('cells', []):
            source = ''.join(cell.get('source', []))
            cell_type = cell.get('cell_type', 'code')

            if cell_type == 'markdown':
                raw_cells.append({'type': 'md', 'content': source})
            elif cell_type == 'code':
                first_line = source.lstrip().split('\n')[0].strip()
                if first_line.startswith('%sql'):
                    raw_cells.append({'type': 'sql', 'content': source.lstrip()[len(first_line):].strip()})
                elif first_line.startswith('%md'):
                    raw_cells.append({'type': 'md', 'content': source.lstrip()[len(first_line):].strip()})
                elif first_line.startswith('%'):
                    # Other magic (%sh, %scala, %r) — store as python (passthrough)
                    raw_cells.append({'type': 'python', 'content': source})
                else:
                    raw_cells.append({'type': 'python', 'content': source})

        return raw_cells

    def _parse_py(self) -> List[Dict]:
        """Parse Databricks Python export (.py with # MAGIC comments)."""
        self.notebooks.append(self.mapping_name)
        text = self.file_content
        raw_cells = []

        # Split on COMMAND separator
        command_sep = re.compile(r'#\s*COMMAND\s*-{5,}')
        blocks = command_sep.split(text)

        for block in blocks:
            lines = block.splitlines()
            # Detect block type by first non-empty line
            magic_lines = [l for l in lines if l.strip().startswith('# MAGIC')]
            plain_lines = [l for l in lines if not l.strip().startswith('# MAGIC') and l.strip()
                           and not l.strip().startswith('# Databricks notebook source')]

            if magic_lines:
                first_magic = magic_lines[0]
                # Determine magic type
                magic_cmd = re.match(r'#\s*MAGIC\s+(%\w+)', first_magic)
                magic_type = magic_cmd.group(1).lower() if magic_cmd else ''

                stripped = '\n'.join(_strip_magic_prefix(l) for l in lines
                                     if l.strip().startswith('# MAGIC'))

                if magic_type == '%md':
                    # Remove the %md marker line itself
                    stripped = re.sub(r'^%md\s*', '', stripped, flags=re.MULTILINE).strip()
                    raw_cells.append({'type': 'md', 'content': stripped})
                elif magic_type == '%sql':
                    stripped = re.sub(r'^%sql\s*', '', stripped, flags=re.MULTILINE).strip()
                    raw_cells.append({'type': 'sql', 'content': stripped})
                else:
                    raw_cells.append({'type': 'python', 'content': stripped})
            elif plain_lines:
                raw_cells.append({'type': 'python', 'content': block.strip()})

        return raw_cells

    def _parse_dbc(self) -> List[Dict]:
        """Parse Databricks .dbc archive (ZIP of JSON notebooks)."""
        raw_cells = []

        try:
            buf = io.BytesIO(self._raw_bytes)
            with zipfile.ZipFile(buf, 'r') as zf:
                for name in zf.namelist():
                    if name.endswith('/'):
                        continue  # skip directories
                    try:
                        data = zf.read(name)
                        nb = json.loads(data.decode('utf-8', errors='replace'))
                    except Exception:
                        continue  # skip non-JSON entries

                    if not isinstance(nb, dict) or 'commands' not in nb:
                        continue

                    nb_name = nb.get('name', os.path.basename(name))
                    self.notebooks.append(nb_name)
                    nb_language = nb.get('language', 'PYTHON').lower()

                    for cmd in nb.get('commands', []):
                        command = cmd.get('command', '').strip()
                        if not command:
                            continue

                        first_line = command.split('\n')[0].strip()

                        if first_line.startswith('%md'):
                            body = command[len(first_line):].strip()
                            raw_cells.append({'type': 'md', 'content': body, '_notebook': nb_name})
                        elif first_line.startswith('%sql'):
                            body = command[len(first_line):].strip()
                            raw_cells.append({'type': 'sql', 'content': body, '_notebook': nb_name})
                        elif first_line.startswith('%python'):
                            body = command[len(first_line):].strip()
                            raw_cells.append({'type': 'python', 'content': body, '_notebook': nb_name})
                        elif first_line.startswith('%'):
                            # %sh, %scala, %r etc — store as-is
                            raw_cells.append({'type': 'python', 'content': command, '_notebook': nb_name})
                        else:
                            # Native language cell
                            cell_type = 'sql' if nb_language == 'sql' else 'python'
                            raw_cells.append({'type': cell_type, 'content': command, '_notebook': nb_name})

        except zipfile.BadZipFile:
            pass  # not a valid ZIP

        return raw_cells

    # ------------------------------------------------------------------
    # Cell processing
    # ------------------------------------------------------------------

    def _process_cells(self, raw_cells: List[Dict]) -> None:
        """
        Convert raw cells into structured attributes.
        Also extracts inline spark.sql("...") calls from Python cells
        and treats their content as additional SQL cells.
        """
        md_texts = []

        for cell in raw_cells:
            ctype = cell.get('type', 'python')
            content = cell.get('content', '').strip()
            notebook = cell.get('_notebook', '')

            if not content:
                continue

            if ctype == 'md':
                title = _extract_title(content)
                # Strip markdown heading symbols for plain-text context
                plain = re.sub(r'^#+\s*', '', content, flags=re.MULTILINE).strip()
                self.markdown_sections.append({'title': title, 'content': content})
                self.cell_sequence.append({'type': 'md', 'content': content, 'title': title, 'notebook': notebook})
                md_texts.append(plain)

            elif ctype == 'sql':
                self.sql_cells.append(content)
                self.cell_sequence.append({'type': 'sql', 'content': content, 'title': '', 'notebook': notebook})

            elif ctype == 'python':
                self.python_cells.append(content)
                self.cell_sequence.append({'type': 'python', 'content': content, 'title': '', 'notebook': notebook})

                # Extract inline spark.sql("...") and add as SQL cells
                for m in _INLINE_SQL.finditer(content):
                    inline_sql = next((g for g in m.groups() if g is not None), '')
                    inline_sql = inline_sql.strip()
                    if inline_sql and len(inline_sql) > 10:
                        self.sql_cells.append(inline_sql)

        self.notebook_context = '\n\n'.join(md_texts)

    def _build_sources_targets(self) -> None:
        """Populate self.sources and self.targets from lineage hints for DataFrames."""
        # Collect tables from Python cells
        for py_code in self.python_cells:
            for pat in _SRC_PATTERNS:
                for m in pat.finditer(py_code):
                    table_full = m.group(1)
                    parts = table_full.split('.')
                    tname = parts[-1].upper()
                    if tname not in self.sources:
                        self.sources[tname] = {'name': tname, 'fields': {'*': {
                            'datatype': 'STRING', 'precision': None, 'scale': None,
                            'nullable': 'Y', 'keytype': ''
                        }}}

            for pat in _TGT_PATTERNS:
                for m in pat.finditer(py_code):
                    table_full = m.group(1)
                    parts = table_full.split('.')
                    tname = parts[-1].upper()
                    if tname not in self.targets:
                        self.targets[tname] = {'name': tname, 'fields': {'*': {
                            'datatype': 'STRING', 'precision': None, 'scale': None,
                            'nullable': 'Y', 'keytype': ''
                        }}}

    # ------------------------------------------------------------------
    # PySpark lineage extraction
    # ------------------------------------------------------------------

    def _extract_pyspark_lineage(self, py_code: str, cell_index: int) -> list:
        """
        Extract table-level lineage from a PySpark code cell.
        Returns 22-column lineage records (one per source→target pair).
        """
        src_tables = []
        tgt_tables = []

        for pat in _SRC_PATTERNS:
            for m in pat.finditer(py_code):
                table = m.group(1).strip()
                if table not in src_tables:
                    src_tables.append(table)

        for pat in _TGT_PATTERNS:
            for m in pat.finditer(py_code):
                table = m.group(1).strip()
                if table not in tgt_tables:
                    tgt_tables.append(table)

        if not src_tables and not tgt_tables:
            return []

        records = []
        # Create a cross-product of source→target pairs
        src_list = src_tables or ['Unknown']
        tgt_list = tgt_tables or ['Unknown']

        for src in src_list:
            for tgt in tgt_list:
                src_parts = src.split('.')
                tgt_parts = tgt.split('.')
                src_table = src_parts[-1].upper()
                tgt_table = tgt_parts[-1].upper()
                src_schema = src_parts[-2].upper() if len(src_parts) > 1 else ''
                tgt_schema = tgt_parts[-2].upper() if len(tgt_parts) > 1 else ''

                expr_snippet = py_code.strip()[:300].replace('\n', ' ')

                records.append({
                    'Mapping_Name': f"{self.mapping_name}_py_cell_{cell_index + 1}",
                    'Source_Table_INSERT': src_table,
                    'Source_Table_Business_Name': f"{src_schema}.{src_table}" if src_schema else src_table,
                    'Source_Column_INSERT': '*',
                    'Source_Column_Business_Name': '',
                    'Source_Table_UPDATE': '',
                    'Source_Column_UPDATE': '',
                    'Source_Datatype': 'STRING',
                    'Source_Key': '',
                    'Target_Table': tgt_table,
                    'Target_Table_Business_Name': f"{tgt_schema}.{tgt_table}" if tgt_schema else tgt_table,
                    'Target_Column': '*',
                    'Target_Column_Business_Name': '',
                    'Target_Datatype': 'STRING',
                    'Target_Key': '',
                    'Expression_Logic': expr_snippet,
                    'Lookup_Condition': '',
                    'SQL_Override': '',
                    'Source_Join_Condition': '',
                    'Source_Filter': '',
                    'Transformations_INSERT': 'PySpark',
                    'Transformations_UPDATE': '',
                })

        return records
