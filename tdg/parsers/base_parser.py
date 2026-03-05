"""
Base Parser — Technical Document Generator (TDG)

Abstract base class that every parser must inherit from.
Enforces a standard interface and output contract so all parsers
produce the same 22-column lineage record format consumed by generators.
"""

import os
from abc import ABC, abstractmethod


class BaseParser(ABC):
    """
    Abstract base class for all TDG file parsers.

    Subclasses handle a specific file type (XML, SQL, DTSX, PY, etc.)
    and must populate the standard output attributes after parse_all().

    Standard output attributes (mirrors InformaticaLineageParser):
        sources   — {table_name: {name, fields: {col: {datatype, ...}}}}
        targets   — same structure as sources
        mappings  — {mapping_name: {name, instances, transformations, connectors, ...}}
        sources_data       — list of dicts (rows for Sources DataFrame)
        targets_data       — list of dicts (rows for Targets DataFrame)
        transformations_data — list of dicts
        instances_data     — list of dicts
        connectors_data    — list of dicts

    build_lineage() must return a list of 22-column lineage dicts.
    """

    def __init__(self, file_content, filename: str = "unknown"):
        if isinstance(file_content, bytes):
            file_content = file_content.decode("utf-8", errors="replace")
        self.file_content = file_content
        self.filename = filename
        self.mapping_name = os.path.splitext(os.path.basename(filename))[0]

        # Standard output attributes — all parsers must populate these
        self.sources: dict = {}
        self.targets: dict = {}
        self.mappings: dict = {}
        self.sources_data: list = []
        self.targets_data: list = []
        self.transformations_data: list = []
        self.instances_data: list = []
        self.connectors_data: list = []

    @abstractmethod
    def parse_all(self) -> None:
        """Parse file content and populate sources, targets, mappings."""
        ...

    @abstractmethod
    def build_lineage(self, **kwargs) -> list:
        """
        Build field-level lineage records from parsed data.

        Returns:
            list of dicts — each dict has the 22 standard lineage columns:
            Mapping_Name, Source_Table_INSERT, Source_Column_INSERT,
            Source_Table_Business_Name, Source_Column_Business_Name,
            Source_Table_UPDATE, Source_Column_UPDATE,
            Source_Datatype, Source_Key,
            Target_Table, Target_Column,
            Target_Table_Business_Name, Target_Column_Business_Name,
            Target_Datatype, Target_Key,
            Expression_Logic, Lookup_Condition, SQL_Override,
            Source_Join_Condition, Source_Filter,
            Transformations_INSERT, Transformations_UPDATE
        """
        ...

    @classmethod
    def can_parse(cls, filename: str, file_content: bytes = None) -> bool:
        """
        Optional content-based detection beyond file extension.

        The ParserRegistry calls this after matching by extension.
        Override in subclasses that need to distinguish between files
        sharing the same extension (e.g. Informatica mapping vs workflow XML).

        Returns:
            True if this parser can handle the given file.
        """
        return True
