"""
Parsers package — Technical Document Generator (TDG)

All built-in parsers are registered here.
To add a new parser, import it and call ParserRegistry.register().
"""

from parsers.registry import ParserRegistry
from parsers.informatica_parser import InformaticaLineageParser
from parsers.sql_parser import SQLLineageParser
from parsers.databricks_notebook_parser import DatabricksNotebookParser

# Register built-in parsers (order matters for same-extension conflicts)
ParserRegistry.register(["xml"], InformaticaLineageParser)       # mapping XMLs only (can_parse filters workflow XMLs)
ParserRegistry.register(["sql"], SQLLineageParser)
ParserRegistry.register(["ipynb", "dbc"], DatabricksNotebookParser)
ParserRegistry.register(["py"], DatabricksNotebookParser)        # can_parse() filters non-Databricks .py files
