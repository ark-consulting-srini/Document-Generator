"""
Parser Registry — Technical Document Generator (TDG)

Maps file extensions to parser classes.
New parsers can be added by calling ParserRegistry.register() without
touching the core application code.

Usage:
    # Register a new parser
    ParserRegistry.register(['py', 'ipynb'], PySparkParser)

    # Resolve a parser for an uploaded file
    parser_class = ParserRegistry.get_parser('my_etl.py', file_bytes)
    if parser_class:
        parser = parser_class(file_bytes, filename='my_etl.py')
        parser.parse_all()
"""

from typing import Optional, Type


class ParserRegistry:
    """
    Central registry mapping file extensions to BaseParser subclasses.

    Supports multiple parsers per extension — the first one whose
    can_parse() returns True is used (registration order matters).
    """

    # ext (lowercase, no dot) → [parser_class, ...]
    _registry: dict = {}

    @classmethod
    def register(cls, extensions: list, parser_class: Type) -> None:
        """
        Register a parser class for one or more file extensions.

        Args:
            extensions: List of file extensions, e.g. ['sql', 'SQL'] or ['xml']
            parser_class: A BaseParser subclass to handle these files
        """
        for ext in extensions:
            ext = ext.lower().lstrip(".")
            if ext not in cls._registry:
                cls._registry[ext] = []
            cls._registry[ext].append(parser_class)

    @classmethod
    def get_parser(cls, filename: str, file_content: bytes = None) -> Optional[Type]:
        """
        Return the first registered parser class that can handle the file.

        Args:
            filename:     Original filename (used to extract extension)
            file_content: Raw file bytes (passed to can_parse() for content detection)

        Returns:
            Parser class, or None if no registered parser matches.
        """
        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        candidates = cls._registry.get(ext, [])
        for parser_class in candidates:
            if parser_class.can_parse(filename, file_content):
                return parser_class
        return None

    @classmethod
    def supported_extensions(cls) -> list:
        """Return list of all registered file extensions (lowercase, no dots)."""
        return list(cls._registry.keys())

    @classmethod
    def supported_extensions_for_uploader(cls) -> list:
        """
        Return extensions in both lower and upper case for Streamlit file_uploader type= param.
        e.g. ['xml', 'XML', 'sql', 'SQL']
        """
        exts = []
        for ext in cls._registry.keys():
            exts.append(ext)
            exts.append(ext.upper())
        return exts

    @classmethod
    def reset(cls) -> None:
        """Clear all registrations (mainly useful for testing)."""
        cls._registry = {}
