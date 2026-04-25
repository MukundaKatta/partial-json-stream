"""partial-json-stream: streaming JSON parser with partial valid trees.

Python port of `@mukundakatta/streamparse`. Same algorithm, same API shape,
same lenient repairs.
"""

from .parser import JsonStreamParser, Snapshot, parse_partial

__all__ = ["JsonStreamParser", "Snapshot", "parse_partial"]
__version__ = "1.0.0"
