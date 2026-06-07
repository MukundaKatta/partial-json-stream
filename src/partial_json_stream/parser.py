"""Streaming JSON parser with partial snapshots.

Python port of @mukundakatta/streamparse. See the Node.js package for the canonical
reference implementation; this file mirrors the same state machine and lenient repair
rules.

Quick start:

    >>> from partial_json_stream import JsonStreamParser, parse_partial
    >>> p = JsonStreamParser()
    >>> p.push('{"name":"Cl')
    >>> p.snapshot().value
    {'name': 'Cl'}
    >>> p.push('aude","tools":[1,2]}')
    >>> p.end()
    >>> p.snapshot().complete
    True
"""

from __future__ import annotations

import copy
import re
from dataclasses import dataclass
from typing import Any, Callable, Optional


JsonValue = Any  # JSON: None, bool, int/float, str, list, dict
JsonPath = list  # list[str | int]


_HEX = re.compile(r"[0-9a-fA-F]")
_KEY_CHAR = re.compile(r"[A-Za-z_$]")
_KEY_CONT = re.compile(r"[A-Za-z0-9_$\-.]")

_LITERALS: dict[str, JsonValue] = {"true": True, "false": False, "null": None}


@dataclass
class Snapshot:
    """A view of the parser state at a point in time. Always represents valid JSON."""

    value: JsonValue
    complete: bool
    path: list
    bytes_in: int
    confidence: float


@dataclass
class _ObjectFrame:
    obj: dict
    pending_key: Optional[str] = None
    kind: str = "object"


@dataclass
class _ArrayFrame:
    arr: list
    kind: str = "array"


class StreamParseError(ValueError):
    """Raised on syntax errors when no error listener is bound."""


class JsonStreamParser:
    """Single-pass streaming JSON parser.

    Push bytes in via :meth:`push`. Take a :meth:`snapshot` at any time to get a
    valid JSON value reflecting current state with synthetic closure of any open
    strings, arrays, or objects.

    Args:
        lenient: Tolerate LLM-isms like trailing commas, single quotes, unquoted
            keys, ``json`` code fences, comments, and prose padding. Default True.
        max_depth: Maximum nesting depth. Default 256.
        max_string_length: Maximum string length in characters. Default unlimited.
    """

    def __init__(
        self,
        lenient: bool = True,
        max_depth: int = 256,
        max_string_length: Optional[int] = None,
    ):
        self.lenient = lenient
        self.max_depth = max_depth
        self.max_string_length = max_string_length

        self._state = "pre-root"
        self._stack: list = []
        self._root: JsonValue = None
        self._root_set = False

        self._str = ""
        self._str_quote = '"'
        self._num = ""
        self._lit = ""
        self._u_hex = ""
        self._high_surrogate: Optional[int] = None

        self._bytes_in = 0
        self._path: list = []

        self._comment_start = False
        self._saw_asterisk = False
        self._comment_mode = "none"
        self._prose_seen = False

        self._listeners: dict[str, list[Callable]] = {}

    # --- public API ---------------------------------------------------------

    def on(self, event: str, fn: Callable) -> Callable[[], None]:
        """Subscribe to an event. Returns an unsubscribe function.

        Events: ``field``, ``container``, ``partial``, ``complete``, ``error``.
        """
        self._listeners.setdefault(event, []).append(fn)

        def off() -> None:
            self._listeners[event].remove(fn)

        return off

    def push(self, chunk: str) -> None:
        """Feed more bytes into the parser."""
        if self._state == "errored":
            self._bytes_in += len(chunk)
            return
        for ch in chunk:
            self._bytes_in += 1
            try:
                self._step(ch)
            except StreamParseError as e:
                self._fail(str(e))
                return
            if self._state == "errored":
                return
        self._emit_partial()

    def end(self) -> None:
        """Signal end of input."""
        if self._state == "errored":
            return
        if self._state == "number":
            self._commit_number()
        if self._state == "literal":
            self._commit_literal()
        if self._state in ("string", "string-esc", "string-uXXXX"):
            self._commit_open_string()
        if self._state == "unquoted-key":
            self._commit_open_unquoted_key()

        if self._state == "done":
            self._emit_partial()
            return

        if not self.lenient:
            self._fail("unexpected end of input")
            return

        while self._stack:
            top = self._stack[-1]
            if top.kind == "object" and top.pending_key is not None:
                # Synthetic closure: a key whose value never arrived becomes
                # null, mirroring snapshot() semantics rather than dropping it.
                top.obj.setdefault(top.pending_key, None)
                top.pending_key = None
            self._pop_container()

        self._state = "done"
        self._emit_partial()
        if self._root_set:
            self._emit("complete", self._root)

    def reset(self) -> None:
        """Reset to initial state."""
        self.__init__(
            lenient=self.lenient,
            max_depth=self.max_depth,
            max_string_length=self.max_string_length,
        )

    def snapshot(self) -> Snapshot:
        """Snapshot current state as a valid JSON value."""
        return Snapshot(
            value=self._materialize(),
            complete=self._state == "done",
            path=list(self._path),
            bytes_in=self._bytes_in,
            confidence=self._confidence(),
        )

    # --- core state machine -------------------------------------------------

    def _step(self, ch: str) -> None:
        # Comment passthrough first.
        if self._comment_mode == "line":
            if ch == "\n":
                self._comment_mode = "none"
            return
        if self._comment_mode == "block":
            if self._saw_asterisk and ch == "/":
                self._comment_mode = "none"
                self._saw_asterisk = False
            else:
                self._saw_asterisk = ch == "*"
            return

        s = self._state
        if s == "pre-root":
            self._step_pre_root(ch)
        elif s == "value":
            self._step_value(ch)
        elif s == "string":
            self._step_string(ch)
        elif s == "string-esc":
            self._step_string_esc(ch)
        elif s == "string-uXXXX":
            self._step_string_uhex(ch)
        elif s == "number":
            self._step_number(ch)
        elif s == "literal":
            self._step_literal(ch)
        elif s == "key-or-end":
            self._step_key_or_end(ch)
        elif s == "unquoted-key":
            self._step_unquoted_key(ch)
        elif s == "colon":
            self._step_colon(ch)
        elif s == "comma-or-end":
            self._step_comma_or_end(ch)
        elif s == "done":
            self._step_done(ch)
        # errored: no-op

    def _step_done(self, ch: str) -> None:
        if ch in (" ", "\t", "\n", "\r"):
            return
        if self.lenient:
            return
        self._fail(f"unexpected character {ch!r} after end of value")

    def _step_pre_root(self, ch: str) -> None:
        if ch in (" ", "\t", "\n", "\r"):
            return
        if self.lenient:
            if self._try_start_comment(ch):
                return
            if ch == "`":
                self._prose_seen = True
                return
            obvious_opener = ch in ("{", "[", '"', "'")
            if self._prose_seen:
                if not obvious_opener:
                    return
            else:
                if not _is_value_start(ch) and not obvious_opener:
                    self._prose_seen = True
                    return
        self._state = "value"
        self._step_value(ch)

    def _step_value(self, ch: str) -> None:
        if ch in (" ", "\t", "\n", "\r"):
            return
        if self.lenient and self._try_start_comment(ch):
            return
        if ch == "{":
            self._push_container(_ObjectFrame(obj={}))
            self._state = "key-or-end"
            return
        if ch == "[":
            self._push_container(_ArrayFrame(arr=[]))
            self._state = "value"
            return
        if ch == '"' or (self.lenient and ch == "'"):
            self._str = ""
            self._str_quote = ch
            self._state = "string"
            return
        if ch == "-" or ch.isdigit():
            self._num = ch
            self._state = "number"
            return
        if ch in ("t", "f", "n"):
            self._lit = ch
            self._state = "literal"
            return
        if ch == "]" and self._top_is_array():
            if not self.lenient and len(self._top_array()) > 0:
                self._fail("unexpected ']' (trailing comma not allowed in strict mode)")
                return
            self._pop_container()
            self._after_value_commit()
            return
        self._fail(f"unexpected character {ch!r} when expecting a value")

    def _step_string(self, ch: str) -> None:
        if ch == "\\":
            self._state = "string-esc"
            return
        if ch == self._str_quote:
            s = self._str
            self._str = ""
            self._commit_value(s)
            return
        if not self.lenient and ord(ch) < 0x20:
            self._fail(f"unescaped control character in string: U+{ord(ch):04X}")
            return
        if (
            self.max_string_length is not None
            and len(self._str) >= self.max_string_length
        ):
            self._fail("string exceeds max_string_length")
            return
        self._str += ch

    def _step_string_esc(self, ch: str) -> None:
        if ch in ('"', "'", "\\", "/"):
            self._str += ch
            self._state = "string"
            return
        mapping = {"b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t"}
        if ch in mapping:
            self._str += mapping[ch]
            self._state = "string"
            return
        if ch == "u":
            self._u_hex = ""
            self._state = "string-uXXXX"
            return
        if self.lenient:
            self._str += ch
            self._state = "string"
            return
        self._fail(f"bad string escape \\{ch}")

    def _step_string_uhex(self, ch: str) -> None:
        if not _HEX.match(ch):
            self._fail(f"bad unicode escape, expected hex digit, got {ch!r}")
            return
        self._u_hex += ch
        if len(self._u_hex) == 4:
            code = int(self._u_hex, 16)
            self._u_hex = ""
            if 0xD800 <= code <= 0xDBFF:
                self._high_surrogate = code
            elif 0xDC00 <= code <= 0xDFFF:
                if self._high_surrogate is not None:
                    combined = (
                        ((self._high_surrogate - 0xD800) << 10)
                        + (code - 0xDC00)
                        + 0x10000
                    )
                    self._str += chr(combined)
                    self._high_surrogate = None
                else:
                    if self.lenient:
                        self._str += "�"
                    else:
                        self._fail("lone low surrogate")
            else:
                self._str += chr(code)
                self._high_surrogate = None
            self._state = "string"

    def _step_number(self, ch: str) -> None:
        if ch.isdigit() or ch in (".", "e", "E", "+", "-"):
            self._num += ch
            return
        self._commit_number()
        self._step(ch)

    def _step_literal(self, ch: str) -> None:
        self._lit += ch
        if self._lit in ("true", "false", "null"):
            self._commit_literal()
            return
        if not any(lit.startswith(self._lit) for lit in ("true", "false", "null")):
            self._fail(f"unknown literal: {self._lit!r}")

    def _step_key_or_end(self, ch: str) -> None:
        if ch in (" ", "\t", "\n", "\r"):
            return
        if self.lenient and self._try_start_comment(ch):
            return
        if ch == "}":
            top = self._stack[-1] if self._stack else None
            if not self.lenient and top and top.kind == "object" and len(top.obj) > 0:
                self._fail("unexpected '}' (trailing comma not allowed in strict mode)")
                return
            self._pop_container()
            self._after_value_commit()
            return
        if ch == '"' or (self.lenient and ch == "'"):
            self._str = ""
            self._str_quote = ch
            self._state = "string"
            return
        if self.lenient and _KEY_CHAR.match(ch):
            self._str = ch
            self._state = "unquoted-key"
            return
        self._fail(f"unexpected character {ch!r} when expecting object key")

    def _step_unquoted_key(self, ch: str) -> None:
        if ch in (" ", "\t", "\n", "\r", ":", ",", "}"):
            top = self._stack[-1] if self._stack else None
            if not top or top.kind != "object":
                self._fail("internal: unquoted key without object frame")
                return
            top.pending_key = self._str
            self._path.append(self._str)
            self._str = ""
            self._state = "colon"
            self._step(ch)
            return
        if _KEY_CONT.match(ch):
            self._str += ch
            return
        self._fail(f"unexpected character {ch!r} in unquoted key")

    def _step_colon(self, ch: str) -> None:
        if ch in (" ", "\t", "\n", "\r"):
            return
        if self.lenient and self._try_start_comment(ch):
            return
        if ch == ":":
            self._state = "value"
            return
        self._fail(f"expected ':' after object key, got {ch!r}")

    def _step_comma_or_end(self, ch: str) -> None:
        if ch in (" ", "\t", "\n", "\r"):
            return
        if self.lenient and self._try_start_comment(ch):
            return
        top = self._stack[-1] if self._stack else None
        if top is None:
            if self.lenient:
                return
            self._fail(f"unexpected character {ch!r} after end of value")
            return
        if ch == ",":
            if top.kind == "object":
                self._path.pop()
                self._state = "key-or-end"
            else:
                self._path.pop()
                self._state = "value"
            return
        if ch == "}" and top.kind == "object":
            self._path.pop()
            self._pop_container()
            self._after_value_commit()
            return
        if ch == "]" and top.kind == "array":
            self._path.pop()
            self._pop_container()
            self._after_value_commit()
            return
        if self.lenient and ch in ("}", "]"):
            self._path.pop()
            while self._stack:
                t = self._stack[-1]
                if (ch == "}" and t.kind == "object") or (
                    ch == "]" and t.kind == "array"
                ):
                    self._pop_container()
                    self._after_value_commit()
                    return
                self._pop_container()
        self._fail(f"unexpected character {ch!r} when expecting ',' or close")

    # --- commits / containers -----------------------------------------------

    def _commit_value(self, value: JsonValue) -> None:
        top = self._stack[-1] if self._stack else None
        if (
            top
            and top.kind == "object"
            and self._state == "string"
            and top.pending_key is None
        ):
            top.pending_key = str(value)
            self._path.append(top.pending_key)
            self._state = "colon"
            return
        if top is None:
            self._root = value
            self._root_set = True
            self._state = "done"
            self._emit("field", list(self._path), value)
            self._emit("complete", value)
            return
        if top.kind == "array":
            idx = len(top.arr)
            top.arr.append(value)
            self._path.append(idx)
            self._emit("field", list(self._path), value)
            self._state = "comma-or-end"
            return
        if top.pending_key is None:
            self._fail("internal: object value without key")
            return
        top.obj[top.pending_key] = value
        self._emit("field", list(self._path), value)
        top.pending_key = None
        self._state = "comma-or-end"

    def _commit_number(self) -> None:
        text = self._num
        self._num = ""
        if text in ("", "-", "+", "."):
            self._fail(f"malformed number: {text!r}")
            return
        try:
            if any(c in text for c in (".", "e", "E")):
                n = float(text)
            else:
                n = int(text)
        except ValueError:
            self._fail(f"malformed number: {text!r}")
            return
        self._commit_value(n)

    def _commit_literal(self) -> None:
        lit = self._lit
        self._lit = ""
        if lit not in _LITERALS:
            self._fail(f"unknown literal: {lit!r}")
            return
        self._commit_value(_LITERALS[lit])

    def _commit_open_string(self) -> None:
        # Close a string that is still in progress at end of input. Mirrors the
        # synthetic closure that snapshot() already performs, so the final value
        # keeps the partial string instead of discarding it.
        s = self._str
        self._str = ""
        self._high_surrogate = None
        self._commit_value(s)

    def _commit_open_unquoted_key(self) -> None:
        # Close an unquoted key still being read at end of input. The value never
        # arrives, so the key is later filled with null by end()'s closure loop.
        top = self._stack[-1] if self._stack else None
        if top is None or top.kind != "object":
            self._str = ""
            return
        top.pending_key = self._str
        self._path.append(self._str)
        self._str = ""
        self._state = "colon"

    def _push_container(self, c) -> None:
        if len(self._stack) >= self.max_depth:
            self._fail(f"max_depth {self.max_depth} exceeded")
            return
        placeholder = c.arr if c.kind == "array" else c.obj
        parent = self._stack[-1] if self._stack else None
        if parent is None:
            if not self._root_set:
                self._root = placeholder
                self._root_set = True
        elif parent.kind == "array":
            idx = len(parent.arr)
            parent.arr.append(placeholder)
            self._path.append(idx)
        else:
            if parent.pending_key is None:
                self._fail("internal: container under object without key")
                return
            parent.obj[parent.pending_key] = placeholder
            parent.pending_key = None
        self._stack.append(c)

    def _pop_container(self) -> None:
        if not self._stack:
            return
        popped = self._stack.pop()
        value = popped.arr if popped.kind == "array" else popped.obj
        self._emit("container", list(self._path), value)

    def _after_value_commit(self) -> None:
        if not self._stack:
            self._state = "done"
            if self._root_set:
                self._emit("complete", self._root)
            return
        self._state = "comma-or-end"

    # --- helpers ------------------------------------------------------------

    def _top_is_array(self) -> bool:
        return bool(self._stack) and self._stack[-1].kind == "array"

    def _top_array(self) -> list:
        return self._stack[-1].arr

    def _try_start_comment(self, ch: str) -> bool:
        if self._comment_start:
            self._comment_start = False
            if ch == "/":
                self._comment_mode = "line"
                return True
            if ch == "*":
                self._comment_mode = "block"
                return True
            return True
        if ch == "/":
            self._comment_start = True
            return True
        return False

    def _fail(self, message: str) -> None:
        self._state = "errored"
        err = StreamParseError(f"streamparse: {message} (at byte {self._bytes_in})")
        listeners = self._listeners.get("error", [])
        if listeners:
            for fn in listeners:
                fn(err)
            return
        raise err

    def _emit(self, event: str, *args) -> None:
        for fn in self._listeners.get(event, []):
            fn(*args)

    def _emit_partial(self) -> None:
        if "partial" in self._listeners:
            snap = self.snapshot()
            for fn in self._listeners["partial"]:
                fn(snap)

    # --- snapshot materialization ------------------------------------------

    def _materialize(self) -> JsonValue:
        if not self._root_set:
            if self._state == "string":
                return self._str
            if self._state == "number":
                try:
                    return (
                        int(self._num)
                        if "." not in self._num and "e" not in self._num.lower()
                        else float(self._num)
                    )
                except ValueError:
                    return None
            if self._state == "literal":
                return _LITERALS.get(self._lit)
            return None

        cloned = copy.deepcopy(self._root)

        s = self._state
        if s in ("string", "string-esc", "string-uXXXX"):
            top = self._stack[-1] if self._stack else None
            if top is None:
                return self._str
            if top.kind == "object" and top.pending_key is None:
                node = _walk(cloned, self._container_path())
                if isinstance(node, dict):
                    node[self._str] = None
            elif top.kind == "object" and top.pending_key is not None:
                node = _walk(cloned, self._container_path())
                if isinstance(node, dict):
                    node[top.pending_key] = self._str
            elif top.kind == "array":
                node = _walk(cloned, self._container_path())
                if isinstance(node, list):
                    node.append(self._str)
        elif s == "unquoted-key":
            top = self._stack[-1] if self._stack else None
            if top and top.kind == "object":
                node = _walk(cloned, self._container_path())
                if isinstance(node, dict):
                    node[self._str] = None
        elif s in ("colon", "value"):
            top = self._stack[-1] if self._stack else None
            if top and top.kind == "object" and top.pending_key is not None:
                node = _walk(cloned, self._path[:-1])
                if isinstance(node, dict) and top.pending_key not in node:
                    node[top.pending_key] = None
        elif s == "number":
            try:
                v = (
                    int(self._num)
                    if "." not in self._num and "e" not in self._num.lower()
                    else float(self._num)
                )
            except ValueError:
                v = None
            self._patch_scratch(cloned, v)
        elif s == "literal":
            self._patch_scratch(cloned, _LITERALS.get(self._lit))

        return cloned

    def _container_path(self) -> list:
        top = self._stack[-1] if self._stack else None
        if not top:
            return []
        if top.kind == "object":
            if top.pending_key is None:
                return list(self._path)
            return self._path[:-1]
        return list(self._path)

    def _patch_scratch(self, cloned: JsonValue, value: JsonValue) -> None:
        top = self._stack[-1] if self._stack else None
        if top is None:
            return
        parent = _walk(cloned, self._container_path())
        if parent is None:
            return
        if isinstance(parent, list):
            parent.append(value)
        elif top.kind == "object" and top.pending_key is not None:
            parent[top.pending_key] = value

    def _confidence(self) -> float:
        if self._state == "done":
            return 1.0
        c = 1.0
        c -= min(0.5, len(self._stack) * 0.05)
        if self._state in ("string", "string-esc", "string-uXXXX"):
            c -= 0.2
        elif self._state == "number":
            c -= 0.1
        elif self._state == "literal":
            c -= 0.15
        return round(max(0.0, c), 3)


# --- module helpers --------------------------------------------------------


def parse_partial(text: str, lenient: bool = True) -> JsonValue:
    """Parse a possibly-truncated JSON string. Returns the best valid value.

    Returns ``None`` if input is empty. In lenient mode never raises.
    """
    p = JsonStreamParser(lenient=lenient)
    try:
        p.push(text)
    except StreamParseError:
        pass
    return p.snapshot().value


def _is_value_start(ch: str) -> bool:
    return ch in ("{", "[", '"', "-", "t", "f", "n") or ch.isdigit()


def _walk(root: JsonValue, path: list) -> JsonValue:
    cur = root
    for seg in path:
        if cur is None:
            return None
        if isinstance(seg, int):
            if not isinstance(cur, list):
                return None
            if seg >= len(cur):
                return None
            cur = cur[seg]
        else:
            if not isinstance(cur, dict):
                return None
            if seg not in cur:
                return None
            cur = cur[seg]
    return cur
