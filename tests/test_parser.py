"""Tests for partial_json_stream.parser.

Mirror of the TS test suite. Run with ``pytest``.
"""

from __future__ import annotations

import json

import pytest

from partial_json_stream import JsonStreamParser, parse_partial


VALID_INPUTS = [
    None,
    True,
    False,
    0,
    1,
    -1,
    3.14,
    -2.5e-3,
    "",
    "hello",
    'with "quotes"',
    "with \\backslash",
    "tab\there",
    "unicode ☃ snowman",
    [],
    [1, 2, 3],
    [None, True, False],
    ["a", "b", "c"],
    {},
    {"a": 1},
    {"a": 1, "b": "two", "c": True, "d": None},
    {"nested": {"deep": {"deeper": [1, [2, [3, [4]]]]}}},
    {
        "type": "tool_use",
        "id": "toolu_01abc",
        "name": "edit_file",
        "input": {
            "path": "src/foo.ts",
            "patches": [
                {"line": 10, "op": "insert", "content": 'console.log("hi")'},
                {"line": 20, "op": "delete"},
            ],
        },
    },
]


@pytest.mark.parametrize("v", VALID_INPUTS)
def test_full_parse_matches_json(v):
    text = json.dumps(v)
    p = JsonStreamParser()
    p.push(text)
    p.end()
    snap = p.snapshot()
    assert snap.complete
    assert snap.value == v


def test_chunked_input_every_split():
    v = {
        "type": "tool_use",
        "id": "toolu_01",
        "name": "edit",
        "input": {"path": "a/b.ts", "changes": [1, 2, 3], "on": True, "off": None},
    }
    text = json.dumps(v)
    for split in range(1, len(text)):
        p = JsonStreamParser()
        p.push(text[:split])
        p.push(text[split:])
        p.end()
        assert p.snapshot().value == v


def test_one_byte_at_a_time():
    v = {"items": [{"id": 1, "tags": ["a", "b"]}, {"id": 2, "tags": []}]}
    text = json.dumps(v)
    p = JsonStreamParser()
    for ch in text:
        p.push(ch)
    p.end()
    assert p.snapshot().value == v


def test_partial_snapshot_in_string():
    p = JsonStreamParser()
    p.push('{"name":"Cl')
    snap = p.snapshot()
    assert snap.value == {"name": "Cl"}
    assert not snap.complete


def test_partial_snapshot_in_number():
    p = JsonStreamParser()
    p.push('{"n":12.3')
    snap = p.snapshot()
    assert snap.value == {"n": 12.3}
    assert not snap.complete


def test_partial_snapshot_nested():
    p = JsonStreamParser()
    p.push('{"a":[1,2,{"b":[3,4')
    snap = p.snapshot()
    assert snap.value == {"a": [1, 2, {"b": [3, 4]}]}


def test_partial_snapshot_partial_key():
    p = JsonStreamParser()
    p.push('{"nam')
    assert p.snapshot().value == {"nam": None}


def test_lenient_trailing_comma_object():
    assert parse_partial('{"a":1,"b":2,}') == {"a": 1, "b": 2}


def test_lenient_trailing_comma_array():
    assert parse_partial("[1,2,3,]") == [1, 2, 3]


def test_lenient_single_quotes():
    assert parse_partial("{'a': 'hi'}") == {"a": "hi"}


def test_lenient_unquoted_keys():
    assert parse_partial("{a: 1, b: 2}") == {"a": 1, "b": 2}


def test_lenient_prose_before_json():
    assert parse_partial('Sure! Here is the JSON:\n```json\n{"ok":true}\n```') == {
        "ok": True
    }


def test_lenient_line_comments():
    assert parse_partial('{\n  // comment\n  "a": 1\n}') == {"a": 1}


def test_lenient_block_comments():
    assert parse_partial('{ /* block */ "a": 1 /* trailing */ }') == {"a": 1}


def test_lenient_unicode_escape():
    assert parse_partial('"\\u2603"') == "☃"


def test_lenient_surrogate_pair():
    assert parse_partial('"\\uD83D\\uDE80"') == "\U0001f680"


def test_strict_trailing_comma_array():
    p = JsonStreamParser(lenient=False)
    with pytest.raises(Exception):
        p.push("[1,2,]")


def test_strict_trailing_comma_object():
    p = JsonStreamParser(lenient=False)
    with pytest.raises(Exception):
        p.push('{"a":1,}')


def test_strict_unknown_literal():
    p = JsonStreamParser(lenient=False)
    with pytest.raises(Exception):
        p.push("truex")


def test_max_depth():
    p = JsonStreamParser(max_depth=3)
    with pytest.raises(Exception):
        p.push("[[[[[1]]]]]")


def test_max_string_length():
    p = JsonStreamParser(max_string_length=5)
    with pytest.raises(Exception):
        p.push('"123456"')


def test_field_events():
    events = []
    p = JsonStreamParser()
    p.on("field", lambda path, value: events.append(value))
    p.push('{"a":1,"b":[true,null]}')
    p.end()
    assert events == [1, True, None]


def test_complete_event_fires_once():
    counter = {"n": 0}
    p = JsonStreamParser()
    p.on("complete", lambda v: counter.update(n=counter["n"] + 1))
    p.push('{"x":1}')
    p.end()
    assert counter["n"] == 1


def test_parse_partial_recovers_truncated():
    truncated = '{"type":"tool_use","name":"edit","input":{"path":"a/b.ts","cont'
    result = parse_partial(truncated)
    assert result["type"] == "tool_use"
    assert result["name"] == "edit"
    assert result["input"] == {"path": "a/b.ts", "cont": None}


def test_parse_partial_empty():
    assert parse_partial("") is None


def test_parse_partial_pure_prose():
    assert parse_partial("hello world this has no json") is None


def test_streaming_top_level_string():
    p = JsonStreamParser()
    p.push('"he')
    assert p.snapshot().value == "he"
    p.push('llo"')
    p.end()
    assert p.snapshot().value == "hello"


def test_streaming_top_level_number():
    p = JsonStreamParser()
    p.push("123")
    p.end()
    assert p.snapshot().value == 123


def test_streaming_top_level_literal():
    p = JsonStreamParser()
    p.push("tr")
    assert p.snapshot().value is None  # partial literal coerces to None
    p.push("ue")
    p.end()
    assert p.snapshot().value is True


def test_messy_llm_input():
    messy = """Sure! Here is the data.
```json
{
  // toolset
  "tools": [
    {"name": 'edit_file', "args": {path: "a.ts", line: 10}},
    {"name": "read_file", "args": {path: "b.ts",}}
  ],
  count: 2
}
```
Anything else?"""
    v = parse_partial(messy)
    assert len(v["tools"]) == 2
    assert v["count"] == 2


def test_truncated_mid_deep_string():
    truncated = (
        '{"events":[{"id":"a"},{"id":"b"},{"id":"c","note":"this is going to be cut'
    )
    v = parse_partial(truncated)
    assert len(v["events"]) == 3
    assert v["events"][2]["id"] == "c"
    assert v["events"][2]["note"] == "this is going to be cut"


def test_path_tracking():
    p = JsonStreamParser()
    p.push('{"items":[{"name":"')
    assert p.snapshot().path == ["items", 0, "name"]


def test_every_byte_snapshot_round_trips():
    v = {
        "type": "response",
        "items": [
            {"id": 1, "tags": ["a", "b"], "meta": {"ok": True}},
            {"id": 2, "tags": [], "meta": {"ok": False, "why": "rate"}},
        ],
        "cursor": "next",
    }
    text = json.dumps(v)
    p = JsonStreamParser()
    for ch in text:
        p.push(ch)
        snap = p.snapshot()
        # Round-trip through json: must always succeed.
        json.loads(json.dumps(snap.value))
    p.end()
    assert p.snapshot().value == v


@pytest.mark.parametrize(
    "text",
    [
        '{"name":"Cla',
        '{"a":1,"b":"hi',
        '["x","incomp',
        '{"k":"v","j":',
        '{"a":1,"b":',
        '{"name":',
        "{abc",
        '"top level str',
        '{"items":[{"name":"foo","note":"trunc',
    ],
)
def test_end_preserves_partial_snapshot(text):
    # end() must perform the same synthetic closure as snapshot(): an open
    # string, pending value, or unquoted key is preserved, never silently
    # dropped. Regression test for data loss on truncated input.
    p = JsonStreamParser()
    p.push(text)
    before = p.snapshot().value
    p.end()
    after = p.snapshot()
    assert after.value == before
    assert after.complete


def test_end_keeps_open_string_value():
    p = JsonStreamParser()
    p.push('{"name":"Clau')
    p.end()
    assert p.snapshot().value == {"name": "Clau"}


def test_end_pending_key_becomes_null():
    p = JsonStreamParser()
    p.push('{"a":1,"b":')
    p.end()
    assert p.snapshot().value == {"a": 1, "b": None}


def test_end_complete_event_fires_once_on_truncated():
    counter = {"n": 0}
    p = JsonStreamParser()
    p.on("complete", lambda v: counter.update(n=counter["n"] + 1))
    p.push('{"name":"Cla')
    p.end()
    assert counter["n"] == 1
    assert p.snapshot().value == {"name": "Cla"}
