# partial-json-stream

[![pypi](https://img.shields.io/pypi/v/partial-json-stream.svg)](https://pypi.org/project/partial-json-stream/)
[![python](https://img.shields.io/pypi/pyversions/partial-json-stream.svg)](https://pypi.org/project/partial-json-stream/)
[![tests](https://img.shields.io/badge/tests-67%20passing-brightgreen.svg)](#)
[![zero deps](https://img.shields.io/badge/dependencies-0-blue.svg)](#)

A streaming JSON parser that yields **partial valid trees** as tokens arrive.

Python port of [`@mukundakatta/streamparse`](https://github.com/MukundaKatta/streamparse).
Built for LLM tool-call payloads, structured-output streams, and any place
`json.loads` waits too long.

```python
from partial_json_stream import JsonStreamParser

p = JsonStreamParser()

p.push('{"name":"Cl')
p.snapshot().value      # {'name': 'Cl'}

p.push('aude","tools":[1,2')
p.snapshot().value      # {'name': 'Claude', 'tools': [1, 2]}

p.push(']}')
p.end()
p.snapshot().complete   # True
```

Every snapshot is **valid JSON**. Synthetic closure of in-progress strings,
numbers, and containers means you can render it directly, persist it, or
round-trip it through `json.loads(json.dumps(...))`.

## Why

Every Python LLM project ships its own broken version of this. They wait for
the full payload (slow), or hand-roll `json.loads` in a try/except on a growing
buffer (O(n²) and useless until the very last byte), or use ad-hoc regex.

`partial-json-stream` is the version you should reuse.

## Install

```bash
pip install partial-json-stream
```

Zero runtime dependencies. Python 3.9+.

## API

### `JsonStreamParser(lenient=True, max_depth=256, max_string_length=None)`

**Lenient mode** (default) tolerates the LLM-isms you actually see in
production:

- trailing commas: `{"a": 1,}`
- single-quoted strings: `{'a': 'hi'}`
- unquoted keys: `{a: 1, b: 2}`
- ` ```json ` code fences
- `// line` and `/* block */` comments
- prose before/after the JSON: `Sure! Here it is: {...}`

Set `lenient=False` for strict RFC 8259 mode.

### `parser.push(chunk: str) -> None`

Feed in more bytes.

### `parser.end() -> None`

Signal end of input. Lenient mode silently closes any open containers.

### `parser.snapshot() -> Snapshot`

Take a snapshot. Always returns valid JSON.

```python
@dataclass
class Snapshot:
    value: Any
    complete: bool
    path: list             # cursor location, e.g. ['tools', 2, 'input', 'path']
    bytes_in: int
    confidence: float      # 0..1
```

### `parser.on(event, fn)`

Subscribe to events. Returns an unsubscribe function.

| event       | callback signature       | when                                  |
| ----------- | ------------------------ | ------------------------------------- |
| `field`     | `(path, value)`          | every leaf commit                     |
| `container` | `(path, value)`          | every `{}` or `[]` close              |
| `partial`   | `(snapshot)`             | every `push()`                        |
| `complete`  | `(value)`                | once, when top-level value closes     |
| `error`     | `(err)`                  | on syntax error (suppresses raise)    |

### `parse_partial(text: str, lenient=True) -> Any`

One-shot helper for a possibly-truncated blob.

```python
from partial_json_stream import parse_partial

truncated = '{"type":"tool_use","name":"edit","input":{"path":"a/b.ts","cont'
parse_partial(truncated)
# {'type': 'tool_use', 'name': 'edit', 'input': {'path': 'a/b.ts', 'cont': None}}
```

## Real-world usage

### Recover a dropped tool call

```python
from partial_json_stream import parse_partial

# The connection dropped before the model finished writing its tool call.
partial = '{"type":"tool_use","name":"edit_file","input":{"path":"a.ts","patches":[{"line":10,"op":"insert","content":"print('

value = parse_partial(partial)
# value['input']['patches'][0] is fully usable.
```

### Stream Anthropic tool calls

```python
from partial_json_stream import JsonStreamParser

parser = JsonStreamParser()

@parser.on("partial", lambda snap: ui.update(snap.value))

for chunk in client.messages.stream(...).text_stream:
    parser.push(chunk)
parser.end()
```

## License

MIT, by [Mukunda Katta](https://github.com/MukundaKatta).
