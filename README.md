# arrow-to-json

A native Node.js addon that converts [Apache Arrow](https://arrow.apache.org/) IPC bytes to JSON. Written in Rust using [napi-rs](https://napi.rs/) for maximum throughput — typically **~20x faster** than parsing with the JavaScript `apache-arrow` library and serializing with `JSON.stringify`.

## Install

```bash
npm install arrow-to-json
```

## Usage

### Row-object format

```ts
import { arrowIpcToJson } from 'arrow-to-json'

const json: string = arrowIpcToJson(arrowBytes)
const rows: unknown[] = JSON.parse(json)
// [{ id: 1, name: "Alice" }, { id: 2, name: "Bob" }]
```

Returns a JSON array of row objects. Each element has column names as keys.

### Columnar format (recommended)

```ts
import { arrowIpcToJsonColumns } from 'arrow-to-json'

const json: string = arrowIpcToJsonColumns(arrowBytes)
const cols: Record<string, unknown[]> = JSON.parse(json)
// { id: [1, 2], name: ["Alice", "Bob"] }
```

Returns a JSON object where each key is a column name and each value is an array of all row values. This format is **~36% smaller** than the row-object format because column names appear only once, leading to proportionally faster `JSON.parse` on the JS side.

### Profiling with timed variants

Both formats have a `*Timed` variant that returns Rust-internal timing breakdown:

```ts
import { arrowIpcToJsonColumnsTimed } from 'arrow-to-json'

const result = arrowIpcToJsonColumnsTimed(arrowBytes)
// result.json        — JSON string
// result.ipcParseUs  — microseconds spent parsing IPC bytes
// result.jsonWriteUs — microseconds spent writing JSON
// result.totalUs     — total microseconds (Rust wall clock)
// result.rows        — number of rows decoded
// result.jsonBytes   — byte length of the JSON string
```

`arrowIpcToJsonTimed` provides the same for the row-object format.

## Supported Arrow types

| Arrow type                                                    | JSON representation                  |
| ------------------------------------------------------------- | ------------------------------------ |
| `Boolean`                                                     | `true` / `false`                     |
| `Int8` .. `Int32`, `UInt8` .. `UInt32`                        | number                               |
| `Int64` / `UInt64`                                            | number if ≤ 2^53, string otherwise   |
| `Float16` / `Float32` / `Float64`                             | number (`NaN` / `Infinity` → `null`) |
| `Utf8` / `LargeUtf8`                                          | string (JSON-escaped)                |
| `Binary` / `LargeBinary`                                      | array of byte values                 |
| `List` / `LargeList` / `FixedSizeList`                        | array (recursive)                    |
| `Struct`                                                      | object (recursive)                   |
| `Map<Utf8, *>`                                                | object (`{key: value}`)              |
| `Map<non-Utf8, *>`                                            | array of `{key, value}` objects      |
| `Dictionary<*, *>`                                            | resolved value (recursive)           |
| `Timestamp`, `Date32/64`, `Time32/64`, `Duration`, `Interval` | string (cast to Utf8)                |
| Null values                                                   | omitted from output objects          |
| Empty `Map`                                                   | omitted from output objects          |

## API

### `arrowIpcToJson(data: Buffer): string`

Converts Arrow IPC bytes to a JSON array of row objects.

- **data** — `Buffer` containing Arrow IPC bytes (file or stream format)
- **Returns** — JSON string: `[{"col": val, ...}, ...]`
- **Throws** — if the input is not valid Arrow IPC data

### `arrowIpcToJsonColumns(data: Buffer): string`

Converts Arrow IPC bytes to a columnar JSON object.

- **data** — `Buffer` containing Arrow IPC bytes (file or stream format)
- **Returns** — JSON string: `{"col": [v1, v2, ...], ...}`
- **Throws** — if the input is not valid Arrow IPC data

### `arrowIpcToJsonTimed(data: Buffer): TimedResult`

Same as `arrowIpcToJson` with Rust-internal timing breakdown.

### `arrowIpcToJsonColumnsTimed(data: Buffer): TimedResult`

Same as `arrowIpcToJsonColumns` with Rust-internal timing breakdown.

```ts
interface TimedResult {
  json: string // the JSON string
  ipcParseUs: number // IPC parsing time (µs)
  jsonWriteUs: number // JSON serialization time (µs)
  totalUs: number // total Rust wall-clock time (µs)
  rows: number // number of rows decoded
  jsonBytes: number // byte length of JSON output
}
```

## Performance

The Rust implementation uses several optimizations for throughput:

- **Direct JSON writing** — JSON is written directly to a pre-allocated `String` buffer, bypassing any intermediate value tree.
- **Pre-downcast columns** — Arrow column types are resolved once per batch into a `ColWriter` enum, eliminating per-cell dynamic dispatch and `downcast_ref` in the hot loop.
- **Specialized fast paths** — Common column patterns (`Map<Utf8, Utf8>`, `List<Int64>`) have dedicated write functions that skip generic dispatch.
- **Fast number formatting** — Uses [`itoa`](https://crates.io/crates/itoa) and [`ryu`](https://crates.io/crates/ryu) for integer and float serialization.
- **Pre-computed column metadata** — JSON-escaped column keys and skip-check flags are computed once per batch.

## Development

```bash
yarn install
yarn build          # native release build
yarn build:debug    # native debug build
yarn test           # run tests
yarn bench          # run benchmarks
```

### Release

Pushing to `main` triggers CI. The `publish` job inspects the last commit message:

- Exact semver (e.g. `0.2.0`) → publishes to npm as `latest`
- Semver with pre-release suffix (e.g. `0.2.0-beta.1`) → publishes with `next` tag
- Anything else → skips publishing

To release a new version:

```bash
npm version patch   # or minor / major
git push && git push --tags
```

## License

MIT
