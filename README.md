# arrow-to-json

A native Node.js addon that converts [Apache Arrow](https://arrow.apache.org/) IPC bytes to JSON. Written in Rust using [napi-rs](https://napi.rs/) for maximum throughput — typically **5-7x faster** than parsing with the JavaScript `apache-arrow` library and serializing with `JSON.stringify`.

## Install

```bash
npm install arrow-to-json
```

## Usage

```ts
import { arrowIpcToJson } from 'arrow-to-json'

const json: string = arrowIpcToJson(arrowBytes)
const rows: unknown[] = JSON.parse(json)
```

The function accepts a `Buffer` containing Arrow IPC bytes (file or streaming format, auto-detected) and returns a JSON string. Each element in the resulting array is a row object with column names as keys.

## Supported Arrow types

| Arrow type                                                    | JSON representation                |
| ------------------------------------------------------------- | ---------------------------------- |
| `Boolean`                                                     | `true` / `false`                   |
| `Int8` .. `Int32`, `UInt8` .. `UInt32`                        | number                             |
| `Int64` / `UInt64`                                            | number if ≤ 2^53, string otherwise |
| `Float16` / `Float32` / `Float64`                             | number (NaN/Infinity → `null`)     |
| `Utf8` / `LargeUtf8`                                          | string                             |
| `Binary` / `LargeBinary`                                      | array of byte values               |
| `List` / `LargeList` / `FixedSizeList`                        | array (recursive)                  |
| `Struct`                                                      | object (recursive)                 |
| `Map<Utf8, *>`                                                | object (`{key: value}`)            |
| `Map<non-Utf8, *>`                                            | array of `{key, value}` objects    |
| `Dictionary<*, *>`                                            | resolved value (recursive)         |
| `Timestamp`, `Date32/64`, `Time32/64`, `Duration`, `Interval` | string (cast to Utf8)              |
| Null values                                                   | omitted from output objects        |

## API

### `arrowIpcToJson(data: Buffer): string`

Converts Arrow IPC bytes to a JSON array string.

**Parameters:**

- `data` — `Buffer` containing Arrow IPC bytes

**Returns:** JSON string representing an array of row objects

**Throws:** if the input is not valid Arrow IPC data

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
