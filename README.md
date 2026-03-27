# arrow-to-json

A native Node.js addon that converts [Apache Arrow](https://arrow.apache.org/) IPC bytes directly into JavaScript objects. Written in Rust using [napi-rs](https://napi.rs/) for maximum throughput — typically **5-7x faster** than parsing with the JavaScript `apache-arrow` library.

Objects are constructed directly through N-API — no JSON string intermediate, no `JSON.parse` needed.

## Install

```bash
npm install arrow-to-json
```

## Usage

```ts
import { arrowIpcToJson } from 'arrow-to-json'

const rows: Array<Record<string, unknown>> = arrowIpcToJson(arrowBytes)
```

The function accepts a `Buffer` containing Arrow IPC bytes (file or streaming format, auto-detected) and returns an array of row objects with column names as keys.

## Supported Arrow types

| Arrow type | JS representation |
|---|---|
| `Boolean` | `true` / `false` |
| `Int8` .. `Int32`, `UInt8` .. `UInt32` | number |
| `Int64` / `UInt64` | number if ≤ 2^53, string otherwise |
| `Float16` / `Float32` / `Float64` | number (NaN/Infinity → `null`) |
| `Utf8` / `LargeUtf8` | string |
| `Binary` / `LargeBinary` | array of byte values |
| `List` / `LargeList` / `FixedSizeList` | array (recursive) |
| `Struct` | object (recursive) |
| `Map<Utf8, *>` | object (`{key: value}`) |
| `Map<non-Utf8, *>` | array of `{key, value}` objects |
| `Dictionary<*, *>` | resolved value (recursive) |
| `Timestamp`, `Date32/64`, `Time32/64`, `Duration`, `Interval` | string (cast to Utf8) |
| Null values | omitted from output objects |

## API

### `arrowIpcToJson(data: Buffer): Array<Record<string, unknown>>`

Converts Arrow IPC bytes to an array of JavaScript objects.

**Parameters:**
- `data` — `Buffer` containing Arrow IPC bytes

**Returns:** Array of row objects, constructed directly through N-API

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
