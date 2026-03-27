# arrow-to-json

Fast Arrow IPC to JSON conversion using Rust (napi-rs).

## Usage

```ts
import { arrowIpcToJson } from 'arrow-to-json'

// Pass Arrow IPC bytes (file or streaming format)
const json: string = arrowIpcToJson(arrowBytes)
const rows: unknown[] = JSON.parse(json)
```

## API

### `arrowIpcToJson(data: Buffer): string`

Converts Arrow IPC bytes to a JSON array string. Each element is a row object with column names as keys.

- Accepts both Arrow IPC **file** and **streaming** formats (auto-detected).
- Null values are **omitted** from output objects.
- `Map<Utf8, *>` columns are emitted as JSON objects (`{key: value}`).
- `Int64`/`UInt64` values exceeding `2^53` are emitted as strings.
- `List`, `Struct`, and `Dictionary` types are fully supported.
- Temporal types are cast to their string representation.

## Build

```bash
yarn install
yarn build        # release
yarn build:debug  # debug
yarn test
yarn bench
```
