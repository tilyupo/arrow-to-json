import {
  Field,
  Float64,
  Int32,
  Int64,
  Int8,
  List,
  Map_ as ArrowMap,
  Struct,
  Table,
  Utf8,
  tableFromIPC,
  tableToIPC,
  vectorFromArray,
} from 'apache-arrow'
import { Bench } from 'tinybench'

import { arrowIpcToJson, arrowIpcToJsonColumns } from '../index.js'

// Build a table that resembles OSM tile data
const NUM_ROWS = 5000

const TAGS_TYPE = new ArrowMap(
  new Field(
    'entries',
    new Struct([new Field('key', new Utf8(), false), new Field('value', new Utf8(), false)]),
    false,
  ) as any,
)
const NDS_TYPE = new List(new Field('item', new Int64()))

const ids = BigInt64Array.from(Array.from({ length: NUM_ROWS }, (_, i) => BigInt(i + 1000)))
const types = Int8Array.from(Array.from({ length: NUM_ROWS }, (_, i) => (i % 10 < 7 ? 0 : i % 10 < 9 ? 1 : 2)))
const lats = Float64Array.from(Array.from({ length: NUM_ROWS }, (_, i) => 48.0 + i * 0.001))
const lons = Float64Array.from(Array.from({ length: NUM_ROWS }, (_, i) => 11.0 + i * 0.001))
const versions = Int32Array.from(Array.from({ length: NUM_ROWS }, () => 1))

const tagsArr: Array<Map<string, string> | null> = Array.from({ length: NUM_ROWS }, (_, i) =>
  i % 3 === 0
    ? new Map([
        ['highway', 'residential'],
        ['name', `Street ${i}`],
        ['surface', 'asphalt'],
      ])
    : i % 3 === 1
      ? new Map([['amenity', 'parking']])
      : null,
)

const ndsArr: Array<bigint[] | null> = Array.from({ length: NUM_ROWS }, (_, i) =>
  types[i] === 1 ? [BigInt(i + 1), BigInt(i + 2), BigInt(i + 3)] : null,
)

const table = new Table({
  id: vectorFromArray(ids),
  type: vectorFromArray(types),
  lat: vectorFromArray(lats),
  lon: vectorFromArray(lons),
  version: vectorFromArray(versions),
  tags: vectorFromArray(tagsArr, TAGS_TYPE),
  nds: vectorFromArray(ndsArr, NDS_TYPE),
})

const ipcBuffer = Buffer.from(tableToIPC(table, 'stream'))
console.log(`Benchmark data: ${NUM_ROWS} rows, ${(ipcBuffer.length / 1024).toFixed(1)} KB IPC`)

// JS baseline: parse IPC + iterate rows → JS objects
function jsBaselineObjects(buf: Uint8Array): Record<string, unknown>[] {
  const t = tableFromIPC(buf)
  const rows: Record<string, unknown>[] = []
  for (let r = 0; r < t.numRows; r++) {
    const row: Record<string, unknown> = {}
    for (const field of t.schema.fields) {
      const val = t.getChild(field.name)?.get(r)
      if (val != null) {
        if (typeof val === 'bigint') {
          row[field.name] = Number(val)
        } else if (val instanceof Map) {
          row[field.name] = Object.fromEntries(val)
        } else if (typeof val === 'object' && Symbol.iterator in val) {
          row[field.name] = Array.from(val as Iterable<unknown>, (v) => (typeof v === 'bigint' ? Number(v) : v))
        } else {
          row[field.name] = val
        }
      }
    }
    rows.push(row)
  }
  return rows
}

// Warmup + size comparison
arrowIpcToJson(ipcBuffer)
arrowIpcToJsonColumns(ipcBuffer)
const rowJson = arrowIpcToJson(ipcBuffer)
const colJson = arrowIpcToJsonColumns(ipcBuffer)
const reduction = ((1 - colJson.length / rowJson.length) * 100).toFixed(1)
console.log(`  Row-object JSON: ${(rowJson.length / 1024).toFixed(1)} KB`)
console.log(`  Columnar JSON:   ${(colJson.length / 1024).toFixed(1)} KB (${reduction}% smaller)\n`)

const b = new Bench({ warmupIterations: 5 })

b.add('Rust columnar', () => {
  arrowIpcToJsonColumns(ipcBuffer)
})

b.add('Rust row-object', () => {
  arrowIpcToJson(ipcBuffer)
})

b.add('JS apache-arrow + JSON.stringify', () => {
  JSON.stringify(jsBaselineObjects(ipcBuffer))
})

await b.run()
console.table(b.table())
