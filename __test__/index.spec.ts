import {
  Bool,
  Dictionary,
  Field,
  Float32,
  Float64,
  Int8,
  Int16,
  Int32,
  Int64,
  List,
  Map_ as ArrowMap,
  Struct,
  Table,
  TimestampMillisecond,
  Uint8,
  Uint16,
  Uint32,
  Uint64,
  Utf8,
  makeVector,
  tableToIPC,
  vectorFromArray,
} from 'apache-arrow'
import test from 'ava'

import { arrowIpcToJson, arrowIpcToJsonTimed } from '../index'

function makeIpcStream(table: Table): Buffer {
  return Buffer.from(tableToIPC(table, 'stream'))
}

function makeIpcFile(table: Table): Buffer {
  return Buffer.from(tableToIPC(table, 'file'))
}

function parse(buf: Buffer): Array<Record<string, any>> {
  return JSON.parse(arrowIpcToJson(buf))
}

// ---------------------------------------------------------------------------
// Basic functionality
// ---------------------------------------------------------------------------

test('arrowIpcToJson is a function', (t) => {
  t.is(typeof arrowIpcToJson, 'function')
})

test('arrowIpcToJsonTimed is a function', (t) => {
  t.is(typeof arrowIpcToJsonTimed, 'function')
})

test('throws on invalid input', (t) => {
  t.throws(() => arrowIpcToJson(Buffer.from('not arrow data')))
})

test('throws on empty buffer', (t) => {
  t.throws(() => arrowIpcToJson(Buffer.alloc(0)))
})

test('handles empty table', (t) => {
  const table = new Table({ id: vectorFromArray(Int32Array.from([])) })
  t.deepEqual(parse(makeIpcStream(table)), [])
})

test('handles single-row table', (t) => {
  const table = new Table({ x: vectorFromArray(Int32Array.from([42])) })
  const rows = parse(makeIpcStream(table))
  t.is(rows.length, 1)
  t.is(rows[0].x, 42)
})

// ---------------------------------------------------------------------------
// IPC format detection (stream vs file)
// ---------------------------------------------------------------------------

test('reads IPC stream format', (t) => {
  const table = new Table({ v: vectorFromArray(Int32Array.from([1, 2])) })
  const rows = parse(makeIpcStream(table))
  t.is(rows.length, 2)
  t.is(rows[0].v, 1)
})

test('reads IPC file format', (t) => {
  const table = new Table({ v: vectorFromArray(Int32Array.from([1, 2])) })
  const rows = parse(makeIpcFile(table))
  t.is(rows.length, 2)
  t.is(rows[0].v, 1)
})

test('stream and file produce identical output', (t) => {
  const table = new Table({
    id: vectorFromArray(Int32Array.from([1, 2, 3])),
    name: vectorFromArray(['a', 'b', 'c']),
  })
  const fromStream = arrowIpcToJson(makeIpcStream(table))
  const fromFile = arrowIpcToJson(makeIpcFile(table))
  t.is(fromStream, fromFile)
})

// ---------------------------------------------------------------------------
// Boolean
// ---------------------------------------------------------------------------

test('decodes Boolean column', (t) => {
  const table = new Table({
    flag: vectorFromArray([true, false, true], new Bool()),
  })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].flag, true)
  t.is(rows[1].flag, false)
  t.is(rows[2].flag, true)
})

// ---------------------------------------------------------------------------
// Integer types
// ---------------------------------------------------------------------------

test('decodes Int8 column', (t) => {
  const table = new Table({ v: vectorFromArray(Int8Array.from([-128, 0, 127])) })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].v, -128)
  t.is(rows[1].v, 0)
  t.is(rows[2].v, 127)
})

test('decodes Int16 column', (t) => {
  const table = new Table({ v: vectorFromArray(Int16Array.from([-32768, 0, 32767])) })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].v, -32768)
  t.is(rows[1].v, 0)
  t.is(rows[2].v, 32767)
})

test('decodes Int32 column', (t) => {
  const table = new Table({ v: vectorFromArray(Int32Array.from([-2147483648, 0, 2147483647])) })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].v, -2147483648)
  t.is(rows[1].v, 0)
  t.is(rows[2].v, 2147483647)
})

test('decodes Int64 within safe range as numbers', (t) => {
  const table = new Table({ v: vectorFromArray(BigInt64Array.from([100n, -200n, 0n])) })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].v, 100)
  t.is(rows[1].v, -200)
  t.is(rows[2].v, 0)
})

test('decodes Int64 at 2^53 boundary as number', (t) => {
  const limit = 2n ** 53n
  const table = new Table({ v: vectorFromArray(BigInt64Array.from([limit, -limit])) })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].v, Number(limit))
  t.is(rows[1].v, Number(-limit))
})

test('decodes Int64 exceeding 2^53 as string', (t) => {
  const over = 2n ** 53n + 1n
  const table = new Table({ v: vectorFromArray(BigInt64Array.from([over, -over])) })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].v, over.toString())
  t.is(rows[1].v, (-over).toString())
})

test('decodes UInt8 column', (t) => {
  const table = new Table({ v: vectorFromArray(Uint8Array.from([0, 128, 255])) })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].v, 0)
  t.is(rows[1].v, 128)
  t.is(rows[2].v, 255)
})

test('decodes UInt16 column', (t) => {
  const table = new Table({ v: vectorFromArray(Uint16Array.from([0, 65535])) })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].v, 0)
  t.is(rows[1].v, 65535)
})

test('decodes UInt32 column', (t) => {
  const table = new Table({ v: vectorFromArray(Uint32Array.from([0, 4294967295])) })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].v, 0)
  t.is(rows[1].v, 4294967295)
})

test('decodes UInt64 within safe range as numbers', (t) => {
  const table = new Table({ v: vectorFromArray(BigUint64Array.from([0n, 42n, 2n ** 53n])) })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].v, 0)
  t.is(rows[1].v, 42)
  t.is(rows[2].v, Number(2n ** 53n))
})

test('decodes UInt64 exceeding 2^53 as string', (t) => {
  const over = 2n ** 53n + 1n
  const table = new Table({ v: vectorFromArray(BigUint64Array.from([over])) })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].v, over.toString())
})

// ---------------------------------------------------------------------------
// Float types
// ---------------------------------------------------------------------------

test('decodes Float32 column', (t) => {
  const table = new Table({ v: vectorFromArray(Float32Array.from([1.5, -0.25, 0])) })
  const rows = parse(makeIpcStream(table))
  t.is(typeof rows[0].v, 'number')
  t.true(Math.abs(rows[0].v - 1.5) < 1e-6)
  t.true(Math.abs(rows[1].v - -0.25) < 1e-6)
  t.is(rows[2].v, 0)
})

test('decodes Float64 column', (t) => {
  const table = new Table({ v: vectorFromArray(Float64Array.from([3.141592653589793, -1e-10, 1e20])) })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].v, 3.141592653589793)
  t.true(Math.abs(rows[1].v - -1e-10) < 1e-20)
  t.is(rows[2].v, 1e20)
})

test('decodes Float64 NaN as JSON null', (t) => {
  const table = new Table({ v: vectorFromArray(Float64Array.from([NaN])) })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].v, null)
})

test('decodes Float64 Infinity as JSON null', (t) => {
  const table = new Table({ v: vectorFromArray(Float64Array.from([Infinity])) })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].v, null)
})

test('decodes Float64 -Infinity as JSON null', (t) => {
  const table = new Table({ v: vectorFromArray(Float64Array.from([-Infinity])) })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].v, null)
})

test('decodes Float64 mixed finite and non-finite', (t) => {
  const table = new Table({
    v: vectorFromArray(Float64Array.from([1.5, NaN, Infinity, -Infinity, 2.5])),
  })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].v, 1.5)
  t.is(rows[1].v, null)
  t.is(rows[2].v, null)
  t.is(rows[3].v, null)
  t.is(rows[4].v, 2.5)
})

test('decodes Float64 negative zero as -0', (t) => {
  const table = new Table({ v: vectorFromArray(Float64Array.from([-0])) })
  const rows = parse(makeIpcStream(table))
  t.true(Object.is(rows[0].v, -0))
})

// ---------------------------------------------------------------------------
// String types
// ---------------------------------------------------------------------------

test('decodes Utf8 column', (t) => {
  const table = new Table({ s: vectorFromArray(['hello', 'world', '']) })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].s, 'hello')
  t.is(rows[1].s, 'world')
  t.is(rows[2].s, '')
})

test('handles strings with JSON-special characters', (t) => {
  const table = new Table({
    s: vectorFromArray([
      'has "quotes"',
      'back\\slash',
      'new\nline',
      'tab\there',
      'carriage\rreturn',
    ]),
  })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].s, 'has "quotes"')
  t.is(rows[1].s, 'back\\slash')
  t.is(rows[2].s, 'new\nline')
  t.is(rows[3].s, 'tab\there')
  t.is(rows[4].s, 'carriage\rreturn')
})

test('handles strings with control characters', (t) => {
  const table = new Table({
    s: vectorFromArray([
      'null\x00char',
      'bell\x07char',
      'bs\x08char',
      'ff\x0cchar',
      'esc\x1bchar',
    ]),
  })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].s, 'null\x00char')
  t.is(rows[1].s, 'bell\x07char')
  t.is(rows[2].s, 'bs\x08char')
  t.is(rows[3].s, 'ff\x0cchar')
  t.is(rows[4].s, 'esc\x1bchar')
})

test('handles strings with unicode (multi-byte UTF-8)', (t) => {
  const table = new Table({
    s: vectorFromArray(['café', '日本語', '🎉🚀', 'Ñoño', '中文测试']),
  })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].s, 'café')
  t.is(rows[1].s, '日本語')
  t.is(rows[2].s, '🎉🚀')
  t.is(rows[3].s, 'Ñoño')
  t.is(rows[4].s, '中文测试')
})

test('handles string that is only special characters', (t) => {
  const table = new Table({
    s: vectorFromArray(['"\\"\\n\\t"', '\n\r\t', '\x00\x01\x1f']),
  })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].s, '"\\"\\n\\t"')
  t.is(rows[1].s, '\n\r\t')
  t.is(rows[2].s, '\x00\x01\x1f')
})

// ---------------------------------------------------------------------------
// Null handling
// ---------------------------------------------------------------------------

test('null values are omitted from top-level row objects', (t) => {
  const id = vectorFromArray(Int32Array.from([1, 2, 3]))
  const name = vectorFromArray(['alice', null, 'charlie'])
  const table = new Table({ id, name })

  const rows = parse(makeIpcStream(table))
  t.is(rows.length, 3)
  t.deepEqual(Object.keys(rows[0]), ['id', 'name'])
  t.deepEqual(Object.keys(rows[1]), ['id'])
  t.is(rows[1].name, undefined)
  t.is(rows[2].name, 'charlie')
})

test('row with all null columns produces empty object', (t) => {
  const table = new Table({
    a: vectorFromArray([null, 'x']),
    b: vectorFromArray([null, 'y']),
  })
  const rows = parse(makeIpcStream(table))
  t.deepEqual(rows[0], {})
  t.deepEqual(rows[1], { a: 'x', b: 'y' })
})

test('nullable Int32 column', (t) => {
  const table = new Table({
    v: vectorFromArray([1, null, 3], new Int32()),
  })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].v, 1)
  t.is(rows[1].v, undefined)
  t.is(rows[2].v, 3)
})

test('nullable Float64 column', (t) => {
  const table = new Table({
    v: vectorFromArray([1.5, null, 3.5], new Float64()),
  })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].v, 1.5)
  t.is(rows[1].v, undefined)
  t.is(rows[2].v, 3.5)
})

test('nullable Boolean column', (t) => {
  const table = new Table({
    v: vectorFromArray([true, null, false], new Bool()),
  })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].v, true)
  t.is(rows[1].v, undefined)
  t.is(rows[2].v, false)
})

// ---------------------------------------------------------------------------
// List types
// ---------------------------------------------------------------------------

test('decodes List<Int32>', (t) => {
  const LIST_TYPE = new List(new Field('item', new Int32()))
  const table = new Table({
    v: vectorFromArray([[1, 2, 3], [], [10]], LIST_TYPE),
  })
  const rows = parse(makeIpcStream(table))
  t.deepEqual(rows[0].v, [1, 2, 3])
  t.deepEqual(rows[1].v, [])
  t.deepEqual(rows[2].v, [10])
})

test('decodes List<Int64> as array of numbers', (t) => {
  const LIST_TYPE = new List(new Field('item', new Int64()))
  const table = new Table({
    v: vectorFromArray([[1n, 2n, 3n], null, [10n, 20n]], LIST_TYPE),
  })
  const rows = parse(makeIpcStream(table))
  t.deepEqual(rows[0].v, [1, 2, 3])
  t.is(rows[1].v, undefined)
  t.deepEqual(rows[2].v, [10, 20])
})

test('decodes List<Utf8>', (t) => {
  const LIST_TYPE = new List(new Field('item', new Utf8()))
  const table = new Table({
    v: vectorFromArray([['a', 'b'], [], ['hello']], LIST_TYPE),
  })
  const rows = parse(makeIpcStream(table))
  t.deepEqual(rows[0].v, ['a', 'b'])
  t.deepEqual(rows[1].v, [])
  t.deepEqual(rows[2].v, ['hello'])
})

test('decodes nested List<List<Int32>>', (t) => {
  const INNER = new List(new Field('item', new Int32()))
  const OUTER = new List(new Field('item', INNER))
  const table = new Table({
    v: vectorFromArray([[[1, 2], [3]], [[4]]], OUTER),
  })
  const rows = parse(makeIpcStream(table))
  t.deepEqual(rows[0].v, [[1, 2], [3]])
  t.deepEqual(rows[1].v, [[4]])
})

// ---------------------------------------------------------------------------
// Struct
// ---------------------------------------------------------------------------

test('decodes Struct column', (t) => {
  const STRUCT_TYPE = new Struct([new Field('x', new Int32()), new Field('y', new Utf8())])
  const table = new Table({
    s: vectorFromArray([{ x: 1, y: 'a' }, { x: 2, y: 'b' }], STRUCT_TYPE),
  })
  const rows = parse(makeIpcStream(table))
  t.deepEqual(rows[0].s, { x: 1, y: 'a' })
  t.deepEqual(rows[1].s, { x: 2, y: 'b' })
})

// ---------------------------------------------------------------------------
// Map types
// ---------------------------------------------------------------------------

test('decodes Map<Utf8, Utf8> as JSON object', (t) => {
  const MAP_TYPE = new ArrowMap(
    new Field('entries', new Struct([new Field('key', new Utf8(), false), new Field('value', new Utf8(), false)]), false) as any,
  )
  const table = new Table({
    tags: vectorFromArray(
      [new Map([['highway', 'primary'], ['name', 'Main St']]), null, new Map([['amenity', 'cafe']])],
      MAP_TYPE,
    ),
  })
  const rows = parse(makeIpcStream(table))
  t.deepEqual(rows[0].tags, { highway: 'primary', name: 'Main St' })
  t.is(rows[1].tags, undefined)
  t.deepEqual(rows[2].tags, { amenity: 'cafe' })
})

test('Map with empty entries is omitted', (t) => {
  const MAP_TYPE = new ArrowMap(
    new Field('entries', new Struct([new Field('key', new Utf8(), false), new Field('value', new Utf8(), false)]), false) as any,
  )
  const table = new Table({
    id: vectorFromArray(Int32Array.from([1])),
    tags: vectorFromArray([new Map<string, string>()], MAP_TYPE),
  })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].tags, undefined)
})

test('Map with special characters in keys and values', (t) => {
  const MAP_TYPE = new ArrowMap(
    new Field('entries', new Struct([new Field('key', new Utf8(), false), new Field('value', new Utf8(), false)]), false) as any,
  )
  const table = new Table({
    tags: vectorFromArray(
      [new Map([['key "with" quotes', 'value\nwith\nnewlines'], ['back\\slash', 'tab\there']])],
      MAP_TYPE,
    ),
  })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].tags['key "with" quotes'], 'value\nwith\nnewlines')
  t.is(rows[0].tags['back\\slash'], 'tab\there')
})

// ---------------------------------------------------------------------------
// Nested Struct in List
// ---------------------------------------------------------------------------

test('decodes List<Struct> (members-like pattern)', (t) => {
  const MEMBERS_TYPE = new List(
    new Field(
      'item',
      new Struct([new Field('type', new Utf8()), new Field('ref', new Int64()), new Field('role', new Utf8())]),
    ),
  )
  const table = new Table({
    members: vectorFromArray(
      [
        [
          { type: 'node', ref: 100n, role: 'stop' },
          { type: 'way', ref: 200n, role: '' },
        ],
        null,
      ],
      MEMBERS_TYPE,
    ),
  })
  const rows = parse(makeIpcStream(table))
  t.deepEqual(rows[0].members, [
    { type: 'node', ref: 100, role: 'stop' },
    { type: 'way', ref: 200, role: '' },
  ])
  t.is(rows[1].members, undefined)
})

// ---------------------------------------------------------------------------
// Dictionary type
// ---------------------------------------------------------------------------

test('decodes Dictionary<Int32, Utf8> column', (t) => {
  const dict = vectorFromArray(['red', 'green', 'red', 'blue', 'green'], new Dictionary(new Utf8(), new Int32()))
  const table = new Table({ color: dict })
  const rows = parse(makeIpcStream(table))
  t.is(rows[0].color, 'red')
  t.is(rows[1].color, 'green')
  t.is(rows[2].color, 'red')
  t.is(rows[3].color, 'blue')
  t.is(rows[4].color, 'green')
})

// ---------------------------------------------------------------------------
// Timestamp
// ---------------------------------------------------------------------------

test('decodes Timestamp column as string', (t) => {
  const now = Date.now()
  const table = new Table({
    ts: vectorFromArray([BigInt(now), BigInt(now + 1000)], new TimestampMillisecond()),
  })
  const rows = parse(makeIpcStream(table))
  t.is(typeof rows[0].ts, 'string')
  t.is(typeof rows[1].ts, 'string')
})

// ---------------------------------------------------------------------------
// Mixed-type table (simulating real OSM data shape)
// ---------------------------------------------------------------------------

test('decodes OSM-like table with all column types', (t) => {
  const MAP_TYPE = new ArrowMap(
    new Field('entries', new Struct([new Field('key', new Utf8(), false), new Field('value', new Utf8(), false)]), false) as any,
  )
  const NDS_TYPE = new List(new Field('item', new Int64()))
  const MEMBERS_TYPE = new List(
    new Field(
      'item',
      new Struct([new Field('type', new Utf8()), new Field('ref', new Int64()), new Field('role', new Utf8())]),
    ),
  )

  const table = new Table({
    id: vectorFromArray(BigInt64Array.from([1001n, 1002n, 1003n])),
    type: vectorFromArray(Int8Array.from([1, 2, 3])),
    lat: vectorFromArray(Float64Array.from([48.123, 0, 0])),
    lon: vectorFromArray(Float64Array.from([11.456, 0, 0])),
    version: vectorFromArray(Int32Array.from([1, 3, 1])),
    tags: vectorFromArray(
      [new Map([['highway', 'primary']]), new Map([['name', 'Test Way']]), null],
      MAP_TYPE,
    ),
    nds: vectorFromArray([null, [100n, 200n, 300n], null], NDS_TYPE),
    members: vectorFromArray(
      [null, null, [{ type: 'way', ref: 500n, role: 'outer' }]],
      MEMBERS_TYPE,
    ),
  })

  const rows = parse(makeIpcStream(table))
  t.is(rows.length, 3)

  // Node
  t.is(rows[0].id, 1001)
  t.is(rows[0].type, 1)
  t.is(rows[0].lat, 48.123)
  t.is(rows[0].lon, 11.456)
  t.deepEqual(rows[0].tags, { highway: 'primary' })
  t.is(rows[0].nds, undefined)
  t.is(rows[0].members, undefined)

  // Way
  t.is(rows[1].id, 1002)
  t.is(rows[1].type, 2)
  t.deepEqual(rows[1].tags, { name: 'Test Way' })
  t.deepEqual(rows[1].nds, [100, 200, 300])
  t.is(rows[1].members, undefined)

  // Relation
  t.is(rows[2].id, 1003)
  t.is(rows[2].type, 3)
  t.is(rows[2].tags, undefined)
  t.deepEqual(rows[2].members, [{ type: 'way', ref: 500, role: 'outer' }])
})

// ---------------------------------------------------------------------------
// arrowIpcToJsonTimed
// ---------------------------------------------------------------------------

test('arrowIpcToJsonTimed returns timing breakdown', (t) => {
  const table = new Table({
    id: vectorFromArray(Int32Array.from([1, 2, 3])),
    name: vectorFromArray(['a', 'b', 'c']),
  })
  const timed = arrowIpcToJsonTimed(makeIpcStream(table))

  t.is(typeof timed.json, 'string')
  t.is(typeof timed.ipcParseUs, 'number')
  t.is(typeof timed.jsonWriteUs, 'number')
  t.is(typeof timed.totalUs, 'number')
  t.is(timed.rows, 3)
  t.true(timed.jsonBytes > 0)
  t.true(timed.totalUs >= timed.ipcParseUs)
  t.true(timed.totalUs >= timed.jsonWriteUs)

  const rows = JSON.parse(timed.json)
  t.is(rows.length, 3)
  t.is(rows[0].id, 1)
})

test('arrowIpcToJsonTimed produces same JSON as arrowIpcToJson', (t) => {
  const table = new Table({
    id: vectorFromArray(BigInt64Array.from([1n, 2n])),
    name: vectorFromArray(['hello', 'world']),
  })
  const buf = makeIpcStream(table)
  const fast = arrowIpcToJson(buf)
  const timed = arrowIpcToJsonTimed(buf)
  t.is(fast, timed.json)
})

// ---------------------------------------------------------------------------
// Output validity: ensure output is always parseable JSON
// ---------------------------------------------------------------------------

test('output is valid JSON for large row count', (t) => {
  const n = 10000
  const table = new Table({
    id: vectorFromArray(Int32Array.from(Array.from({ length: n }, (_, i) => i))),
    val: vectorFromArray(Float64Array.from(Array.from({ length: n }, (_, i) => i * 0.1))),
  })
  const json = arrowIpcToJson(makeIpcStream(table))
  const rows = JSON.parse(json)
  t.is(rows.length, n)
  t.is(rows[0].id, 0)
  t.is(rows[n - 1].id, n - 1)
})

// ---------------------------------------------------------------------------
// Consistency: multiple calls produce identical output
// ---------------------------------------------------------------------------

test('deterministic output across multiple calls', (t) => {
  const table = new Table({
    id: vectorFromArray(Int32Array.from([1, 2, 3])),
    name: vectorFromArray(['a', 'b', 'c']),
  })
  const buf = makeIpcStream(table)
  const a = arrowIpcToJson(buf)
  const b = arrowIpcToJson(buf)
  const c = arrowIpcToJson(buf)
  t.is(a, b)
  t.is(b, c)
})
