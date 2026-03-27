import {
  Field,
  Float64,
  Int32,
  Int64,
  List,
  Map_ as ArrowMap,
  RecordBatch,
  Struct,
  Table,
  Utf8,
  tableToIPC,
  vectorFromArray,
} from 'apache-arrow'
import test from 'ava'

import { arrowIpcToJson } from '../index'

function makeIpcBytes(table: Table): Buffer {
  const ipcBytes = tableToIPC(table, 'stream')
  return Buffer.from(ipcBytes)
}

test('arrowIpcToJson is a function', (t) => {
  t.is(typeof arrowIpcToJson, 'function')
})

test('throws on invalid input', (t) => {
  t.throws(() => arrowIpcToJson(Buffer.from('not arrow data')))
})

test('returns an array of objects directly (no JSON.parse needed)', (t) => {
  const table = new Table({
    id: vectorFromArray(Int32Array.from([1])),
  })

  const result = arrowIpcToJson(makeIpcBytes(table))
  t.true(Array.isArray(result))
  t.is(result.length, 1)
  t.is(typeof result[0], 'object')
})

test('decodes simple scalar columns', (t) => {
  const table = new Table({
    id: vectorFromArray(Int32Array.from([1, 2, 3])),
    name: vectorFromArray(['alice', 'bob', 'charlie']),
    score: vectorFromArray(Float64Array.from([9.5, 8.0, 7.3])),
  })

  const rows = arrowIpcToJson(makeIpcBytes(table))

  t.is(rows.length, 3)
  t.is(rows[0].id, 1)
  t.is(rows[0].name, 'alice')
  t.is(rows[0].score, 9.5)
  t.is(rows[1].id, 2)
  t.is(rows[1].name, 'bob')
  t.is(rows[2].name, 'charlie')
})

test('decodes Int64 values within safe range as numbers', (t) => {
  const table = new Table({
    big: vectorFromArray(BigInt64Array.from([100n, -200n, 42n])),
  })

  const rows = arrowIpcToJson(makeIpcBytes(table))

  t.is(rows.length, 3)
  t.is(rows[0].big, 100)
  t.is(rows[1].big, -200)
  t.is(rows[2].big, 42)
})

test('decodes Map<Utf8, Utf8> as plain object', (t) => {
  const TAGS_TYPE = new ArrowMap(
    new Field(
      'entries',
      new Struct([new Field('key', new Utf8(), false), new Field('value', new Utf8(), false)]),
      false,
    ) as any,
  )

  const tagsArr = [
    new Map([
      ['highway', 'primary'],
      ['name', 'Main St'],
    ]),
    null,
    new Map([['amenity', 'cafe']]),
  ]

  const table = new Table({
    id: vectorFromArray(Int32Array.from([1, 2, 3])),
    tags: vectorFromArray(tagsArr, TAGS_TYPE),
  })

  const rows = arrowIpcToJson(makeIpcBytes(table)) as Array<Record<string, any>>

  t.is(rows.length, 3)
  t.deepEqual(rows[0].tags, { highway: 'primary', name: 'Main St' })
  t.is(rows[1].tags, undefined) // null maps omitted
  t.deepEqual(rows[2].tags, { amenity: 'cafe' })
})

test('decodes List<Int64> as array of numbers', (t) => {
  const NDS_TYPE = new List(new Field('item', new Int64()))

  const ndsArr = [[1n, 2n, 3n], null, [10n, 20n]]

  const table = new Table({
    id: vectorFromArray(Int32Array.from([1, 2, 3])),
    nds: vectorFromArray(ndsArr, NDS_TYPE),
  })

  const rows = arrowIpcToJson(makeIpcBytes(table)) as Array<Record<string, any>>

  t.is(rows.length, 3)
  t.deepEqual(rows[0].nds, [1, 2, 3])
  t.is(rows[1].nds, undefined) // null list omitted
  t.deepEqual(rows[2].nds, [10, 20])
})

test('decodes nested Struct in List', (t) => {
  const MEMBERS_TYPE = new List(
    new Field(
      'item',
      new Struct([new Field('type', new Utf8()), new Field('ref', new Int64()), new Field('role', new Utf8())]),
    ),
  )

  const membersArr = [
    [
      { type: 'node', ref: 100n, role: 'stop' },
      { type: 'way', ref: 200n, role: 'platform' },
    ],
    null,
  ]

  const table = new Table({
    id: vectorFromArray(Int32Array.from([1, 2])),
    members: vectorFromArray(membersArr, MEMBERS_TYPE),
  })

  const rows = arrowIpcToJson(makeIpcBytes(table)) as Array<Record<string, any>>

  t.is(rows.length, 2)
  t.deepEqual(rows[0].members, [
    { type: 'node', ref: 100, role: 'stop' },
    { type: 'way', ref: 200, role: 'platform' },
  ])
  t.is(rows[1].members, undefined) // null list omitted
})

test('handles empty table', (t) => {
  const table = new Table({
    id: vectorFromArray(Int32Array.from([])),
  })

  const rows = arrowIpcToJson(makeIpcBytes(table))

  t.is(rows.length, 0)
})
