import { test } from 'node:test'
import assert from 'node:assert/strict'
import { DbConnection } from '../src/index'
import { toSchema } from '../src/db/adapters/pg'
import { loadConnections, reachable, runDbSuite, connHost, connPort } from './helpers'

const conns = await loadConnections()
const conn = conns?.pg ?? null
const skip = conn
	? ((await reachable(connHost(conn), connPort(conn))) ? false : 'pg: host unreachable')
	: 'pg: .connect.json missing'

runDbSuite('pg', conn, 'SELECT :v AS v', {
	create: 'CREATE TABLE vv_ra_test (id INT, name VARCHAR(20))',
	insert: "INSERT INTO vv_ra_test (id, name) VALUES (1, 'a')",
	update: "UPDATE vv_ra_test SET name = 'x' WHERE id = 1",
	del: 'DELETE FROM vv_ra_test WHERE id = 1',
	drop: 'DROP TABLE vv_ra_test',
}, skip)

// A colon inside a string literal is text, not a parameter: the query must come back
// verbatim, not with the literal rewritten to '12@30'.
test('pg: colon inside a string literal stays literal', { skip }, async (t) => {
	const db = new DbConnection(conn!)
	t.after(async () => { await db.close() })
	const r = await db.exec("SELECT '12:30' AS t, :v AS v", [{ name: 'v', value: 'hello' }])
	assert.equal(r.ok, true, r.ok ? '' : r.error.message)
	if (!r.ok) return
	const row = r.result[0]!.rows[0] as Record<string, unknown>
	assert.equal(row.t, '12:30')
	assert.equal(row.v, 'hello')
})

// review2 §3 (group D): kind used to be the raw numeric OID ('23'). It must be a
// readable type name ('int4', 'text', ...) mapped from the RowDescription typeOid.
test('pg: kind maps OIDs to readable type names', { skip }, async (t) => {
	const db = new DbConnection(conn!)
	t.after(async () => { await db.close() })
	const r = await db.exec("SELECT 1::int4 AS i, 'x'::text AS t, 1.5::numeric AS n, now()::timestamptz AS ts", [])
	assert.equal(r.ok, true, r.ok ? '' : r.error.message)
	if (!r.ok) return
	const kindByName = Object.fromEntries(r.result[0]!.schema.map((c) => [c.name, c.kind]))
	assert.equal(kindByName.i, 'int4')
	assert.equal(kindByName.t, 'text')
	assert.equal(kindByName.n, 'numeric')
	assert.equal(kindByName.ts, 'timestamptz')
})

// review2 4.9: the schema carried only the type name; the declared length/precision was
// dropped. dataTypeModifier (the catalog's atttypmod) encodes it: varchar(n)/char(n) →
// len, numeric(p,s) → precision/scale. Unit test against the FieldDef shape, no live DB.
test('pg: toSchema decodes length and precision from dataTypeModifier', () => {
	const schema = toSchema([
		{ name: 'v', tableID: 0, columnID: 0, dataTypeID: 1043, dataTypeSize: -1, dataTypeModifier: 20 + 4, format: 'text' },
		{ name: 'n', tableID: 0, columnID: 0, dataTypeID: 1700, dataTypeSize: -1, dataTypeModifier: ((10 << 16) | 2) + 4, format: 'text' },
		{ name: 't', tableID: 0, columnID: 0, dataTypeID: 25, dataTypeSize: -1, dataTypeModifier: -1, format: 'text' },
	])
	assert.equal(schema[0]!.kind, 'varchar')
	assert.equal(schema[0]!.len, 20)
	assert.equal(schema[1]!.kind, 'numeric')
	assert.equal(schema[1]!.precision, 10)
	assert.equal(schema[1]!.scale, 2)
	assert.equal(schema[2]!.kind, 'text')
	assert.equal(schema[2]!.len, undefined)
})
