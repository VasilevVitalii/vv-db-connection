import { test } from 'node:test'
import assert from 'node:assert/strict'
import oracledb from 'oracledb'
import { DbConnection } from '../src/index'
import { oraBind } from '../src/db/adapters/ora'
import { loadConnections, reachable, runDbSuite, connHost, connPort } from './helpers'

const conns = await loadConnections()
const conn = conns?.ora ?? null
const skip = conn
	? ((await reachable(connHost(conn), connPort(conn))) ? false : 'ora: host unreachable')
	: 'ora: .connect.json missing'

runDbSuite('ora', conn, 'SELECT :v AS v FROM dual', {
	create: 'CREATE TABLE vv_ra_test (id NUMBER, name VARCHAR2(20))',
	insert: "INSERT INTO vv_ra_test (id, name) VALUES (1, 'a')",
	update: "UPDATE vv_ra_test SET name = 'x' WHERE id = 1",
	del: 'DELETE FROM vv_ra_test WHERE id = 1',
	drop: 'DROP TABLE vv_ra_test',
}, skip)

// review2 2.9: a SELECT hidden behind a leading comment used to be misrouted to
// execute(), which discards rows - the same query returned an empty result silently.
test('ora: SELECT behind a leading comment still returns rows', { skip }, async () => {
	const db = new DbConnection(conn!)
	try {
		const r = await db.exec('/* leading comment */ SELECT :v AS v FROM dual', [{ name: 'v', value: 'hello' }])
		assert.equal(r.ok, true, r.ok ? '' : r.error.message)
		if (r.ok) {
			assert.equal(r.result.length, 1)
			assert.equal(Object.values(r.result[0]!.rows[0] as Record<string, unknown>)[0], 'hello')
		}
	} finally {
		await db.close()
	}
})

// review2 §3 (group A): TDbParam.type pins the oracledb bind type instead of letting the
// driver infer it from the JS value. No live database needed.
test('ora: a typed param builds a BIND_IN bind parameter', () => {
	const b = oraBind({ name: 'x', value: 42, type: 'NUMBER' }) as { dir: number; type: unknown; val: unknown }
	assert.equal(b.dir, oracledb.BIND_IN)
	assert.ok(b.type)
	assert.equal(b.val, 42)
})

test('ora: an untyped param passes the raw value through', () => {
	assert.equal(oraBind({ name: 'x', value: 'abc' }), 'abc')
})

test('ora: an unknown type name throws', () => {
	assert.throws(() => oraBind({ name: 'x', value: 1, type: 'NoSuch' }), /unknown param type: NoSuch/)
})

// review2 §3 (group A): out/inout binds. oracledb needs a type to allocate the out
// buffer, so a direction out/inout without `type` is rejected client-side.
test('ora: an out param without a type throws', () => {
	assert.throws(() => oraBind({ name: 'x', value: 42, direction: 'out' }), /out parameter x needs a type/)
})

test('ora: an out param builds a BIND_OUT bind with no in value', () => {
	const b = oraBind({ name: 'x', value: 42, type: 'NUMBER', direction: 'out' }) as { dir: number; type: unknown; val: unknown }
	assert.equal(b.dir, oracledb.BIND_OUT)
	assert.ok(b.type)
	assert.equal(b.val, undefined)
})

test('ora: an inout param keeps its value and sets BIND_INOUT', () => {
	const b = oraBind({ name: 'x', value: 42, type: 'NUMBER', direction: 'inout' }) as { dir: number; type: unknown; val: unknown }
	assert.equal(b.dir, oracledb.BIND_INOUT)
	assert.equal(b.val, 42)
})

// review2 §3 (group A): out/inout binds against a live Oracle - the value comes back in
// TDbRes.outParams via oracledb outBinds.
test('ora: an OUT parameter returns its value in outParams', { skip }, async () => {
	const db = new DbConnection(conn!)
	try {
		const r = await db.exec('BEGIN :n := 42; END;', [{ name: 'n', direction: 'out', type: 'NUMBER' }])
		assert.equal(r.ok, true, r.ok ? '' : r.error.message)
		if (!r.ok) return
		assert.equal(r.result[0]!.outParams!.n, 42)
	} finally {
		await db.close()
	}
})

test('ora: an INOUT parameter keeps its input and returns the updated value', { skip }, async () => {
	const db = new DbConnection(conn!)
	try {
		const r = await db.exec('BEGIN :n := :n * 2; END;', [{ name: 'n', direction: 'inout', type: 'NUMBER', value: 21 }])
		assert.equal(r.ok, true, r.ok ? '' : r.error.message)
		if (!r.ok) return
		assert.equal(r.result[0]!.outParams!.n, 42)
	} finally {
		await db.close()
	}
})
