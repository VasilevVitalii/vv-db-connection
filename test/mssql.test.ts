import { test } from 'node:test'
import assert from 'node:assert/strict'
import sql from 'mssql'
import { DbConnection } from '../src/index'
import { toSchema, mssqlType, mssqlInput, MssqlAdapter } from '../src/db/adapters/mssql'
import { loadConnections, reachable, runDbSuite, connHost, connPort } from './helpers'

const conns = await loadConnections()
const conn = conns?.mssql ?? null
const skip = conn
	? ((await reachable(connHost(conn), connPort(conn))) ? false : 'mssql: host unreachable')
	: 'mssql: .connect.json missing'

runDbSuite('mssql', conn, 'SELECT :v AS v', {
	create: 'CREATE TABLE vv_ra_test (id INT, name VARCHAR(20))',
	insert: "INSERT INTO vv_ra_test (id, name) VALUES (1, 'a')",
	update: "UPDATE vv_ra_test SET name = 'x' WHERE id = 1",
	del: 'DELETE FROM vv_ra_test WHERE id = 1',
	drop: 'DROP TABLE vv_ra_test',
}, skip)

// A colon inside a string literal is text, not a parameter: the query must come back
// verbatim, not with the literal rewritten to '12@30'.
test('mssql: colon inside a string literal stays literal', { skip }, async (t) => {
	const db = new DbConnection(conn!)
	t.after(async () => { await db.close() })
	const r = await db.exec("SELECT '12:30' AS t, :v AS v", [{ name: 'v', value: 'hello' }])
	assert.equal(r.ok, true, r.ok ? '' : r.error.message)
	if (!r.ok) return
	const row = r.result[0]!.rows[0] as Record<string, unknown>
	assert.equal(row.t, '12:30')
	assert.equal(row.v, 'hello')
})

// review2 §3: mssql's recordset column carries the type constructor (sql.Int, ...),
// whose .name is the type name. The old code called c.type(), which builds a
// declaration object ({ type }) with no .name, so every column reported kind 'unknown'.
// Regression test against the real shape, no live database needed.
test('mssql: toSchema reads the type name from the constructor, not c.type()', () => {
	const Int = Object.defineProperty(function () { /* sql.Int constructor */ }, 'name', { value: 'Int' })
	const schema = toSchema({ id: { name: 'id', type: Int, length: 4, scale: undefined, precision: 10 } })
	assert.equal(schema[0]!.kind, 'Int')
	assert.equal(schema[0]!.name, 'id')
	assert.equal(schema[0]!.precision, 10)
})

// review2 §3 (group A): TDbParam.type forces a driver-native type instead of letting
// mssql infer it from the JS value (a Date becomes DateTime, losing sub-second precision;
// a long string becomes NVarChar). No live database needed.
test('mssql: mssqlType resolves a type name to its factory', () => {
	assert.equal(mssqlType('DateTime2').name, 'DateTime2')
	assert.equal(mssqlType('NVarChar').name, 'NVarChar')
})

test('mssql: mssqlType throws on an unknown type name', () => {
	assert.throws(() => mssqlType('NoSuchType'), /unknown param type: NoSuchType/)
})

test('mssql: a typed param passes the type as the second input argument', () => {
	const calls: [string, unknown, unknown][] = []
	const request = {
		input(name: string, a?: unknown, b?: unknown): void { calls.push([name, a, b]) },
	} as any
	mssqlInput(request, { name: 'ts', value: new Date(), type: 'DateTime2' })
	mssqlInput(request, { name: 's', value: 'hello' })
	assert.equal(calls.length, 2)
	assert.equal(calls[0]![0], 'ts')
	assert.equal((calls[0]![1] as { name: string }).name, 'DateTime2')
	assert.ok(calls[0]![2] instanceof Date)
	assert.equal(calls[1]![0], 's')
	assert.equal(calls[1]![1], 'hello')
	assert.equal(calls[1]![2], undefined)
})

// review2 §3 (group A): out/inout binds are Oracle-only; the other adapters reject the
// direction rather than silently binding an undefined value and inserting NULL.
test('mssql: an out param is rejected', () => {
	const request = { input(): void {} } as any
	assert.throws(
		() => mssqlInput(request, { name: 'x', value: 1, type: 'Int', direction: 'out' }),
		/mssql: out parameters are not supported/,
	)
})

// review2 4.4: pool() must keep one ConnectionPool across calls, and a failed connect()
// must not strand it - the next call reconnects on the same pool. mssql's ConnectionPool
// serializes concurrent connect() calls on an internal stack, so there is no create race
// here (unlike the ora pool). This pins the adapter's own behaviour: one pool, and
// connect() is tried again after a rejection.
test('mssql: pool() keeps one ConnectionPool and reconnects after a failed connect', async () => {
	const orig = sql.ConnectionPool
	let created = 0
	let connects = 0
	class FakePool {
		constructor() { created += 1 }
		on() { /* swallow the pool-level error re-emit */ }
		connect() {
			connects += 1
			return connects === 1 ? Promise.reject(new Error('login failed')) : Promise.resolve(this)
		}
		close() { return Promise.resolve() }
	}
	try {
		;(sql as any).ConnectionPool = FakePool
		const adapter = new MssqlAdapter({ kind: 'mssql', server: 'x', auth: { kind: 'mssql', user: 'u', password: 'p' } })
		await assert.rejects(() => (adapter as any).pool(), /login failed/)
		const pool = await (adapter as any).pool()
		assert.ok(pool instanceof FakePool)
		assert.equal(created, 1)
		assert.equal(connects, 2)
	} finally {
		;(sql as any).ConnectionPool = orig
	}
})
