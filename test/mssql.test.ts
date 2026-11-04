import { test } from 'node:test'
import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import sql from 'mssql'
import { DbConnection } from '../src/index'
import { toSchema, mssqlType, mssqlInput, MssqlAdapter, buildPoolConfig } from '../src/db/adapters/mssql'
import type { TConnMssql } from '../src/db/connection'
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

// review3 §2.5: rowsAffected is one entry per statement. The old code read only [0] and
// only when no recordset had been seen, so INSERT; UPDATE reported a single count and
// SELECT; UPDATE dropped the DML count entirely. Each DML statement must be its own result.
test('mssql: a multi-statement DML batch reports one result per statement', async () => {
	class FakeRequest extends EventEmitter {
		stream = false
		input(): void {}
		query(): void {
			setImmediate(() => {
				this.emit('rowsaffected', 5)
				this.emit('rowsaffected', 7)
				this.emit('done', { rowsAffected: [5, 7] })
			})
		}
	}
	const req = new FakeRequest()
	const pool = { request: () => req }
	const adapter = new MssqlAdapter({ kind: 'mssql', server: 'x', auth: { kind: 'mssql', user: 'u', password: 'p' } })
	;(adapter as unknown as { _pool: unknown })._pool = { connect: async () => pool }
	const events: { kind: string; count?: number }[] = []
	await adapter.execStream('INSERT INTO a VALUES (1); UPDATE b SET x = 2', [], (e) => events.push(e))
	const affected = events.filter((e) => e.kind === 'rowsAffected').map((e) => e.count)
	assert.deepEqual(affected, [5, 7])
	assert.equal(events.filter((e) => e.kind === 'schema').length, 2)
})

// review3 §2.5 (mixed batch): a SELECT entry in rowsAffected is its returned-row count
// (number), not undefined; the row events already emitted it, so only the trailing UPDATE
// count becomes a separate DML result.
test('mssql: a mixed SELECT; UPDATE batch keeps the DML result', async () => {
	class FakeRequest extends EventEmitter {
		stream = false
		input(): void {}
		query(): void {
			setImmediate(() => {
				this.emit('recordset', { v: { name: 'v', type: sql.Int, length: 4, scale: undefined, precision: 10 } })
				this.emit('row', { v: 1 })
				this.emit('rowsaffected', 1) // SELECT: returned 1 row
				this.emit('rowsaffected', 3) // UPDATE: affected 3
				this.emit('done', { rowsAffected: [1, 3] })
			})
		}
	}
	const req = new FakeRequest()
	const pool = { request: () => req }
	const adapter = new MssqlAdapter({ kind: 'mssql', server: 'x', auth: { kind: 'mssql', user: 'u', password: 'p' } })
	;(adapter as unknown as { _pool: unknown })._pool = { connect: async () => pool }
	const events: { kind: string; count?: number }[] = []
	await adapter.execStream('SELECT v FROM t; UPDATE b SET x = 2', [], (e) => events.push(e))
	const affected = events.filter((e) => e.kind === 'rowsAffected').map((e) => e.count)
	assert.deepEqual(affected, [3])
})

// review5 §2.3: under SET NOCOUNT ON a SELECT emits no rowsaffected, so a following
// DML's count arrives while inSelect is still open. It must surface as a DML result,
// not be eaten as the SELECT's own (suppressed) row count.
test('mssql: a NOCOUNT-suppressed SELECT does not eat the following DML count', async () => {
	class FakeRequest extends EventEmitter {
		stream = false
		input(): void {}
		query(): void {
			setImmediate(() => {
				this.emit('recordset', { v: { name: 'v', type: sql.Int, length: 4, scale: undefined, precision: 10 } })
				this.emit('row', { v: 1 })
				// SELECT's rowsaffected suppressed by SET NOCOUNT ON — only the UPDATE emits one
				this.emit('rowsaffected', 3)
				this.emit('done', { rowsAffected: [3] })
			})
		}
	}
	const req = new FakeRequest()
	const pool = { request: () => req }
	const adapter = new MssqlAdapter({ kind: 'mssql', server: 'x', auth: { kind: 'mssql', user: 'u', password: 'p' } })
	;(adapter as unknown as { _pool: unknown })._pool = { connect: async () => pool }
	const events: { kind: string; count?: number }[] = []
	await adapter.execStream('SET NOCOUNT ON; SELECT v FROM t; SET NOCOUNT OFF; UPDATE b SET x = 2', [], (e) => events.push(e))
	const affected = events.filter((e) => e.kind === 'rowsAffected').map((e) => e.count)
	assert.deepEqual(affected, [3])
})

// review3 §2.5: a lone SELECT must not emit a spurious empty-schema DML result - its
// rowsAffected entry is the returned-row count and the rows already went out as 'row'.
test('mssql: a lone SELECT emits no DML result', async () => {
	class FakeRequest extends EventEmitter {
		stream = false
		input(): void {}
		query(): void {
			setImmediate(() => {
				this.emit('recordset', { v: { name: 'v', type: sql.Int, length: 4, scale: undefined, precision: 10 } })
				this.emit('row', { v: 1 })
				this.emit('rowsaffected', 1) // SELECT: returned 1 row
				this.emit('done', { rowsAffected: [1] })
			})
		}
	}
	const req = new FakeRequest()
	const pool = { request: () => req }
	const adapter = new MssqlAdapter({ kind: 'mssql', server: 'x', auth: { kind: 'mssql', user: 'u', password: 'p' } })
	;(adapter as unknown as { _pool: unknown })._pool = { connect: async () => pool }
	const events: { kind: string; count?: number }[] = []
	await adapter.execStream('SELECT v FROM t', [], (e) => events.push(e))
	assert.deepEqual(events.map((e) => e.kind), ['schema', 'row', 'finish'])
})

// review4 §2.1: the old code matched `done.rowsAffected` entries to SELECT row counts by
// value, so an UPDATE whose affected count equalled a SELECT's returned-row count was
// dropped (or a DML that ran before a SELECT was misattributed to it). The per-statement
// `rowsaffected` order keeps both, in statement order.
test('mssql: UPDATE; SELECT with equal counts keeps both results in order', async () => {
	class FakeRequest extends EventEmitter {
		stream = false
		input(): void {}
		query(): void {
			setImmediate(() => {
				this.emit('rowsaffected', 2) // UPDATE: affected 2, no recordset
				this.emit('recordset', { v: { name: 'v', type: sql.Int, length: 4, scale: undefined, precision: 10 } })
				this.emit('row', { v: 1 })
				this.emit('row', { v: 2 })
				this.emit('rowsaffected', 2) // SELECT: returned 2 rows
				this.emit('done', { rowsAffected: [2, 2] })
			})
		}
	}
	const req = new FakeRequest()
	const pool = { request: () => req }
	const adapter = new MssqlAdapter({ kind: 'mssql', server: 'x', auth: { kind: 'mssql', user: 'u', password: 'p' } })
	;(adapter as unknown as { _pool: unknown })._pool = { connect: async () => pool }
	const events: { kind: string; count?: number }[] = []
	await adapter.execStream('UPDATE b SET x = 2; SELECT v FROM t', [], (e) => events.push(e))
	const affected = events.filter((e) => e.kind === 'rowsAffected').map((e) => e.count)
	assert.deepEqual(affected, [2])
	assert.deepEqual(events.map((e) => e.kind), ['schema', 'rowsAffected', 'schema', 'row', 'row', 'finish'])
})

// review4 §2.1: DML results must be emitted in statement order, not bunched after every
// SELECT at `done`. SELECT; UPDATE; SELECT emits the DML count between the two SELECTs.
test('mssql: SELECT; UPDATE; SELECT keeps the DML result in statement order', async () => {
	class FakeRequest extends EventEmitter {
		stream = false
		input(): void {}
		query(): void {
			setImmediate(() => {
				this.emit('recordset', { v: { name: 'v', type: sql.Int, length: 4, scale: undefined, precision: 10 } })
				this.emit('row', { v: 1 })
				this.emit('rowsaffected', 1) // SELECT 1
				this.emit('rowsaffected', 2) // UPDATE
				this.emit('recordset', { v: { name: 'v', type: sql.Int, length: 4, scale: undefined, precision: 10 } })
				this.emit('row', { v: 9 })
				this.emit('rowsaffected', 1) // SELECT 2
				this.emit('done', { rowsAffected: [1, 2, 1] })
			})
		}
	}
	const req = new FakeRequest()
	const pool = { request: () => req }
	const adapter = new MssqlAdapter({ kind: 'mssql', server: 'x', auth: { kind: 'mssql', user: 'u', password: 'p' } })
	;(adapter as unknown as { _pool: unknown })._pool = { connect: async () => pool }
	const events: { kind: string; count?: number }[] = []
	await adapter.execStream('SELECT v FROM t; UPDATE b SET x = 2; SELECT v FROM t', [], (e) => events.push(e))
	const affected = events.filter((e) => e.kind === 'rowsAffected').map((e) => e.count)
	assert.deepEqual(affected, [2])
	assert.deepEqual(events.map((e) => e.kind), ['schema', 'row', 'schema', 'rowsAffected', 'schema', 'row', 'finish'])
})

// review3 §2.12: a bad param (missing, out, unknown type) must fail before connecting, so
// it never holds a pool slot nor leaves an abort listener dangling on an unsent request.
test('mssql: param not found fails before connecting', async () => {
	let connected = 0
	const adapter = new MssqlAdapter({ kind: 'mssql', server: 'x', auth: { kind: 'mssql', user: 'u', password: 'p' } })
	;(adapter as unknown as { _pool: unknown })._pool = { connect: async () => { connected += 1; throw new Error('unreachable') } }
	const events: { kind: string; error?: { message?: string } }[] = []
	await adapter.execStream('SELECT :missing AS x', [], (e) => events.push(e))
	assert.equal(connected, 0)
	assert.equal(events.length, 1)
	assert.equal(events[0]!.kind, 'finish')
	assert.match(events[0]!.error?.message ?? '', /param not found: missing/)
})

test('mssql: an out param fails before connecting', async () => {
	let connected = 0
	const adapter = new MssqlAdapter({ kind: 'mssql', server: 'x', auth: { kind: 'mssql', user: 'u', password: 'p' } })
	;(adapter as unknown as { _pool: unknown })._pool = { connect: async () => { connected += 1; throw new Error('unreachable') } }
	const events: { kind: string; error?: { message?: string } }[] = []
	await adapter.execStream('SELECT 1 AS x', [{ name: 'x', value: 1, type: 'Int', direction: 'out' }], (e) => events.push(e))
	assert.equal(connected, 0)
	assert.equal(events.length, 1)
	assert.equal(events[0]!.kind, 'finish')
	assert.match(events[0]!.error?.message ?? '', /mssql: out parameters are not supported/)
})

// review4 §2.2: execStream with an already-aborted signal used to `return` silently after
// connect(), breaking the adapter's contract that every call ends with one finish. A
// direct adapter call must still emit exactly one aborted finish, not zero events.
test('mssql: execStream with an already-aborted signal emits one aborted finish', async () => {
	const adapter = new MssqlAdapter({ kind: 'mssql', server: 'x', auth: { kind: 'mssql', user: 'u', password: 'p' } })
	;(adapter as unknown as { _pool: unknown })._pool = { connect: async () => ({ request: () => ({}) }) }
	const controller = new AbortController()
	controller.abort(new Error('cancelled'))
	const events: { kind: string; error?: { kind?: string; message?: string } }[] = []
	await adapter.execStream('SELECT 1 AS x', [], (e) => events.push(e), controller.signal)
	assert.equal(events.length, 1)
	assert.equal(events[0]!.kind, 'finish')
	assert.equal(events[0]!.error?.kind, 'aborted')
	assert.equal(events[0]!.error?.message, 'cancelled')
})

// review3 §4 (MSSQL): onAbort hard-closes the borrowed tedious connection via
// request._currentRequest.connection.close(). The review left open whether that dead
// connection comes back into tarn or is discarded. tarn validates on acquire and destroys a
// closed connection, so the pool must keep serving. Prove it live: hard-close more than the
// default pool.max (10) connections mid-flight and confirm every slot is released (borrowed
// drops back to 0) and a plain query still works afterwards.
test('mssql: pool survives repeated hard-closes (dead slots are discarded)', { skip }, async (t) => {
	const adapter = new MssqlAdapter(conn as TConnMssql)
	t.after(async () => { await adapter.close() })

	// Streams rows for a while, so aborting on the first 'row' is guaranteed to tear a
	// borrowed connection down mid-flight: by then the request has borrowed a connection and
	// _currentRequest is set, so onAbort takes the hard-close path instead of cancel().
	const q = 'WITH n AS (SELECT 1 AS i UNION ALL SELECT i + 1 FROM n WHERE i < 100000) SELECT i FROM n OPTION (MAXRECURSION 0)'

	const borrowed = () => (adapter as unknown as { _pool?: { borrowed?: number } })._pool?.borrowed

	for (let round = 0; round < 12; round++) {
		const controller = new AbortController()
		let torn = false
		const run = adapter.execStream(q, [], (e) => {
			if (e.kind === 'row' && !torn) {
				torn = true
				controller.abort()
			}
		}, controller.signal)
		await run

		// The borrowed slot must be released back (then destroyed by validation) rather than
		// held forever. Poll so the rounds do not pile up and deadlock each other.
		const start = Date.now()
		while (borrowed() !== 0) {
			if (Date.now() - start > 10_000) {
				throw new Error(`round ${round}: the hard-closed slot was never released`)
			}
			await new Promise((r) => setTimeout(r, 20))
		}
	}

	// After 12 hard-closes (more than pool.max = 10) the pool must still hand out a live
	// connection - proof the dead connections were discarded, not recycled into tarn.
	const out: { row?: unknown; error?: string } = {}
	await adapter.execStream('SELECT 1 AS p', [], (e) => {
		if (e.kind === 'row') out.row = e.row
		if (e.kind === 'finish' && e.error) out.error = e.error.message
	})
	assert.equal(out.error, undefined, `query after hard-closes failed: ${out.error}`)
	assert.deepEqual(out.row, { p: 1 })
})

// review9 §4: mssql teardown relies on the private Transaction._acquiredConnection.socket
// to hard-break a stuck statement (pool.close() alone is graceful - it waits for the busy
// connection). If a future mssql upgrade renames/removes it, an abort silently degrades
// into a commit after the deadline (review9 §1.1). Pin the shape against the installed
// driver so an upgrade breaks this test loudly instead of silently.
test('mssql: a begun transaction exposes _acquiredConnection.socket for teardown', { skip }, async (t) => {
	const pool = await new sql.ConnectionPool(buildPoolConfig(conn as TConnMssql)).connect()
	t.after(async () => { await pool.close() })
	const tx = new sql.Transaction(pool)
	await tx.begin()
	try {
		const acquired = (tx as unknown as { _acquiredConnection?: { socket?: { destroy(): void } } })._acquiredConnection
		assert.ok(acquired, 'mssql Transaction must expose _acquiredConnection (teardown depends on it)')
		assert.ok(acquired?.socket, '_acquiredConnection must expose a socket to destroy')
		assert.equal(typeof acquired?.socket?.destroy, 'function')
	} finally {
		await tx.rollback()
	}
})
