import { test } from 'node:test'
import assert from 'node:assert/strict'
import oracledb from 'oracledb'
import { DbConnection } from '../src/index'
import { OraAdapter, oraBind, buildQuery, isRowQuery } from '../src/db/adapters/ora'
import type { TConnOra } from '../src/db/connection'
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
test('ora: SELECT behind a leading comment still returns rows', { skip }, async (t) => {
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
test('ora: a typed param builds a BIND_IN bind parameter', async (t) => {
	const b = oraBind({ name: 'x', value: 42, type: 'NUMBER' }) as { dir: number; type: unknown; val: unknown }
	assert.equal(b.dir, oracledb.BIND_IN)
	assert.ok(b.type)
	assert.equal(b.val, 42)
})

test('ora: an untyped param passes the raw value through', async (t) => {
	assert.equal(oraBind({ name: 'x', value: 'abc' }), 'abc')
})

test('ora: an unknown type name throws', async (t) => {
	assert.throws(() => oraBind({ name: 'x', value: 1, type: 'NoSuch' }), /unknown param type: NoSuch/)
})

// review2 §3 (group A): out/inout binds. oracledb needs a type to allocate the out
// buffer, so a direction out/inout without `type` is rejected client-side.
test('ora: an out param without a type throws', async (t) => {
	assert.throws(() => oraBind({ name: 'x', value: 42, direction: 'out' }), /out parameter x needs a type/)
})

test('ora: an out param builds a BIND_OUT bind with no in value', async (t) => {
	const b = oraBind({ name: 'x', value: 42, type: 'NUMBER', direction: 'out' }) as { dir: number; type: unknown; val: unknown }
	assert.equal(b.dir, oracledb.BIND_OUT)
	assert.ok(b.type)
	assert.equal(b.val, undefined)
})

test('ora: an inout param keeps its value and sets BIND_INOUT', async (t) => {
	const b = oraBind({ name: 'x', value: 42, type: 'NUMBER', direction: 'inout' }) as { dir: number; type: unknown; val: unknown }
	assert.equal(b.dir, oracledb.BIND_INOUT)
	assert.equal(b.val, 42)
})

// review2 §3 (group A): out/inout binds against a live Oracle - the value comes back in
// TDbRes.outParams via oracledb outBinds.
test('ora: an OUT parameter returns its value in outParams', { skip }, async (t) => {
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

test('ora: an INOUT parameter keeps its input and returns the updated value', { skip }, async (t) => {
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

// review2 §4 (group C): :new/:old are trigger correlation names, not bind placeholders.
// A CREATE TRIGGER using them must run without a "param not found: new" error, and the
// trigger must actually fire.
test('ora: CREATE TRIGGER with :new/:old correlation names runs', { skip }, async (t) => {
	const db = new DbConnection(conn!)
	try {
		const created = await db.exec('CREATE TABLE vv_ra_trg (id NUMBER, name VARCHAR2(20))', [])
		assert.equal(created.ok, true, created.ok ? '' : created.error.message)
		const trig = await db.exec('CREATE OR REPLACE TRIGGER vv_ra_trg BEFORE INSERT ON vv_ra_trg FOR EACH ROW BEGIN :new.name := UPPER(:new.name); END;', [])
		assert.equal(trig.ok, true, trig.ok ? '' : trig.error.message)
		const ins = await db.exec('INSERT INTO vv_ra_trg (id, name) VALUES (1, :name)', [{ name: 'name', value: 'lower' }])
		assert.equal(ins.ok, true, ins.ok ? '' : ins.error.message)
		const sel = await db.exec('SELECT name FROM vv_ra_trg WHERE id = 1', [])
		assert.equal(sel.ok, true, sel.ok ? '' : sel.error.message)
		if (sel.ok) assert.equal(Object.values(sel.result[0]!.rows[0] as Record<string, unknown>)[0], 'LOWER')
	} finally {
		await db.exec('DROP TABLE vv_ra_trg', [])
		await db.exec('DROP TRIGGER vv_ra_trg', [])
		await db.close()
	}
})

// review3 1.2: on abort, break() interrupts the statement running on the server; close()
// must wait for it - a close() while a call is in flight answers NJS-032 and the
// connection is not returned to the pool, so its slot leaks. The old code fired break()
// and close() in parallel and swallowed the NJS-032 from the raced close(). The fake
// connection below reproduces that: close() before break() settles rejects with NJS-032.
test('ora: abort waits for break() before close(), returning the slot', async (t) => {
	let breakDone = false
	let breakResolve!: () => void
	let closed = 0
	const fakeConn = {
		break: () => new Promise<void>((resolve) => { breakResolve = () => { breakDone = true; resolve() } }),
		close: () => {
			if (!breakDone) return Promise.reject(Object.assign(new Error('NJS-032: connection cannot be released'), { code: 'NJS-032' }))
			closed += 1
			return Promise.resolve()
		},
		execute: () => new Promise(() => {}), // a stuck DML: never settles
		queryStream: () => { throw new Error('not a row query') },
	}
	t.mock.method(oracledb, 'createPool', () => Promise.resolve({ getConnection: async () => fakeConn, close: async () => {} }))

	const adapter = new OraAdapter({ kind: 'ora', host: '127.0.0.1', port: 1521, service: 'x', user: 'u', password: 'p' } as TConnOra)
	const controller = new AbortController()
	const run = adapter.execStream('UPDATE t SET x = 1', [], () => {}, controller.signal)
	await new Promise((resolve) => setImmediate(resolve)) // let execute() start (stuck)
	controller.abort()
	await new Promise((resolve) => setImmediate(resolve)) // onAbort ran, finally awaits break()
	breakResolve()
	await run

	assert.equal(closed, 1, 'the connection must be returned to the pool')
})

// The same NJS-032 race on the transaction path: a mid-transaction abort tears the
// connection down while a statement is still running on the server.
test('ora: execTxDynamic closes only after break() lands on a mid-transaction abort', async (t) => {
	let breakDone = false
	let breakResolve!: () => void
	let closed = 0
	const fakeConn = {
		break: () => new Promise<void>((resolve) => { breakResolve = () => { breakDone = true; resolve() } }),
		close: () => {
			if (!breakDone) return Promise.reject(Object.assign(new Error('NJS-032: connection cannot be released'), { code: 'NJS-032' }))
			closed += 1
			return Promise.resolve()
		},
		execute: () => new Promise(() => {}), // stuck; break() would end it on a real server
		rollback: () => Promise.resolve(),
		commit: () => Promise.resolve(),
	}
	t.mock.method(oracledb, 'createPool', () => Promise.resolve({ getConnection: async () => fakeConn, close: async () => {} }))

	const adapter = new OraAdapter({ kind: 'ora', host: '127.0.0.1', port: 1521, service: 'x', user: 'u', password: 'p' } as TConnOra)
	const p = adapter.execTxDynamic(async (exec) => {
		await exec('SELECT 1 FROM dual', [])
	}, 50)
	const r = await p
	assert.equal(r.ok, false)
	if (!r.ok) assert.equal(r.error.kind, 'timeout')

	// Let the abort handler run, then let break() land so close() may proceed.
	await new Promise((resolve) => setImmediate(resolve))
	breakResolve()
	await new Promise((resolve) => setImmediate(resolve))

	assert.equal(closed, 1)
})

// review3 §2.6: :new/:old are trigger correlation names only inside a CREATE [OR REPLACE]
// TRIGGER, in any case. Outside a trigger they are ordinary bind names, so :old/:new must
// still bind instead of being silently skipped (the server would answer ORA-01008).
test('ora: :NEW/:OLD are correlation names only inside CREATE TRIGGER', async () => {
	const t = buildQuery('CREATE TRIGGER trg BEFORE INSERT ON x FOR EACH ROW BEGIN :NEW.name := :OLD.name; END;', [])
	assert.equal(t.sql, 'CREATE TRIGGER trg BEFORE INSERT ON x FOR EACH ROW BEGIN :NEW.name := :OLD.name; END;')
	assert.deepEqual(t.binds, {})
	const u = buildQuery('UPDATE t SET ver = ver + 1 WHERE ver = :old', [{ name: 'old', value: 5 }])
	assert.deepEqual(u.binds, { old: 5 })
	const v = buildQuery('SELECT :new AS x FROM dual', [{ name: 'new', value: 1 }])
	assert.ok('new' in v.binds)
})

// review3 §2.7: connection.ts documents queueTimeout 0 as "never", but the pool dropped it
// and fell back to the driver default. It must be passed through verbatim.
test('ora: queueTimeout 0 is passed through to createPool', async (t) => {
	let opts: { queueTimeout?: number } = {}
	const m = t.mock.method(oracledb, 'createPool', (o: { queueTimeout?: number }) => {
		opts = o
		return Promise.resolve({ getConnection: async () => { throw new Error('unreachable') }, close: async () => {} })
	})
	try {
		const adapter = new OraAdapter({ kind: 'ora', host: 'h', port: 1521, service: 's', user: 'u', password: 'p', queueTimeout: 0 } as TConnOra)
		await adapter.execStream('SELECT 1 FROM dual', [], () => {})
	} finally {
		m.mock.restore()
	}
	assert.equal(opts.queueTimeout, 0)
})

// review3 §2.8: a parenthesised subquery still returns rows; it must route to queryStream,
// not execute(), which discards the rows.
test('ora: a parenthesised SELECT or WITH is a row query', async (t) => {
	assert.equal(isRowQuery('(SELECT 1 FROM dual)'), true)
	assert.equal(isRowQuery('( WITH x AS (SELECT 1 AS a) SELECT a FROM x )'), true)
	assert.equal(isRowQuery('( UPDATE t SET x = 1 )'), false)
	assert.equal(isRowQuery('BEGIN :n := 1; END;'), false)
})

// review3 §2.8: buildQuery runs before getConnection(), so a "param not found" never holds
// a pool slot the way it used to when validation happened after the connection was taken.
test('ora: param not found fails before acquiring a connection', async (t) => {
	let acquired = 0
	const m = t.mock.method(oracledb, 'createPool', () => Promise.resolve({
		getConnection: async () => { acquired += 1; throw new Error('unreachable') },
		close: async () => {},
	}))
	try {
		const adapter = new OraAdapter({ kind: 'ora', host: 'h', port: 1521, service: 's', user: 'u', password: 'p' } as TConnOra)
		await assert.rejects(adapter.execStream('SELECT :missing FROM dual', [], () => {}), /param not found: missing/)
	} finally {
		m.mock.restore()
	}
	assert.equal(acquired, 0)
})
