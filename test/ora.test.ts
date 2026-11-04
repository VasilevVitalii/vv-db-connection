import { test } from 'node:test'
import assert from 'node:assert/strict'
import oracledb from 'oracledb'
import { EventEmitter } from 'node:events'
import { DbConnection } from '../src/index'
import type { TDbTxResult } from '../src/index'
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

// review4 §2.3: an abort that lands while commit() is in flight tears the connection down
// in the abort handler (break + close). The commit catch used to roll back unconditionally,
// so rollback() raced the handler's close() on the same connection. The fix only rolls back
// a real commit failure (signal not aborted) and leaves the aborted path to the teardown.
test('ora: an abort during commit skips rollback (no race with the closing connection)', async (t) => {
	let breakResolve!: () => void
	let commitReject!: (err: Error) => void
	let rollbackCalls = 0
	let closed = 0
	const controller = new AbortController()
	const fakeConn = {
		// break() interrupts the in-flight commit on the server, which rejects it.
		break: () => {
			commitReject(Object.assign(new Error('ORA-01013: user requested cancel'), { code: 'NJS-005' }))
			return new Promise<void>((resolve) => { breakResolve = resolve })
		},
		close: () => { closed += 1; return Promise.resolve() },
		execute: async () => ({ rowsAffected: 0, implicitResults: undefined, outBinds: undefined }),
		rollback: () => { rollbackCalls += 1; return Promise.resolve() },
		commit: () => new Promise<void>((_res, rej) => {
			commitReject = rej
			// The commit is now pending; abort while it is in flight. (The deadline is
			// cleared before commit, so an external abort is what interrupts it now.)
			controller.abort()
		}),
	}
	t.mock.method(oracledb, 'createPool', () => Promise.resolve({ getConnection: async () => fakeConn, close: async () => {} }))

	const adapter = new OraAdapter({ kind: 'ora', host: '127.0.0.1', port: 1521, service: 'x', user: 'u', password: 'p' } as TConnOra)
	// The callback succeeds immediately, so the transaction reaches commit(); the
	// external abort then fires while commit() is still pending.
	const r = await adapter.execTxDynamic(async () => {}, undefined, controller.signal)
	assert.equal(r.ok, false)

	// Let the abort handler finish its teardown (release() awaits break(), then closes).
	await new Promise((resolve) => setImmediate(resolve))
	breakResolve()
	await new Promise((resolve) => setImmediate(resolve))

	assert.equal(rollbackCalls, 0, 'an aborted commit must not rollback against the closing connection')
	assert.equal(closed, 1, 'the teardown must close the connection exactly once')
})

// review4 1.2: the deadline can fire while getConnection() is still in flight; break() on
// a connection where nothing is executing would leave the session cancelled, so its next
// statement answers ORA-01013. The fix finishes without break() and lets the finally close
// the idle connection back to the pool.
test('ora: execStream early abort closes without break() on an idle connection', async (t) => {
	let release!: () => void
	const gate = new Promise<void>((resolve) => { release = resolve })
	let broken = 0
	let closed = 0
	const fakeConn = {
		break: () => { broken += 1; return Promise.resolve() },
		close: () => { closed += 1; return Promise.resolve() },
		execute: () => new Promise(() => {}),
		queryStream: () => { throw new Error('must not be reached') },
	}
	t.mock.method(oracledb, 'createPool', () => Promise.resolve({
		getConnection: () => gate.then(() => fakeConn),
		close: async () => {},
	}))

	const adapter = new OraAdapter({ kind: 'ora', host: '127.0.0.1', port: 1521, service: 'x', user: 'u', password: 'p' } as TConnOra)
	const controller = new AbortController()
	const events: string[] = []
	const run = adapter.execStream('SELECT 1 FROM dual', [], (e) => events.push(e.kind), controller.signal)

	controller.abort(new Error('timeout: no data'))
	release()
	await run

	assert.equal(broken, 0, 'break() must not poison an idle connection')
	assert.equal(closed, 1, 'the idle connection must be closed back to the pool')
	assert.equal(events[events.length - 1], 'finish')
})

// review4 1.3: a rejected createPool used to be cached in this._pool forever, so a
// transient failure (network down, misconfiguration) replayed the same stale rejection on
// every call until close(). The fix clears the promise on rejection so the next call
// retries, mirroring DbConnection._adapter's non-caching failure.
test('ora: a rejected createPool is retried on the next call', async (t) => {
	let attempts = 0
	const m = t.mock.method(oracledb, 'createPool', () => {
		attempts += 1
		if (attempts === 1) return Promise.reject(new Error('network down'))
		return Promise.resolve({
			getConnection: async () => ({ close: () => Promise.resolve(), execute: async () => ({ rowsAffected: 0, implicitResults: undefined, outBinds: undefined }) }),
			close: async () => {},
		})
	})
	try {
		const adapter = new OraAdapter({ kind: 'ora', host: 'h', port: 1521, service: 's', user: 'u', password: 'p' } as TConnOra)
		let firstKind: string | undefined
		await adapter.execStream('UPDATE t SET x = 1', [], (e) => {
			if (e.kind === 'finish' && e.error) firstKind = e.error.kind
		})
		assert.equal(firstKind, 'connect', 'the first call must surface the connect failure')
		await adapter.execStream('UPDATE t SET x = 1', [], () => {})
	} finally {
		m.mock.restore()
	}
	assert.equal(attempts, 2, 'the second call must create the pool afresh, not replay the stale rejection')
})

// review3 §4 (Oracle): the review left open whether a connection torn down mid-transaction
// (break() while a statement is still running) comes back into the pool in a healthy state
// or leaks its slot. Prove it live: abort more transactions than the pool has slots while
// DBMS_LOCK.SLEEP keeps a statement running on the server, then run a plain query — it must
// succeed on a healthy connection, so every slot was released and none left dangling.
test('ora: pool stays healthy after repeated mid-transaction aborts', { skip }, async (t) => {
	// A pool of 2 makes a leak impossible to hide: if a torn-down connection is not
	// returned, the next getConnection() stalls and queueTimeout fails the test.
	const oraConn = { ...(conn as TConnOra), poolMax: 2, queueTimeout: 8000 } as TConnOra
	const db = new DbConnection(oraConn)
	t.after(async () => { await db.close() })

	for (let round = 0; round < 4; round++) {
		const r = await db.execTxDynamic(async (exec) => {
			// SLEEP(10) keeps the statement running on the server well past the deadline,
			// so teardown has to break() a live statement and then close() the connection.
			await exec('BEGIN DBMS_LOCK.SLEEP(10); END;', [])
		}, { timeoutMs: 500 })
		assert.equal(r.ok, false, `round ${round}: the deadline should have fired`)
		if (r.ok) return
		assert.equal(r.error.kind, 'timeout', `round ${round}: ${r.error.kind} (${r.error.message})`)
	}

	// After more aborts than poolMax, a plain query must still run on a healthy connection.
	const r2 = await db.exec('SELECT 1 AS p FROM dual', [])
	assert.equal(r2.ok, true, r2.ok ? '' : r2.error.message)
	if (r2.ok) assert.equal(Object.values(r2.result[0]!.rows[0] as Record<string, unknown>)[0], 1)
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

// review7 §2.5: Oracle bind names are case-insensitive, so a query writing one param in
// two cases (:V ... :v) must bind both spellings, not just the first - the server would
// answer ORA-01008 for the unbound spelling.
test('ora: a param written in two cases binds under both spellings', () => {
	const q = buildQuery('SELECT :V AS a, :v AS b FROM dual', [{ name: 'v', value: 1 }])
	assert.deepEqual(q.binds, { V: 1, v: 1 })
})

// review8 §2.7: two params differing only by case (:V vs :v) are the same Oracle bind,
// so a silent overwrite would be wrong - buildQuery rejects the conflict up front.
test('ora: two params differing only by case are rejected', () => {
	assert.throws(
		() => buildQuery('SELECT :V AS a, :v AS b FROM dual', [
			{ name: 'V', value: 1 },
			{ name: 'v', value: 2 },
		]),
		/duplicate param \(case-insensitive\)/,
	)
})

// review4 §2.4: the isTrigger probe missed CREATE [OR REPLACE] EDITIONABLE/NONEDITIONABLE
// TRIGGER and the REFERENCING clause, so :new/:old inside those were treated as binds (the
// server answers ORA-01008) and a REFERENCING NEW AS n / OLD AS o rename was never honoured
// (param not found: n). Both must be correlation names, not placeholders.
test('ora: EDITIONABLE trigger and REFERENCING NEW AS are correlation names', async () => {
	const a = buildQuery('CREATE OR REPLACE EDITIONABLE TRIGGER trg BEFORE INSERT ON x FOR EACH ROW BEGIN :NEW.n := :OLD.o; END;', [])
	assert.deepEqual(a.binds, {})
	const b = buildQuery('CREATE OR REPLACE NONEDITIONABLE TRIGGER trg BEFORE UPDATE ON x REFERENCING NEW AS n OLD AS o FOR EACH ROW BEGIN :n.x := :o.y; END;', [])
	assert.deepEqual(b.binds, {})
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

// review8 §3.d (deferred): pool sizing went into oracledb.createPool unvalidated, so a
// negative / fractional / NaN value failed (or misbehaved) there instead of at construction.
test('ora: invalid pool sizing throws a TypeError at construction', () => {
	const base = { kind: 'ora', host: 'h', port: 1521, service: 's', user: 'u', password: 'p' }
	assert.throws(() => new OraAdapter({ ...base, poolMin: -1 } as TConnOra), TypeError)
	assert.throws(() => new OraAdapter({ ...base, poolMax: 0 } as TConnOra), TypeError)
	assert.throws(() => new OraAdapter({ ...base, poolIncrement: 0 } as TConnOra), TypeError)
	assert.throws(() => new OraAdapter({ ...base, poolMin: 5, poolMax: 3 } as TConnOra), TypeError)
	assert.throws(() => new OraAdapter({ ...base, poolMin: 1.5 } as TConnOra), TypeError)
})

// review4 §2.10: the abort finish used to carry String(signal.reason), so an Error reason
// surfaced as "Error: timeout: ...". abortError() must take Error.message instead.
test('ora: execStream abort reason is the error message, not "Error: ..."', async (t) => {
	t.mock.method(oracledb, 'createPool', () => Promise.resolve({ getConnection: async () => ({ close: async () => {} }), close: async () => {} }))
	const adapter = new OraAdapter({ kind: 'ora', host: '127.0.0.1', port: 1521, service: 'x', user: 'u', password: 'p' } as TConnOra)
	const controller = new AbortController()
	controller.abort(new Error('cancelled by client'))
	const events: { kind: string; error?: { kind?: string; message?: string } }[] = []
	await adapter.execStream('SELECT 1 FROM dual', [], (e) => events.push(e), controller.signal)
	assert.equal(events.length, 1)
	assert.equal(events[0]!.kind, 'finish')
	assert.equal(events[0]!.error?.kind, 'aborted')
	assert.equal(events[0]!.error?.message, 'cancelled by client')
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
	const events: { kind: string; error?: { message?: string } }[] = []
	try {
		const adapter = new OraAdapter({ kind: 'ora', host: 'h', port: 1521, service: 's', user: 'u', password: 'p' } as TConnOra)
		await adapter.execStream('SELECT :missing FROM dual', [], (e) => events.push(e))
	} finally {
		m.mock.restore()
	}
	assert.equal(acquired, 0)
	assert.equal(events.length, 1)
	assert.equal(events[0]!.kind, 'finish')
	assert.match(events[0]!.error?.message ?? '', /param not found: missing/)
})

// review3 §2.8: a PL/SQL block with DBMS_SQL.RETURN_RESULT returns its result sets in
// res.implicitResults; with resultSet:true each is a ResultSet carrying its own metadata.
// The adapter must emit each as its own schema+rows and close every ResultSet (an open
// ResultSet keeps its cursor alive and the connection never returns to the pool).
test('ora: implicit results are emitted as their own result sets and closed', async (t) => {
	let executeOpts: { resultSet?: boolean } = {}
	let closed = 0
	const fakeConn = {
		break: () => Promise.resolve(),
		close: () => Promise.resolve(),
		queryStream: () => { throw new Error('not a row query') },
		execute: async (_sql: string, _binds: unknown, opts: { resultSet?: boolean }) => {
			executeOpts = opts
			return {
				rowsAffected: undefined,
				implicitResults: [
					{ metaData: [{ name: 'A', dbTypeName: 'NUMBER' }], getRows: async () => [{ A: 1 }], close: async () => { closed += 1 } },
					{ metaData: [{ name: 'B', dbTypeName: 'VARCHAR2' }], getRows: async () => [{ B: 'x' }, { B: 'y' }], close: async () => { closed += 1 } },
				],
			}
		},
	}
	t.mock.method(oracledb, 'createPool', () => Promise.resolve({ getConnection: async () => fakeConn, close: async () => {} }))

	const adapter = new OraAdapter({ kind: 'ora', host: '127.0.0.1', port: 1521, service: 'x', user: 'u', password: 'p' } as TConnOra)
	const events: any[] = []
	await adapter.execStream('BEGIN NULL; END;', [], (e) => events.push(e))

	assert.equal(executeOpts.resultSet, true, 'resultSet:true is what carries each set\'s metadata')
	const schemas = events.filter((e) => e.kind === 'schema')
	const rows = events.filter((e) => e.kind === 'row')
	assert.equal(schemas.length, 2)
	assert.equal(schemas[0].schema[0].name, 'A')
	assert.equal(schemas[1].schema[0].name, 'B')
	assert.equal(rows.length, 3)
	assert.deepEqual(rows[0].row, { A: 1 })
	assert.deepEqual(rows[2].row, { B: 'y' })
	assert.equal(events[events.length - 1].kind, 'finish')
	assert.equal(closed, 2, 'every implicit ResultSet must be closed')
})

// review8 §3: outParams fired after the implicit result sets, so exec() attached them to
// the last implicit set instead of the block's own result, and the block's rowsAffected
// was dropped. The block's own result must be emitted before the sets.
test('ora: outParams come before implicit result sets and keep the block rowsAffected', async (t) => {
	const fakeConn = {
		break: () => Promise.resolve(),
		close: () => Promise.resolve(),
		queryStream: () => { throw new Error('not a row query') },
		execute: async () => ({
			rowsAffected: 2,
			implicitResults: [
				{ metaData: [{ name: 'A', dbTypeName: 'NUMBER' }], getRows: async () => [{ A: 1 }], close: async () => {} },
			],
			outBinds: { n: 42 },
		}),
	}
	t.mock.method(oracledb, 'createPool', () => Promise.resolve({ getConnection: async () => fakeConn, close: async () => {} }))

	const adapter = new OraAdapter({ kind: 'ora', host: '127.0.0.1', port: 1521, service: 'x', user: 'u', password: 'p' } as TConnOra)
	const events: any[] = []
	await adapter.execStream('BEGIN UPDATE t SET x = 1; :n := 42; RETURN_RESULT; END;', [{ name: 'n', direction: 'out', type: 'NUMBER' }], (e) => events.push(e))

	const outParamsIdx = events.findIndex((e) => e.kind === 'outParams')
	const implicitSchemaIdx = events.findIndex((e) => e.kind === 'schema' && e.schema.length > 0)
	assert.ok(outParamsIdx >= 0, 'outParams must be emitted')
	assert.ok(implicitSchemaIdx >= 0, 'the implicit schema must be emitted')
	assert.ok(implicitSchemaIdx > outParamsIdx, 'outParams must precede the implicit set')
	assert.equal(events.filter((e) => e.kind === 'rowsAffected').length, 1, 'the block rowsAffected is kept, not dropped')
	assert.equal(events.filter((e) => e.kind === 'schema' && e.schema.length === 0).length, 1, 'one empty-schema block result')
})

// review3 §2.8: live - a PL/SQL block that returns several implicit result sets must
// surface each as its own result, with its own schema, instead of silently dropping them.
test('ora: DBMS_SQL.RETURN_RESULT surfaces every implicit result set', { skip }, async (t) => {
	const db = new DbConnection(conn!)
	try {
		const r = await db.exec(
			"DECLARE c1 SYS_REFCURSOR; c2 SYS_REFCURSOR; BEGIN OPEN c1 FOR SELECT 1 AS a, 'x' AS b FROM dual; DBMS_SQL.RETURN_RESULT(c1); OPEN c2 FOR SELECT 2 AS c FROM dual; DBMS_SQL.RETURN_RESULT(c2); END;",
			[],
		)
		assert.equal(r.ok, true, r.ok ? '' : r.error.message)
		if (!r.ok) return
		assert.equal(r.result.length, 2)
		assert.equal(r.result[0]!.rows.length, 1)
		assert.equal(r.result[1]!.rows.length, 1)
		assert.deepEqual(Object.values(r.result[0]!.rows[0] as Record<string, unknown>), [1, 'x'])
		assert.deepEqual(Object.values(r.result[1]!.rows[0] as Record<string, unknown>), [2])
		assert.equal(r.result[0]!.schema.length, 2)
		assert.equal(r.result[1]!.schema.length, 1)
	} finally {
		await db.close()
	}
})

// review3 2.8 + TDbTxResult: the transaction exec surfaces Oracle implicit results as
// TDbTxResult.implicitResults, and closes every ResultSet it reads.
test('ora: execTxDynamic exec returns implicit results and closes ResultSets', async (t) => {
	let closed = 0
	const fakeConn = {
		break: () => Promise.resolve(),
		close: () => Promise.resolve(),
		rollback: () => Promise.resolve(),
		commit: () => Promise.resolve(),
		execute: async () => ({
			resultSet: undefined,
			rowsAffected: undefined,
			implicitResults: [
				{ metaData: [{ name: 'A', dbTypeName: 'NUMBER' }], getRows: async () => [{ A: 7 }], close: async () => { closed += 1 } },
			],
		}),
	}
	t.mock.method(oracledb, 'createPool', () => Promise.resolve({ getConnection: async () => fakeConn, close: async () => {} }))

	const adapter = new OraAdapter({ kind: 'ora', host: 'h', port: 1521, service: 's', user: 'u', password: 'p' } as TConnOra)
	let got: TDbTxResult | undefined
	await adapter.execTxDynamic(async (exec) => {
		got = await exec('BEGIN NULL; END;', [])
	})

	assert.ok(got)
	assert.equal(got!.implicitResults!.length, 1)
	assert.equal(got!.implicitResults![0]!.schema[0]!.name, 'A')
	assert.deepEqual(got!.implicitResults![0]!.rows, [{ A: 7 }])
	assert.equal(closed, 1, 'the implicit ResultSet must be closed')
})

// resultSet:true (needed for implicit results) moves a SELECT's rows onto res.resultSet;
// the transaction exec must still return them in `rows` and close the ResultSet.
test('ora: execTxDynamic exec reads a SELECT from res.resultSet', async (t) => {
	let closed = 0
	const fakeConn = {
		break: () => Promise.resolve(),
		close: () => Promise.resolve(),
		rollback: () => Promise.resolve(),
		commit: () => Promise.resolve(),
		execute: async () => ({
			resultSet: { metaData: [{ name: 'V', dbTypeName: 'VARCHAR2' }], getRows: async () => [{ V: 'world' }], close: async () => { closed += 1 } },
			rowsAffected: undefined,
			implicitResults: undefined,
		}),
	}
	t.mock.method(oracledb, 'createPool', () => Promise.resolve({ getConnection: async () => fakeConn, close: async () => {} }))

	const adapter = new OraAdapter({ kind: 'ora', host: 'h', port: 1521, service: 's', user: 'u', password: 'p' } as TConnOra)
	let got: TDbTxResult | undefined
	await adapter.execTxDynamic(async (exec) => {
		got = await exec('SELECT :v AS v FROM dual', [{ name: 'v', value: 'world' }])
	})

	assert.ok(got)
	assert.deepEqual(got!.rows, [{ V: 'world' }])
	assert.equal(got!.implicitResults, undefined)
	assert.equal(closed, 1, 'the SELECT ResultSet must be closed')
})

// review3 2.8 + TDbTxResult: live - a PL/SQL block inside a transaction returns its
// implicit result sets on TDbTxResult.implicitResults.
test('ora: execTxDynamic surfaces PL/SQL implicit results', { skip }, async (t) => {
	const db = new DbConnection(conn!)
	try {
		const r = await db.execTxDynamic(async (exec) => {
			const res = await exec(
				"DECLARE c1 SYS_REFCURSOR; c2 SYS_REFCURSOR; BEGIN OPEN c1 FOR SELECT 1 AS a, 'x' AS b FROM dual; DBMS_SQL.RETURN_RESULT(c1); OPEN c2 FOR SELECT 2 AS c FROM dual; DBMS_SQL.RETURN_RESULT(c2); END;",
				[],
			)
			assert.equal(res.implicitResults!.length, 2)
			assert.deepEqual(Object.values(res.implicitResults![0]!.rows[0] as Record<string, unknown>), [1, 'x'])
			assert.deepEqual(Object.values(res.implicitResults![1]!.rows[0] as Record<string, unknown>), [2])
		})
		assert.equal(r.ok, true, r.ok ? '' : r.error.message)
	} finally {
		await db.close()
	}
})

// review12 §2.1: on the DML/PL-SQL path a mid-fetch abort used to return from the
// execute handler before closing res.resultSet, leaking the unread cursor (a SELECT
// misrouted to execute() leaves its rows on res.resultSet). The abort branch must still
// close it, exactly like the normal path does.
test('ora: an abort while fetching implicit rows still closes the unread resultSet', async (t) => {
	let resultSetClosed = 0
	let resolveRows!: (rows: unknown[]) => void
	const fakeConn = {
		break: () => Promise.resolve(),
		close: () => Promise.resolve(),
		queryStream: () => { throw new Error('not a row query') },
		execute: async () => ({
			resultSet: { close: async () => { resultSetClosed += 1 } },
			rowsAffected: 0,
			implicitResults: [
				{ metaData: [{ name: 'A', dbTypeName: 'NUMBER' }], getRows: () => new Promise<unknown[]>((resolve) => { resolveRows = resolve }), close: async () => {} },
			],
			outBinds: undefined,
		}),
	}
	t.mock.method(oracledb, 'createPool', () => Promise.resolve({ getConnection: async () => fakeConn, close: async () => {} }))

	const adapter = new OraAdapter({ kind: 'ora', host: 'h', port: 1521, service: 's', user: 'u', password: 'p' } as TConnOra)
	const controller = new AbortController()
	const events: string[] = []
	const run = adapter.execStream('UPDATE t SET x = 1', [], (e) => events.push(e.kind), controller.signal)

	await new Promise((resolve) => setImmediate(resolve)) // execute() ran, readImplicitResults blocked on getRows
	controller.abort(new Error('cancelled'))               // done = true, finish(aborted)
	resolveRows([])                                         // readImplicitResults completes -> aborted branch closes resultSet
	await run
	await new Promise((resolve) => setImmediate(resolve))  // flush the execute handler's close

	assert.equal(resultSetClosed, 1, 'the unread resultSet must be closed on the abort path')
	assert.equal(events[events.length - 1], 'finish')
})

// review12 §2.2: DbConnection contains a finish-callback throw, but a direct IDbAdapter
// consumer has no outer layer - the adapter's own finish() must swallow it so resolve()
// still runs (otherwise execStream hangs forever).
test('ora: a throw from the finish callback on a direct adapter still resolves', async (t) => {
	const fakeConn = {
		break: () => Promise.resolve(),
		close: () => Promise.resolve(),
		queryStream: () => { throw new Error('not a row query') },
		execute: async () => ({ resultSet: undefined, rowsAffected: 1, implicitResults: undefined, outBinds: undefined }),
	}
	t.mock.method(oracledb, 'createPool', () => Promise.resolve({ getConnection: async () => fakeConn, close: async () => {} }))

	const adapter = new OraAdapter({ kind: 'ora', host: 'h', port: 1521, service: 's', user: 'u', password: 'p' } as TConnOra)
	await adapter.execStream('UPDATE t SET x = 1', [], (event) => {
		if (event.kind === 'finish') throw new Error('finish boom')
	})
	// Reaching here is the assertion: the throw must not reject execStream nor skip the
	// adapter's resolve() (which would hang the call forever).
})

// review14 2.2: execStream's bounded wait for the stream 'close' used to leave its 1s
// fallback timer armed after the race settled, holding the event loop and delaying
// short-lived scripts/tests by ~1s. The timer must be cleared once the race is over.
test('ora: execStream clears its close-wait fallback timer after the stream closes', async (t) => {
	const stream = new EventEmitter()
	const fakeConn = {
		break: () => Promise.resolve(),
		close: () => Promise.resolve(),
		queryStream: () => {
			// Fire the stream events on the next tick, after the adapter has attached
			// its listeners synchronously right below queryStream().
			setImmediate(() => {
				stream.emit('metadata', [])
				stream.emit('data', { A: 1 })
				stream.emit('close')
			})
			return stream
		},
		execute: () => { throw new Error('must not be reached') },
	}
	t.mock.method(oracledb, 'createPool', () => Promise.resolve({ getConnection: async () => fakeConn, close: async () => {} }))

	let armed: unknown
	const cleared: unknown[] = []
	t.mock.method(globalThis, 'setTimeout', (fn: () => void) => { armed = { fn }; return armed })
	t.mock.method(globalThis, 'clearTimeout', (handle?: unknown) => { cleared.push(handle) })

	const adapter = new OraAdapter({ kind: 'ora', host: 'h', port: 1521, service: 's', user: 'u', password: 'p' } as TConnOra)
	await adapter.execStream('SELECT 1 FROM dual', [], () => {})

	assert.equal(cleared.length, 1, 'the fallback timer must be cleared once the race settles')
	assert.equal(cleared[0], armed, 'the cleared handle must be the armed fallback timer')
})
