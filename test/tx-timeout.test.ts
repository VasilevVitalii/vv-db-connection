import { test } from 'node:test'
import assert from 'node:assert/strict'
import sql from 'mssql'
import { MssqlAdapter } from '../src/db/adapters/mssql'
import type { TConnMssql } from '../src/db/connection'

// Regression guard for review2 1.1: when the tx deadline fires while connect() is still
// in flight, the abort listener is registered on an already-aborted signal (so it never
// runs) and the finally skips close() — the freshly connected pool would leak. The fix
// checks signal.aborted right after connect and tears the pool down at once.
const conn: TConnMssql = {
	kind: 'mssql',
	server: '127.0.0.1',
	auth: { kind: 'mssql', user: 'u', password: 'p' },
}

test('execTxDynamic tears the pool down when the deadline fires during connect', async (t) => {
	let release!: () => void
	const gate = new Promise<void>((resolve) => { release = resolve })
	let closed = 0

	// connect() hangs until we release it — well past the 50ms deadline.
	t.mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) {
		return gate.then(() => this)
	})
	t.mock.method(sql.ConnectionPool.prototype, 'close', () => {
		closed += 1
		return Promise.resolve()
	})

	const adapter = new MssqlAdapter(conn)
	const r = await adapter.execTxDynamic(async () => { throw new Error('body must not run') }, 50)

	assert.equal(r.ok, false)
	if (!r.ok) assert.equal(r.error.kind, 'timeout')

	// Let connect finish: the body resumes on an aborted signal and must close the pool
	// itself, since the abort event has already fired and will not fire again.
	release()
	await new Promise((resolve) => setImmediate(resolve))

	assert.equal(closed, 1)
})

// Regression guard for review2 §2 "тело транзакции после таймаута": a graceful
// pool.close() waits for the busy connection, so a stuck statement would hold the slot
// forever. The teardown must rip the tedious socket instead; pool.close() then frees the
// dedicated pool at once.
test('execTxDynamic hard-breaks the socket when the deadline fires mid-transaction', async (t) => {
	let destroyed = 0
	let closed = 0
	const never = new Promise<void>(() => {})

	t.mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) {
		return Promise.resolve(this)
	})
	t.mock.method(sql.ConnectionPool.prototype, 'close', () => {
		closed += 1
		return Promise.resolve()
	})
	t.mock.method(sql.Transaction.prototype, 'begin', function (this: sql.Transaction) {
		;(this as unknown as { _acquiredConnection: unknown })._acquiredConnection = {
			socket: { destroy: () => { destroyed += 1 } },
		}
		return Promise.resolve()
	})

	const adapter = new MssqlAdapter(conn)
	const r = await adapter.execTxDynamic(async () => { await never }, 50)

	assert.equal(r.ok, false)
	if (!r.ok) assert.equal(r.error.kind, 'timeout')

	assert.equal(destroyed, 1)
	assert.equal(closed, 1)
})

// review2 §2 "классификация ошибок транзакций": begin/commit failures are 'tx'. The
// query/user kinds are covered by the integration runDbSuite in helpers.ts.
test('execTxDynamic reports a begin failure as kind tx', async (t) => {
	t.mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) { return Promise.resolve(this) })
	t.mock.method(sql.ConnectionPool.prototype, 'close', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'begin', () => Promise.reject(new Error('begin failed')))
	t.mock.method(sql.Transaction.prototype, 'rollback', () => Promise.resolve())

	const adapter = new MssqlAdapter(conn)
	const r = await adapter.execTxDynamic(async () => { throw new Error('body must not run') })

	assert.equal(r.ok, false)
	if (!r.ok) assert.equal(r.error.kind, 'tx')
})

// review3 §2.1: buildMssqlQuery/mssqlInput throw synchronously on a missing/out/unknown
// param, before the promise is built. exec() must not throw synchronously (so
// exec(...).catch(...) works) and must classify the failure as 'query', not 'user' — the
// same query through exec() would be 'query'.
test('execTxDynamic turns a sync param throw from exec into a query error', async (t) => {
	t.mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) { return Promise.resolve(this) })
	t.mock.method(sql.ConnectionPool.prototype, 'close', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'begin', function (this: sql.Transaction) {
		;(this as unknown as { _acquiredConnection: unknown })._acquiredConnection = { socket: { destroy() {} } }
		return Promise.resolve()
	})
	t.mock.method(sql.Transaction.prototype, 'commit', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'rollback', () => Promise.resolve())

	const adapter = new MssqlAdapter(conn)
	const r = await adapter.execTxDynamic(async (exec) => {
		// ':missing' is not in params, so buildMssqlQuery throws synchronously.
		await exec('SELECT :missing AS x', [])
	})

	assert.equal(r.ok, false)
	if (!r.ok) assert.equal(r.error.kind, 'query')
})

test('execTxDynamic reports a commit failure as kind tx', async (t) => {
	t.mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) { return Promise.resolve(this) })
	t.mock.method(sql.ConnectionPool.prototype, 'close', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'begin', function (this: sql.Transaction) {
		;(this as unknown as { _acquiredConnection: unknown })._acquiredConnection = { socket: { destroy() {} } }
		return Promise.resolve()
	})
	t.mock.method(sql.Transaction.prototype, 'commit', () => Promise.reject(new Error('commit failed')))
	t.mock.method(sql.Transaction.prototype, 'rollback', () => Promise.resolve())

	const adapter = new MssqlAdapter(conn)
	const r = await adapter.execTxDynamic(async () => {})

	assert.equal(r.ok, false)
	if (!r.ok) assert.equal(r.error.kind, 'tx')
})

// review3 3.1: TDbTxResult carries rowsAffected so a transaction callback can see how
// many rows its DML changed (mssql res.rowsAffected[0]).
test('execTxDynamic exec returns rowsAffected for a DML statement', async (t) => {
	t.mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) { return Promise.resolve(this) })
	t.mock.method(sql.ConnectionPool.prototype, 'close', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'begin', function (this: sql.Transaction) {
		;(this as unknown as { _acquiredConnection: unknown })._acquiredConnection = { socket: { destroy() {} } }
		return Promise.resolve()
	})
	t.mock.method(sql.Transaction.prototype, 'commit', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'rollback', () => Promise.resolve())
	t.mock.method(sql.Request.prototype, 'query', function (this: sql.Request) {
		return new Promise((resolve) => {
			setImmediate(() => {
				this.emit('rowsaffected', 5)
				this.emit('done', { rowsAffected: [5] })
				resolve({ recordset: undefined, rowsAffected: [5] })
			})
		})
	})

	const adapter = new MssqlAdapter(conn)
	let affected: number | undefined
	const r = await adapter.execTxDynamic(async (exec) => {
		const res = await exec('UPDATE t SET x = 1', [])
		affected = res.rowsAffected
	})

	assert.equal(r.ok, true, r.ok ? '' : r.error.message)
	assert.equal(affected, 5)
})

// review6 1.2: tx rowsAffected must count every SELECT's rows, not just the first
// recordset, so a SELECT-only batch reports no DML count (unset) instead of a phantom one.
test('execTxDynamic tx rowsAffected stays unset for a SELECT-only batch', async (t) => {
	t.mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) { return Promise.resolve(this) })
	t.mock.method(sql.ConnectionPool.prototype, 'close', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'begin', function (this: sql.Transaction) {
		;(this as unknown as { _acquiredConnection: unknown })._acquiredConnection = { socket: { destroy() {} } }
		return Promise.resolve()
	})
	t.mock.method(sql.Transaction.prototype, 'commit', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'rollback', () => Promise.resolve())
	t.mock.method(sql.Request.prototype, 'query', function (this: sql.Request) {
		const col = { v: { name: 'v', type: sql.Int, length: 4, scale: undefined, precision: 10 } }
		return new Promise((resolve) => {
			setImmediate(() => {
				this.emit('recordset', col)
				this.emit('row', { v: 1 })
				this.emit('rowsaffected', 1)
				this.emit('recordset', col)
				this.emit('row', { v: 2 })
				this.emit('rowsaffected', 1)
				this.emit('done', { rowsAffected: [1, 1] })
				resolve({ recordset: [{ v: 1 }], recordsets: [[{ v: 1 }], [{ v: 2 }]], rowsAffected: [1, 1] })
			})
		})
	})

	const adapter = new MssqlAdapter(conn)
	let affected: number | undefined
	await adapter.execTxDynamic(async (exec) => {
		affected = (await exec('SELECT 1; SELECT 2', [])).rowsAffected
	})
	assert.equal(affected, undefined)
})

// review6 1.2 (companion): a batch that also ran DML reports total minus the SELECT rows,
// so a DML touching zero rows yields 0, not unset (the README rowsAffected contract).
test('execTxDynamic tx rowsAffected reports 0 for a DML that touched no rows', async (t) => {
	t.mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) { return Promise.resolve(this) })
	t.mock.method(sql.ConnectionPool.prototype, 'close', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'begin', function (this: sql.Transaction) {
		;(this as unknown as { _acquiredConnection: unknown })._acquiredConnection = { socket: { destroy() {} } }
		return Promise.resolve()
	})
	t.mock.method(sql.Transaction.prototype, 'commit', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'rollback', () => Promise.resolve())
	t.mock.method(sql.Request.prototype, 'query', function (this: sql.Request) {
		const col = { v: { name: 'v', type: sql.Int, length: 4, scale: undefined, precision: 10 } }
		return new Promise((resolve) => {
			setImmediate(() => {
				this.emit('rowsaffected', 0)
				this.emit('recordset', col)
				this.emit('row', { v: 1 })
				this.emit('row', { v: 2 })
				this.emit('rowsaffected', 2)
				this.emit('done', { rowsAffected: [0, 2] })
				resolve({ recordset: [{ v: 1 }, { v: 2 }], recordsets: [[{ v: 1 }, { v: 2 }]], rowsAffected: [0, 2] })
			})
		})
	})

	const adapter = new MssqlAdapter(conn)
	let affected: number | undefined
	await adapter.execTxDynamic(async (exec) => {
		affected = (await exec('UPDATE t SET x = 1; SELECT 1', [])).rowsAffected
	})
	assert.equal(affected, 0)
})

// review13 2.2: under SET NOCOUNT ON a SELECT emits no rowsaffected, so the following
// DML's count arrives while the SELECT's recordset is still open. The non-streaming
// res.rowsAffected/res.recordsets bookkeeping lost the NOCOUNT-suppressed SELECT's slot
// and misread the DML as a SELECT (rowsAffected unset). The transaction path now streams
// and tells SELECT from DML per statement, so the DML count survives.
test('execTxDynamic tx rowsAffected survives a NOCOUNT-suppressed SELECT', async (t) => {
	t.mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) { return Promise.resolve(this) })
	t.mock.method(sql.ConnectionPool.prototype, 'close', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'begin', function (this: sql.Transaction) {
		;(this as unknown as { _acquiredConnection: unknown })._acquiredConnection = { socket: { destroy() {} } }
		return Promise.resolve()
	})
	t.mock.method(sql.Transaction.prototype, 'commit', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'rollback', () => Promise.resolve())
	t.mock.method(sql.Request.prototype, 'query', function (this: sql.Request) {
		const col = { v: { name: 'v', type: sql.Int, length: 4, scale: undefined, precision: 10 } }
		return new Promise((resolve) => {
			setImmediate(() => {
				this.emit('recordset', col)
				this.emit('row', { v: 1 })
				// The SELECT's rowsaffected is suppressed by SET NOCOUNT ON — only the
				// UPDATE emits one.
				this.emit('rowsaffected', 3)
				this.emit('done', { rowsAffected: [3] })
				resolve({ recordset: [{ v: 1 }], recordsets: [[{ v: 1 }]], rowsAffected: [3] })
			})
		})
	})

	const adapter = new MssqlAdapter(conn)
	let affected: number | undefined
	await adapter.execTxDynamic(async (exec) => {
		affected = (await exec('SET NOCOUNT ON; SELECT v FROM t; SET NOCOUNT OFF; UPDATE b SET x = 2', [])).rowsAffected
	})
	assert.equal(affected, 3)
})

// review4 §3.2: execTxDynamic<T> returns the callback's resolved value as the result, so
// a value computed inside the transaction no longer needs a closure to escape.
test('execTxDynamic returns the callback value as result', async (t) => {
	t.mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) { return Promise.resolve(this) })
	t.mock.method(sql.ConnectionPool.prototype, 'close', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'begin', function (this: sql.Transaction) {
		;(this as unknown as { _acquiredConnection: unknown })._acquiredConnection = { socket: { destroy() {} } }
		return Promise.resolve()
	})
	t.mock.method(sql.Transaction.prototype, 'commit', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'rollback', () => Promise.resolve())

	const adapter = new MssqlAdapter(conn)
	const r = await adapter.execTxDynamic(async () => 42)

	assert.equal(r.ok, true)
	if (r.ok) assert.equal(r.result, 42)
})

// review14 2.1: some mssql releases reject the query() promise on a stream error BEFORE
// the 'done' event frees the connection. The .catch must record the error, not reject —
// rejecting there would run the rollback below on a connection the failed request still
// holds (a deadlock). exec() must reject only after 'done'.
test('execTxDynamic defers exec rejection to the done event, not the query() promise', async (t) => {
	t.mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) { return Promise.resolve(this) })
	t.mock.method(sql.ConnectionPool.prototype, 'close', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'begin', function (this: sql.Transaction) {
		;(this as unknown as { _acquiredConnection: unknown })._acquiredConnection = { socket: { destroy() {} } }
		return Promise.resolve()
	})
	t.mock.method(sql.Transaction.prototype, 'commit', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'rollback', () => Promise.resolve())

	let doneFired = false
	t.mock.method(sql.Request.prototype, 'query', function (this: sql.Request) {
		return new Promise((_resolve, reject) => {
			// Error event first, then the promise rejects (a release whose promise rejects
			// on error), and only a later tick frees the connection via 'done'.
			setImmediate(() => {
				this.emit('error', new Error('boom'))
				reject(new Error('boom'))
				setImmediate(() => {
					doneFired = true
					this.emit('done', {})
				})
			})
		})
	})

	const adapter = new MssqlAdapter(conn)
	let rejectedBeforeDone = false
	const r = await adapter.execTxDynamic(async (exec) => {
		try {
			await exec('SELECT 1', [])
		} catch {
			rejectedBeforeDone = !doneFired
			throw new Error('callback rethrow')
		}
	})

	assert.equal(rejectedBeforeDone, false, 'exec must reject only after the done event frees the connection')
	assert.equal(r.ok, false)
	if (!r.ok) assert.equal(r.error.kind, 'user')
})

// review15 2.1: if the driver emits 'error' (or rejects query()) but never emits
// 'done', exec() must not hang forever. A short fallback rips the socket and rejects
// with 'query', so the transaction ends instead of stranding the connection.
test('execTxDynamic rejects via fallback when done never follows the stream error', async (t) => {
	let destroyed = 0
	t.mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) { return Promise.resolve(this) })
	t.mock.method(sql.ConnectionPool.prototype, 'close', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'begin', function (this: sql.Transaction) {
		;(this as unknown as { _acquiredConnection: unknown })._acquiredConnection = {
			socket: { destroy: () => { destroyed += 1 } },
		}
		return Promise.resolve()
	})
	t.mock.method(sql.Transaction.prototype, 'rollback', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'commit', () => Promise.resolve())

	// Arm the watchdog manually so the test does not wait the real window: capture
	// the callback and fire it once the error has been emitted and the watchdog armed.
	let fallbackFn!: () => void
	let handle: unknown
	const setT = t.mock.method(globalThis, 'setTimeout', (fn: () => void) => {
		fallbackFn = fn
		handle = { fn }
		return handle
	})

	t.mock.method(sql.Request.prototype, 'query', function (this: sql.Request) {
		// The statement fails and the driver never emits 'done' (it never releases the
		// connection), leaving the transaction borrowed forever without the fallback.
		setImmediate(() => { this.emit('error', new Error('boom')) })
		return new Promise(() => {}) // never settles
	})

	const adapter = new MssqlAdapter(conn)
	const p = adapter.execTxDynamic(async (exec) => {
		await exec('SELECT 1', [])
	})
	// The error is emitted on a setImmediate from inside the mocked query(), so wait
	// (bounded) until the fallback timer has been armed.
	for (let i = 0; i < 100 && !fallbackFn; i++) {
		await new Promise((resolve) => setImmediate(resolve))
	}
	assert.ok(fallbackFn, 'the fallback timer must be armed after the error event')
	fallbackFn() // fire the fallback
	const r = await p

	assert.equal(r.ok, false)
	if (!r.ok) assert.equal(r.error.kind, 'query')
	assert.equal(destroyed, 1, 'the fallback must rip the socket so pool.close() can free it')

	setT.mock.restore()
})

// review16 2.1 (part 1): the done-watchdog window must follow the transaction's own
// deadline, not a fixed 1s — a fixed 1s ripped the socket out from under a still-running
// batch whose 'done' only arrived later. With a deadline the watchdog must not fire
// earlier than the deadline itself.
test('execTxDynamic watchdog window follows the transaction deadline, not a fixed 1s', async (t) => {
	t.mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) { return Promise.resolve(this) })
	t.mock.method(sql.ConnectionPool.prototype, 'close', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'begin', function (this: sql.Transaction) {
		;(this as unknown as { _acquiredConnection: unknown })._acquiredConnection = { socket: { destroy() {} } }
		return Promise.resolve()
	})
	t.mock.method(sql.Transaction.prototype, 'rollback', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'commit', () => Promise.resolve())

	const delays: number[] = []
	const setT = t.mock.method(globalThis, 'setTimeout', (fn: () => void, delay?: number) => {
		delays.push(delay as number)
		return { fn }
	})

	t.mock.method(sql.Request.prototype, 'query', function (this: sql.Request) {
		setImmediate(() => { this.emit('error', new Error('boom')) })
		return new Promise(() => {}) // never settles — the watchdog is the only way out
	})

	const adapter = new MssqlAdapter(conn)
	// Intentionally not awaited: the transaction hangs on a statement that never 'done's.
	void adapter.execTxDynamic(async (exec) => { await exec('SELECT 1', []) }, 5000)

	// The deadline timer (armed by withTxTimeout) and then the watchdog both go through
	// setTimeout; wait until the error event has armed the watchdog.
	for (let i = 0; i < 100 && delays.length < 2; i++) {
		await new Promise((resolve) => setImmediate(resolve))
	}
	assert.ok(delays.length >= 2, 'the watchdog must be armed after the error event')
	assert.ok(!delays.includes(1000), 'the watchdog must not use a fixed 1s window')
	// delays[0] is withTxTimeout's own deadline timer (also 5000); assert the second
	// timer (the watchdog) specifically — a bare includes() would pass on the deadline
	// alone even if the watchdog window were wrong.
	assert.equal(delays[1], 5000, 'the watchdog window must be the transaction deadline (5000ms)')

	setT.mock.restore()
})

// review16 2.1 (part 2): a statement-level error does not stop the batch, so the watchdog
// must be re-armed by the liveness events that follow (recordset/row/rowsaffected) instead
// of firing after a fixed delay armed on 'error' alone.
test('execTxDynamic re-arms the watchdog on liveness events', async (t) => {
	let destroyed = 0
	t.mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) { return Promise.resolve(this) })
	t.mock.method(sql.ConnectionPool.prototype, 'close', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'begin', function (this: sql.Transaction) {
		;(this as unknown as { _acquiredConnection: unknown })._acquiredConnection = {
			socket: { destroy: () => { destroyed += 1 } },
		}
		return Promise.resolve()
	})
	t.mock.method(sql.Transaction.prototype, 'rollback', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'commit', () => Promise.resolve())

	let setTimeoutCalls = 0
	const setT = t.mock.method(globalThis, 'setTimeout', (fn: () => void) => {
		setTimeoutCalls += 1
		return { fn }
	})

	t.mock.method(sql.Request.prototype, 'query', function (this: sql.Request) {
		return new Promise((resolve) => {
			setImmediate(() => {
				// A statement-level failure, then the batch keeps streaming and 'done' arrives.
				this.emit('error', new Error('constraint violated'))
				this.emit('recordset', {})
				this.emit('row', { v: 1 })
				this.emit('done', {})
				resolve({})
			})
		})
	})

	const adapter = new MssqlAdapter(conn)
	const r = await adapter.execTxDynamic(async (exec) => {
		await exec('UPDATE t SET x = 1; SELECT 1', [])
	})

	// The statement error is still reported, but the connection must not be torn down:
	// 'done' arrived naturally and rollback ran on a healthy connection.
	assert.equal(r.ok, false)
	if (!r.ok) assert.equal(r.error.kind, 'query')
	assert.equal(destroyed, 0, 'the watchdog must not fire while the batch keeps emitting liveness events')
	assert.ok(setTimeoutCalls >= 3, `the watchdog must re-arm on each liveness event; got ${setTimeoutCalls} arms`)

	setT.mock.restore()
})

// review16 2.2: after the watchdog tears the socket down, the rollback in the catch must
// be skipped — running it on the dead socket would (with no deadline to bound it) hang.
test('execTxDynamic skips rollback after the watchdog tears the socket down', async (t) => {
	let destroyed = 0
	let rollbacks = 0
	t.mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) { return Promise.resolve(this) })
	t.mock.method(sql.ConnectionPool.prototype, 'close', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'begin', function (this: sql.Transaction) {
		;(this as unknown as { _acquiredConnection: unknown })._acquiredConnection = {
			socket: { destroy: () => { destroyed += 1 } },
		}
		return Promise.resolve()
	})
	t.mock.method(sql.Transaction.prototype, 'rollback', () => { rollbacks += 1; return Promise.resolve() })
	t.mock.method(sql.Transaction.prototype, 'commit', () => Promise.resolve())

	let watchdogFn!: () => void
	const setT = t.mock.method(globalThis, 'setTimeout', (fn: () => void) => {
		watchdogFn = fn
		return { fn }
	})

	t.mock.method(sql.Request.prototype, 'query', function (this: sql.Request) {
		setImmediate(() => { this.emit('error', new Error('boom')) })
		return new Promise(() => {}) // never emits 'done' — the watchdog fires
	})

	const adapter = new MssqlAdapter(conn)
	const p = adapter.execTxDynamic(async (exec) => {
		await exec('SELECT 1', [])
	})
	for (let i = 0; i < 100 && !watchdogFn; i++) {
		await new Promise((resolve) => setImmediate(resolve))
	}
	assert.ok(watchdogFn, 'the watchdog must be armed after the error event')
	watchdogFn() // fire the watchdog

	const r = await p
	assert.equal(r.ok, false)
	if (!r.ok) assert.equal(r.error.kind, 'query')
	assert.equal(destroyed, 1, 'the watchdog must rip the socket')
	assert.equal(rollbacks, 0, 'rollback must be skipped on a socket the watchdog already tore down')

	setT.mock.restore()
})

// review17 2.2: an abort must clear the watchdog — otherwise the timer outlives the
// exec (up to windowMs) and holds the event loop after the transaction already
// settled (the same class review9 2.2 fixed for armCommitDeadline).
test('execTxDynamic clears the watchdog when the signal aborts mid-exec', async (t) => {
	let destroyed = 0
	t.mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) { return Promise.resolve(this) })
	t.mock.method(sql.ConnectionPool.prototype, 'close', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'begin', function (this: sql.Transaction) {
		;(this as unknown as { _acquiredConnection: unknown })._acquiredConnection = {
			socket: { destroy: () => { destroyed += 1 } },
		}
		return Promise.resolve()
	})
	t.mock.method(sql.Transaction.prototype, 'rollback', () => Promise.resolve())
	t.mock.method(sql.Transaction.prototype, 'commit', () => Promise.resolve())

	// No deadline: the watchdog is the only timer armed (windowMs = the 15s fallback),
	// so the deadline timer does not crowd the captured handles.
	const watchdogHandles: unknown[] = []
	const cleared: unknown[] = []
	const setT = t.mock.method(globalThis, 'setTimeout', (fn: () => void) => {
		const handle = { fn }
		watchdogHandles.push(handle)
		return handle
	})
	const clearT = t.mock.method(globalThis, 'clearTimeout', (h: unknown) => { cleared.push(h) })

	t.mock.method(sql.Request.prototype, 'query', function (this: sql.Request) {
		setImmediate(() => { this.emit('error', new Error('boom')) })
		return new Promise(() => {}) // never emits 'done' — only abort can settle it
	})

	const adapter = new MssqlAdapter(conn)
	const controller = new AbortController()
	const p = adapter.execTxDynamic(async (exec) => { await exec('SELECT 1', []) }, undefined, controller.signal)

	for (let i = 0; i < 100 && watchdogHandles.length < 1; i++) {
		await new Promise((resolve) => setImmediate(resolve))
	}
	assert.ok(watchdogHandles.length >= 1, 'the watchdog must be armed after the error event')
	controller.abort(new Error('cancel'))

	const r = await p
	assert.equal(r.ok, false)
	if (!r.ok) assert.equal(r.error.kind, 'aborted')
	assert.equal(destroyed, 1, 'abort must tear the socket down')
	assert.ok(cleared.includes(watchdogHandles[0]), 'abort must clear the watchdog so it does not outlive the exec')

	setT.mock.restore()
	clearT.mock.restore()
})
