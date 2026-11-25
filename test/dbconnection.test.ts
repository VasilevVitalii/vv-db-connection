import { test } from 'node:test'
import assert from 'node:assert/strict'
import { DbConnection } from '../src/db/index'
import type { TConnectionParam, TConnOra, TConnPg } from '../src/db/connection'
import type { IDbAdapter, TDbError, TDbParam, TDbRes, TDbStreamEvent } from '../src/db/types'
import type { TResult } from '../src/tresult'

const unknown = { kind: 'unknown' } as unknown as TConnectionParam

// review3 §2.2: a missing/unloadable driver (or an unknown kind) must surface through the
// normal result contract - a finish event for execStream, { ok: false } for the rest -
// never a bare rejection that escapes the DbConnection layer.
test('execStream ends with a finish event when the driver kind is unknown', async () => {
	const db = new DbConnection(unknown)
	const events: TDbStreamEvent[] = []
	await db.execStream('SELECT 1', [], (e) => events.push(e))
	assert.equal(events.length, 1)
	const e = events[0] as { kind: 'finish'; error: TDbError }
	assert.equal(e.kind, 'finish')
	assert.equal(e.error.kind, 'connect')
})

test('version/ping/execTxDynamic return { ok: false } when the driver kind is unknown', async () => {
	const db = new DbConnection(unknown)
	const v = await db.version()
	assert.equal(v.ok, false)
	assert.equal((v.error as TDbError).kind, 'connect')
	assert.equal((await db.ping()).ok, false)
	assert.equal((await db.execTxDynamic(async () => {})).ok, false)
})

// review7 §2.7: an already-aborted signal fails execTxDynamic as 'aborted' without
// loading a driver, symmetric with execStream's early abort - a missing driver would
// otherwise surface as 'connect'.
test('execTxDynamic with an already-aborted signal fails as aborted without a driver', async () => {
	const db = new DbConnection(unknown)
	const controller = new AbortController()
	controller.abort()
	const r = await db.execTxDynamic(async () => {}, { signal: controller.signal })
	assert.equal(r.ok, false)
	assert.equal((r.error as TDbError).kind, 'aborted')
})

// review24 2.1: version()/ping() must also fail as 'aborted' on an already-aborted signal
// without loading a driver - a missing driver (unknown kind) would otherwise surface as 'connect'.
test('version/ping with an already-aborted signal fail as aborted without a driver', async () => {
	const db = new DbConnection(unknown)
	const controller = new AbortController()
	controller.abort()
	const v = await db.version({ signal: controller.signal })
	assert.equal(v.ok, false)
	assert.equal((v.error as TDbError).kind, 'aborted')
	const p = await db.ping({ signal: controller.signal })
	assert.equal(p.ok, false)
	assert.equal((p.error as TDbError).kind, 'aborted')
})

// review5 2.10: a negative / NaN / infinite silence deadline is a programming error and
// must fail up front with a TypeError, exactly like execTxDynamic's timeout validation -
// not be reported as a 'query' finish event.
test('execStream rejects with a TypeError on a negative, NaN or infinite silence timeout', async () => {
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	for (const options of [{ firstDataTimeoutMs: -1 }, { idleTimeoutMs: NaN }, { idleTimeoutMs: Infinity }]) {
		await assert.rejects(
			db.execStream('SELECT 1', [], () => {}, options),
			/must be a non-negative finite number of milliseconds/,
		)
	}
})

// review6 1.1: exec() is a TResult-returning convenience over execStream, so a bad
// silence deadline must reject from it too, not fold into a { ok: false, query } result.
test('exec rejects with a TypeError on a negative, NaN or infinite silence timeout', async () => {
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	for (const options of [{ firstDataTimeoutMs: -1 }, { idleTimeoutMs: NaN }, { idleTimeoutMs: Infinity }]) {
		await assert.rejects(
			db.exec('SELECT 1', [], options),
			/must be a non-negative finite number of milliseconds/,
		)
	}
})

// review6 2.1: a bad connectTimeout is a programming error too — it must reject at
// construction, not surface later as a 'connect' error after a driver has been loaded.
test('constructor rejects with a TypeError on a negative, NaN or infinite connectTimeout', () => {
	for (const connectTimeout of [-1, NaN, Infinity]) {
		assert.throws(
			() => new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p', connectTimeout } as TConnPg),
			/must be a non-negative finite number of milliseconds/,
		)
	}
})

// review3 §2.4: the Oracle version fallback (v$version -> PRODUCT_COMPONENT_VERSION) must
// only run when the primary query failed as a query. A timeout or abort would just repeat
// the same failure against the fallback, so it must return the original error untouched.
test('version does not fall back to the second query on a timeout failure', async (t) => {
	const db = new DbConnection({ kind: 'ora', host: '127.0.0.1', service: 'x', user: 'u', password: 'p' } as TConnOra)
	const exec = t.mock.method(db, 'exec', async () =>
		({ ok: false, error: { message: 'TIMEOUT', kind: 'timeout' } }) as TResult<TDbRes[], TDbError>,
	)
	const v = await db.version()
	assert.equal(v.ok, false)
	assert.equal(v.error.kind, 'timeout')
	assert.equal(exec.mock.callCount(), 1)
})

test('version still falls back to the second query on a query failure', async (t) => {
	const db = new DbConnection({ kind: 'ora', host: '127.0.0.1', service: 'x', user: 'u', password: 'p' } as TConnOra)
	const exec = t.mock.method(db, 'exec', async () =>
		({ ok: false, error: { message: 'insufficient privileges', kind: 'query' } }) as TResult<TDbRes[], TDbError>,
	)
	await db.version()
	assert.equal(exec.mock.callCount(), 2)
})

// review25 2.1: an abort that fires between the primary query failure and the fallback
// must not run the fallback - it would come back 'aborted' and get folded into the primary
// 'query' error as a confusing "fallback also failed" message.
test('version reports aborted when the signal aborts between the primary and the fallback', async (t) => {
	const db = new DbConnection({ kind: 'ora', host: '127.0.0.1', service: 'x', user: 'u', password: 'p' } as TConnOra)
	const controller = new AbortController()
	let calls = 0
	const exec = t.mock.method(db, 'exec', async () => {
		calls += 1
		if (calls === 1) {
			controller.abort()
			return { ok: false, error: { message: 'insufficient privileges', kind: 'query' } } as TResult<TDbRes[], TDbError>
		}
		return { ok: false, error: { message: 'fallback failed', kind: 'query' } } as TResult<TDbRes[], TDbError>
	})
	const v = await db.version({ signal: controller.signal })
	assert.equal(v.ok, false)
	assert.equal((v.error as TDbError).kind, 'aborted')
	assert.equal(calls, 1)
	assert.equal(exec.mock.callCount(), 1)
})

// review8 §2.8/2.12 (deferred): version/ping used to load the driver before validating the
// silence deadlines, so an unknown kind + bad timeout surfaced as 'connect' instead of the
// TypeError it is. The deadlines must be validated before _adapter().
test('version/ping reject a bad timeout as a TypeError before loading the driver', async () => {
	const db = new DbConnection(unknown)
	await assert.rejects(() => db.version({ firstDataTimeoutMs: -1 }), TypeError)
	await assert.rejects(() => db.ping({ idleTimeoutMs: NaN }), TypeError)
})

// review8 §2.13 (deferred): when both the primary and the fallback version query fail, the
// fallback's own reason was dropped. It must be kept in the message.
test('version reports both failures when the fallback also fails', async (t) => {
	const db = new DbConnection({ kind: 'ora', host: '127.0.0.1', service: 'x', user: 'u', password: 'p' } as TConnOra)
	t.mock.method(db, 'exec', async () =>
		({ ok: false, error: { message: 'insufficient privileges', kind: 'query' } }) as TResult<TDbRes[], TDbError>,
	)
	const v = await db.version()
	assert.equal(v.ok, false)
	assert.match(v.error.message, /fallback also failed/)
})

// review22 2.2: a NULL in the version column is "no version", not the string "null".
test('version reports NO_VERSION when the version value is NULL', async (t) => {
	const db = new DbConnection({ kind: 'ora', host: '127.0.0.1', service: 'x', user: 'u', password: 'p' } as TConnOra)
	t.mock.method(db, 'exec', async () =>
		({ ok: true, result: [{ schema: [], rows: [{ VERSION: null }] }] }) as TResult<TDbRes[], TDbError>,
	)
	const v = await db.version()
	assert.equal(v.ok, false)
	assert.equal((v.error as TDbError).code, 'NO_VERSION')
})

// review4 §2.6: execTxDynamic validated its timeout after loading the driver, so an invalid
// timeout on an unknown kind surfaced as a connect error instead of the TypeError the docs
// promise. The validation must run first and reject before any driver is touched.
test('execTxDynamic rejects an invalid timeout before the driver is loaded', async () => {
	const db = new DbConnection(unknown)
	await assert.rejects(db.execTxDynamic(async () => {}, -5), /non-negative/)
	await assert.rejects(db.execTxDynamic(async () => {}, NaN), /non-negative/)
	await assert.rejects(db.execTxDynamic(async () => {}, Infinity), /non-negative/)
})

// review4 §2.8: close() awaited _adapterPromise bare, so a load that failed while close() was
// in flight rejected the close itself. It must swallow a failed adapter load.
test('close() does not reject when a concurrent adapter load fails', async () => {
	const db = new DbConnection(unknown)
	const execP = db.exec('SELECT 1', []) // sets _adapterPromise synchronously, then rejects
	await db.close() // must not throw
	await execP // settles on its own ({ ok: false, connect })
})

// review4 §2.9: rowsAffected/outParams may legally arrive with no preceding schema (a DML
// statement, an out-only bind); exec() used to drop them. They must attach to an empty result.
test('exec() keeps rowsAffected/outParams that arrive before any schema', async (t) => {
	const db = new DbConnection(unknown)
	t.mock.method(db, 'execStream', async (_q: string, _p: TDbParam[], cb: (e: TDbStreamEvent) => void) => {
		cb({ kind: 'rowsAffected', count: 5 })
		cb({ kind: 'outParams', params: { n: 1 } })
		cb({ kind: 'finish' })
	})
	const r = await db.exec('UPDATE t SET x = 1', [])
	assert.equal(r.ok, true)
	if (!r.ok) return
	assert.equal(r.result.length, 1)
	assert.deepEqual(r.result[0]!.schema, [])
	assert.equal(r.result[0]!.rowsAffected, 5)
	assert.deepEqual(r.result[0]!.outParams, { n: 1 })
})

// review11 §2.1: invalid Oracle pool sizing must reject at construction (TypeError), the
// same contract as connectTimeout - before any driver is loaded. It must not surface
// later as a 'connect' error.
test('DbConnection rejects invalid Oracle pool sizing at construction', () => {
	const base = { kind: 'ora', host: '127.0.0.1', service: 'x', user: 'u', password: 'p' } as TConnOra
	assert.throws(() => new DbConnection({ ...base, poolMin: -1 }), TypeError)
	assert.throws(() => new DbConnection({ ...base, poolMax: 0 }), TypeError)
	assert.throws(() => new DbConnection({ ...base, poolIncrement: 0 }), TypeError)
	assert.throws(() => new DbConnection({ ...base, queueTimeout: -1 }), TypeError)
	assert.throws(() => new DbConnection({ ...base, poolMin: 5, poolMax: 2 }), TypeError)
})

// A fake adapter that records every query it is asked to run, so the liveness ping
// (SELECT 1 before a query that follows a long idle) is observable by its query text.
function makeFakeAdapter(calls: string[], pingImpl?: (cb: (e: TDbStreamEvent) => void) => void): IDbAdapter {
	return {
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream: async (query: string, _p: TDbParam[], cb: (e: TDbStreamEvent) => void) => {
			calls.push(query)
			if (query === 'SELECT 1 AS p' && pingImpl) { pingImpl(cb); return }
			cb({ kind: 'finish' })
		},
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	}
}

// liveness ping: a query that follows a long idle is preceded by a SELECT 1 with its
// own short deadline, so a half-open socket (server/NAT dropped the idle connection
// without the client noticing) is torn down before the real query goes out - instead of
// the real query hanging until its first-data deadline.
test('a query after a long idle runs a liveness ping first', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => makeFakeAdapter(calls)

	await db.exec('SELECT 1', [])
	assert.deepEqual(calls, ['SELECT 1'], 'a fresh connection must not be pinged')

	now = 31_000 // idle past the 30s threshold
	await db.exec('SELECT 2', [])
	assert.deepEqual(calls, ['SELECT 1', 'SELECT 1 AS p', 'SELECT 2'], 'the real query must be preceded by a liveness ping')
})

// Frequent queries stay under the idle threshold: no ping between them.
test('no liveness ping when queries are frequent', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => makeFakeAdapter(calls)

	await db.exec('SELECT 1', [])
	now = 5_000 // well under the 30s threshold
	await db.exec('SELECT 2', [])
	assert.deepEqual(calls, ['SELECT 1', 'SELECT 2'])
})

// A liveness ping that fails (a dead socket surfaced as a driver error) must not fail
// the caller's real query: the ping error is swallowed, and the query runs regardless.
test('a failing liveness ping does not fail the real query', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => makeFakeAdapter(calls, () => { throw new Error('socket hangup') })

	now = 31_000 // idle past the 30s threshold
	const r = await db.exec('SELECT 2', [])
	assert.equal(r.ok, true, 'the ping failure must be swallowed, not surfaced')
	assert.deepEqual(calls, ['SELECT 1 AS p', 'SELECT 2'])
})

// An adapter that fails one specific query with a structured error, to exercise the
// idle-clock reset on timeout/connect vs. keep on query errors.
function makeFailingAdapter(calls: string[], failOn: string, error: TDbError): IDbAdapter {
	return {
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream: async (query: string, _p: TDbParam[], cb: (e: TDbStreamEvent) => void) => {
			calls.push(query)
			if (query === failOn) { cb({ kind: 'finish', error }); return }
			cb({ kind: 'finish' })
		},
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	}
}

// A timeout leaves the connection in an unknown state (it may be the half-open socket the
// ping is meant to catch), so the idle clock is reset and the very next query pings even
// though no wall-clock idle passed.
test('a timeout resets the idle clock so the next query pings', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => makeFailingAdapter(calls, 'SELECT 1', { message: 'dead socket', kind: 'timeout', code: 'TIMEOUT' })

	const r1 = await db.exec('SELECT 1', [])
	assert.equal(r1.ok, false)
	assert.deepEqual(calls, ['SELECT 1'], 'a fresh connection must not be pinged before the failing query')

	const r2 = await db.exec('SELECT 2', [])
	assert.equal(r2.ok, true)
	assert.deepEqual(calls, ['SELECT 1', 'SELECT 1 AS p', 'SELECT 2'], 'the next query must ping after a timeout')
})

// A query error means the server answered: the connection is known-good, so the idle clock
// is NOT reset and the next query skips the ping.
test('a query error keeps the idle clock (no ping on the next query)', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => makeFailingAdapter(calls, 'SELECT 1', { message: 'syntax error', kind: 'query', code: '2627' })

	await db.exec('SELECT 1', [])
	await db.exec('SELECT 2', [])
	assert.deepEqual(calls, ['SELECT 1', 'SELECT 2'])
})

// A liveness ping that itself times out (the half-open socket the ping is meant to catch)
// flags the connection suspect. A later successful query clears that suspicion: a live
// query is the best proof there is, and relying only on the ping would loop forever on a
// slow-but-healthy connection. So the query after a success skips the ping.
test('a failed liveness ping flags suspect; a successful query clears it', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () =>
		makeFailingAdapter(calls, 'SELECT 1 AS p', { message: 'dead socket', kind: 'timeout', code: 'TIMEOUT' })

	now = 31_000 // idle past the 30s threshold: first query triggers a ping, which fails
	const r1 = await db.exec('SELECT 2', [])
	assert.equal(r1.ok, true)
	assert.deepEqual(calls, ['SELECT 1 AS p', 'SELECT 2'], 'the idle clock fires a ping; it fails and the query still runs')

	// The query above succeeded, so the suspicion the failed ping left behind is cleared:
	// the next query does not ping again without further idle.
	const r2 = await db.exec('SELECT 3', [])
	assert.equal(r2.ok, true)
	assert.deepEqual(calls, ['SELECT 1 AS p', 'SELECT 2', 'SELECT 3'], 'a successful query clears the suspicion (no re-ping)')
})

// An aborted query left the connection in an unknown state too (the caller cancelled
// precisely because the query looked stuck - the half-open signature): the next query
// pings. The adapter's query is stuck until it is released, so the abort is the only way
// the call ends; only once the adapter call settles is its reserved slot freed for a ping.
test('an aborted query marks the connection suspect (next query pings)', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	let querySent!: () => void
	let releaseQuery!: () => void
	const sent = new Promise<void>((resolve) => { querySent = resolve })
	const gate = new Promise<void>((resolve) => { releaseQuery = resolve })
	const hanging = (): IDbAdapter => ({
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream: async (query: string, _p: TDbParam[], cb: (e: TDbStreamEvent) => void) => {
			calls.push(query)
			if (query === 'SELECT 1') { querySent(); await gate; cb({ kind: 'finish' }); return }
			cb({ kind: 'finish' })
		},
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	})
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => hanging()

	const ac = new AbortController()
	const p = db.exec('SELECT 1', [], { signal: ac.signal })
	await sent // the query is actually out on the (stuck) connection before we cancel
	ac.abort()
	const r1 = await p
	assert.equal(r1.ok, false)
	assert.equal(r1.error?.kind, 'aborted')

	// The adapter call now settles (its teardown finishes): only then is the reserved slot
	// released, and the next query pings on the still-suspect connection. The release only
	// resolves the adapter's gate; give the settle (and its run.then decrement) a turn before
	// the next query, which would otherwise start while the slot is still held.
	releaseQuery()
	await new Promise((resolve) => setTimeout(resolve, 0))
	await db.exec('SELECT 2', [])
	assert.deepEqual(calls, ['SELECT 1', 'SELECT 1 AS p', 'SELECT 2'], 'an abort must leave the connection suspect, so the next query pings')
})

// A ping whose finish arrived must not wait out the adapter's teardown: a driver stuck in
// conn.close()/end()/detach() after the finish was already delivered must not hold the
// caller (the 30s hard deadline is only for a finish that never comes).
test('a ping ends on its finish even if the teardown hangs', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	const hangingTeardown = (): IDbAdapter => ({
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream: async (query: string, _p: TDbParam[], cb: (e: TDbStreamEvent) => void) => {
			calls.push(query)
			if (query === 'SELECT 1 AS p') { cb({ kind: 'finish' }); await new Promise<never>(() => {}) }
			cb({ kind: 'finish' })
		},
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	})
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => hangingTeardown()

	now = 31_000 // idle past the 30s threshold: a ping fires
	const start = Date.now()
	const r = await db.exec('SELECT 2', [])
	assert.equal(r.ok, true)
	assert.ok(Date.now() - start < 1000, 'the ping must not wait out a hanging teardown')
	assert.deepEqual(calls, ['SELECT 1 AS p', 'SELECT 2'])
})

// The root fix: a query that produces no first event must be bounded by the first-data
// deadline (the very symptom the incident showed), not by the adapter's silence.
test('a query that never emits data times out on the first-data deadline', async () => {
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => ({
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream: async () => { await new Promise<never>(() => {}) },
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	})
	const r = await db.exec('SELECT 1', [], { firstDataTimeoutMs: 50 })
	assert.equal(r.ok, false)
	assert.equal(r.error?.kind, 'timeout')
})

// While another query is in flight the connection is demonstrably alive, so a long idle on
// the clock must not trigger a ping: long streams would otherwise look "idle" and every
// concurrent query would pile a ping on top of them (and at pool saturation self-perpetuate).
test('no liveness ping while another query is in flight', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	let sent1!: () => void
	let release1!: () => void
	const sent = new Promise<void>((resolve) => { sent1 = resolve })
	const gate1 = new Promise<void>((resolve) => { release1 = resolve })
	const slowFirst = (): IDbAdapter => ({
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream: async (query: string, _p: TDbParam[], cb: (e: TDbStreamEvent) => void) => {
			calls.push(query)
			if (query === 'SELECT 1') { sent1(); await gate1; cb({ kind: 'finish' }); return }
			cb({ kind: 'finish' })
		},
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	})
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => slowFirst()

	const p1 = db.exec('SELECT 1', []) // stays in flight
	await sent // SELECT 1 is actually out on the connection (_active > 0)
	now = 31_000 // idle past the threshold, but a query is in flight
	const r2 = await db.exec('SELECT 2', [])
	assert.equal(r2.ok, true)
	assert.deepEqual(calls, ['SELECT 1', 'SELECT 2'], 'no ping while a query is in flight')
	release1()
	await p1
})

// A suspicion does not override the in-flight guard: while another query runs, no ping is
// started even under suspicion - the ping probes the pool's *next* socket, not the one the
// running query holds, so it would test the wrong thing. The suspicion is kept and pings
// once the connection is free again.
test('no ping under suspicion while another query is in flight', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	let sentA!: () => void
	let releaseA!: () => void
	const sent = new Promise<void>((r) => { sentA = r })
	const gateA = new Promise<void>((r) => { releaseA = r })
	const adapter = (): IDbAdapter => ({
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream: async (query: string, _p: TDbParam[], cb: (e: TDbStreamEvent) => void) => {
			calls.push(query)
			if (query === 'SELECT A') { sentA(); await gateA; cb({ kind: 'finish', error: { message: 'dead', kind: 'timeout', code: 'TIMEOUT' } }); return }
			if (query === 'SELECT B') { cb({ kind: 'finish', error: { message: 'dead', kind: 'timeout', code: 'TIMEOUT' } }); return }
			cb({ kind: 'finish' })
		},
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	})
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => adapter()

	// A goes out first and stays in flight (blocked on gateA).
	const pA = db.exec('SELECT A', [])
	await sent // A is actually out on the connection (_active > 0)
	// B runs while A is in flight and fails -> the connection becomes suspect.
	const rB = await db.exec('SELECT B', [])
	assert.equal(rB.ok, false)
	// C faces suspicion while A is still in flight: no ping, C runs at once.
	now = 31_000 // idle clock past the threshold too
	const rC = await db.exec('SELECT C', [])
	assert.equal(rC.ok, true)
	assert.deepEqual(calls, ['SELECT A', 'SELECT B', 'SELECT C'], 'no ping under suspicion while a query is in flight')
	// A finally fails and the connection is free: the kept suspicion now forces a ping.
	releaseA()
	await pA
	await db.exec('SELECT D', [])
	assert.equal(calls.filter((q) => q === 'SELECT 1 AS p').length, 1, 'the kept suspicion pings once the connection is free')
})

// A ping that never answers (the half-open socket itself) must be cut off by its own 5s
// deadline, not wait out the 30s hard deadline: the timeout releases the waiter on the spot
// even though the abort it sends out is ignored and the adapter never resolves.
test('a never-answering liveness ping is cut off at its 5s deadline', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	const adapter = (): IDbAdapter => ({
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream: async (query: string, _p: TDbParam[], cb: (e: TDbStreamEvent) => void) => {
			calls.push(query)
			if (query === 'SELECT 1 AS p') { await new Promise<never>(() => {}) } // hang forever
			cb({ kind: 'finish' })
		},
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	})
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => adapter()

	now = 31_000 // idle: the ping fires and never answers
	const start = Date.now()
	const r = await db.exec('SELECT 2', [])
	const elapsed = Date.now() - start
	assert.equal(r.ok, true, 'the ping timeout must be swallowed, the real query runs')
	assert.ok(elapsed < 10_000, `the ping must be cut off at 5s, waited ${elapsed}ms`)
})

// A ping answered with a query error is NOT proof of health: the probe could not be
// served (bad SQL, permissions, a dialect quirk in the ping itself), so the ping counts as
// "not alive" and leaves the connection suspect. The real query that follows still runs,
// and only its success clears the suspicion.
test('a ping answered with a query error leaves the connection suspect', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	let sent!: () => void
	let release!: () => void
	const sentP = new Promise<void>((r) => { sent = r })
	const gate = new Promise<void>((r) => { release = r })
	const adapter = (): IDbAdapter => ({
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream: async (query: string, _p: TDbParam[], cb: (e: TDbStreamEvent) => void) => {
			calls.push(query)
			if (query === 'SELECT 1 AS p') { cb({ kind: 'finish', error: { message: 'permission denied', kind: 'query', code: '42501' } }); return }
			if (query === 'SELECT 2') { sent(); await gate; cb({ kind: 'finish' }); return }
			cb({ kind: 'finish' })
		},
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	})
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => adapter()

	now = 31_000
	const p = db.exec('SELECT 2', [])
	await sentP // the ping (query error) ran, and the real query is now out on the connection
	assert.equal((db as unknown as { _suspect: boolean })._suspect, true, 'a query-error ping answer leaves the connection suspect')
	release()
	const r = await p
	assert.equal(r.ok, true)
	assert.equal((db as unknown as { _suspect: boolean })._suspect, false, 'the successful query clears the suspicion')
})

// A cancellation while the adapter is still loading (driver import, first pool connect) must
// end the call as aborted now, not wait out the load and then mislabel its outcome.
test('an abort while the adapter is still loading ends the call as aborted', async (t) => {
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	let started!: () => void
	const loadStarted = new Promise<void>((r) => { started = r })
	// A load that never settles: the abort must cut THIS waiter short; the load keeps going.
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => {
		started()
		await new Promise<never>(() => {})
		throw new Error('unreachable')
	}
	const controller = new AbortController()
	const p = db.exec('SELECT 1', [], { signal: controller.signal })
	await loadStarted
	controller.abort()
	const r = await p
	assert.equal(r.ok, false)
	assert.equal(r.error?.kind, 'aborted')
})

// A second caller arriving while a ping is in flight joins the SAME ping instead of
// starting a second probe: the shared promise is awaited by every caller, and only the
// first (the sole admitted one) started it.
test('a second caller joins the in-flight ping instead of starting its own', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	let pingStarted!: () => void
	let releasePing!: () => void
	const started = new Promise<void>((r) => { pingStarted = r })
	const gate = new Promise<void>((r) => { releasePing = r })
	const adapter = (): IDbAdapter => ({
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream: async (query: string, _p: TDbParam[], cb: (e: TDbStreamEvent) => void) => {
			calls.push(query)
			if (query === 'SELECT 1 AS p') { pingStarted(); await gate; cb({ kind: 'finish' }); return }
			cb({ kind: 'finish' })
		},
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	})
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => adapter()

	now = 31_000 // idle: the first call starts the ping and waits on it
	const p1 = db.exec('SELECT 2', [])
	await started // the ping is in flight and p1 is waiting on it
	const p2 = db.exec('SELECT 3', []) // a second call arrives mid-ping
	releasePing() // the ping answers
	const [r1, r2] = await Promise.all([p1, p2])
	assert.equal(r1.ok, true)
	assert.equal(r2.ok, true)
	assert.deepEqual(calls, ['SELECT 1 AS p', 'SELECT 2', 'SELECT 3'], 'only one ping is shared by both callers')
})

// A cancellation that fires while the call waits on a shared ping must end the call as
// aborted and must NOT run the real query on a signal the caller already gave up on.
test('a cancel while waiting on the ping ends the call as aborted (no query)', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	let pingStarted!: () => void
	let releasePing!: () => void
	const started = new Promise<void>((r) => { pingStarted = r })
	const gate = new Promise<void>((r) => { releasePing = r })
	const adapter = (): IDbAdapter => ({
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream: async (query: string, _p: TDbParam[], cb: (e: TDbStreamEvent) => void) => {
			calls.push(query)
			if (query === 'SELECT 1 AS p') { pingStarted(); await gate; cb({ kind: 'finish' }); return }
			cb({ kind: 'finish' })
		},
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	})
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => adapter()

	now = 31_000 // idle: a ping starts and the call waits on it
	const ac = new AbortController()
	const p = db.exec('SELECT 2', [], { signal: ac.signal })
	await started // the call is now waiting on the ping
	ac.abort()
	const r = await p
	assert.equal(r.ok, false)
	assert.equal(r.error?.kind, 'aborted')
	assert.deepEqual(calls, ['SELECT 1 AS p'], 'the real query must not run after a cancel during the ping')
	releasePing() // let the orphaned ping finish and its shared promise settle
})

// A ping whose adapter call never settles (a driver stuck in cancel past the 5s deadline)
// must not be followed by a second probe over the still-running one: the probe's lifetime
// is the adapter promise, not the 5s wait, so a degraded connection does not pile probes.
test('a timed-out ping does not spawn a second probe over the unfinished one', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	const adapter = (): IDbAdapter => ({
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream: async (query: string, _p: TDbParam[], cb: (e: TDbStreamEvent) => void) => {
			calls.push(query)
			if (query === 'SELECT 1 AS p') { await new Promise<never>(() => {}) } // hang forever
			cb({ kind: 'finish' })
		},
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	})
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => adapter()

	now = 31_000
	const r1 = await db.exec('SELECT 2', []) // pings, times out at 5s, the query still runs
	assert.equal(r1.ok, true)
	assert.equal(calls.filter((q) => q === 'SELECT 1 AS p').length, 1, 'the idle clock fired exactly one ping')

	// The timed-out ping's adapter call is still unfinished (hanging forever): the next
	// query must not start a second probe over it.
	await db.exec('SELECT 3', [])
	assert.equal(calls.filter((q) => q === 'SELECT 1 AS p').length, 1, 'no second probe while the first is still unfinished')
})

// A query that started BEFORE a suspicion was raised must not clear it when it later
// succeeds: the success predates the evidence of trouble, so it says nothing about it.
test('a success started before the suspicion does not clear it', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	let sentX!: () => void
	let releaseX!: () => void
	const sent = new Promise<void>((r) => { sentX = r })
	const gateX = new Promise<void>((r) => { releaseX = r })
	const adapter = (): IDbAdapter => ({
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream: async (query: string, _p: TDbParam[], cb: (e: TDbStreamEvent) => void) => {
			calls.push(query)
			if (query === 'SELECT X') { sentX(); await gateX; cb({ kind: 'finish' }); return }
			if (query === 'SELECT Y') { cb({ kind: 'finish', error: { message: 'dead', kind: 'timeout', code: 'TIMEOUT' } }); return }
			cb({ kind: 'finish' })
		},
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	})
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => adapter()

	const pX = db.exec('SELECT X', []) // starts first, hangs
	await sent // X is out on the connection (before the suspicion)
	const rY = await db.exec('SELECT Y', []) // fails while X is in flight -> suspect
	assert.equal(rY.ok, false)
	releaseX() // X succeeds AFTER the suspicion was raised
	const rX = await pX
	assert.equal(rX.ok, true)

	// X's success predates Y's failure: it must not clear the suspicion, so the next query
	// pings.
	await db.exec('SELECT Z', [])
	assert.equal(calls.filter((q) => q === 'SELECT 1 AS p').length, 1, 'a success that predates the suspicion must not clear it')
})

// A query that times out keeps its slot until the adapter call settles, not until the
// public call returns: the adapter may still be tearing the socket down and holding a pool
// slot, and a concurrent ping would test the wrong socket while it does. Symmetric with the
// probe's flight slot (_probesInFlight).
test('a timed-out query holds its slot until the adapter call settles', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	const adapter = (): IDbAdapter => ({
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream: async (query: string, _p: TDbParam[], cb: (e: TDbStreamEvent) => void) => {
			calls.push(query)
			if (query === 'SELECT X') { await new Promise<never>(() => {}) } // hang forever
			cb({ kind: 'finish' })
		},
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	})
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => adapter()

	const r1 = await db.exec('SELECT X', [], { firstDataTimeoutMs: 5_000 })
	assert.equal(r1.ok, false)
	assert.equal(r1.error?.kind, 'timeout')

	// The adapter call for SELECT X is still hanging: its slot is held until it settles, so
	// the next query must not start a ping over it - it runs straight away.
	await db.exec('SELECT Y', [])
	assert.deepEqual(calls, ['SELECT X', 'SELECT Y'], 'no ping while the timed-out query still holds its slot')
})

// A probe that timed out and whose adapter call hangs (a driver stuck in cancel) must not
// block close(): the probe's own deadline already released its waiter, and close() tears
// the whole connection down rather than waiting on the orphaned probe.
test('close() returns while a timed-out probe still hangs', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	const adapter = (): IDbAdapter => ({
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream: async (query: string, _p: TDbParam[], cb: (e: TDbStreamEvent) => void) => {
			calls.push(query)
			if (query === 'SELECT 1 AS p') { await new Promise<never>(() => {}) } // hang forever
			cb({ kind: 'finish' })
		},
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	})
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => adapter()

	now = 31_000
	const r = await db.exec('SELECT 2', []) // pings, times out at 5s, query runs
	assert.equal(r.ok, true)

	// The probe's adapter call is still hanging. close() must not wait on it: race it
	// against a real timer (performance.now is mocked, so the wall clock is the timer).
	await Promise.race([
		db.close(),
		new Promise((_, reject) => setTimeout(() => reject(new Error('close() waited on the orphaned probe')), 2_000)),
	])
})

// The slot is released only after BOTH the outcome is published and the adapter call has
// settled. A successful finish does not settle the adapter call here: the query ends for
// the caller, but the gated adapter call still holds the slot, so the next query must not
// start a ping over it; once the gate opens and the call settles, the ping is allowed.
test('a finished query holds its slot until the adapter call settles', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	let release!: () => void
	const gate = new Promise<void>((r) => { release = r })
	const adapter = (): IDbAdapter => ({
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream: async (query: string, _p: TDbParam[], cb: (e: TDbStreamEvent) => void) => {
			calls.push(query)
			if (query === 'SELECT A') { cb({ kind: 'finish' }); await gate; return }
			cb({ kind: 'finish' })
		},
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	})
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => adapter()

	const rA = await db.exec('SELECT A', []) // finishes, but its adapter call stays gated
	assert.equal(rA.ok, true)

	// Idle now, but A still holds its slot: the next query runs straight away, no ping.
	now = 40_000
	await db.exec('SELECT B', [])
	assert.deepEqual(calls, ['SELECT A', 'SELECT B'], 'no ping while the finished query still holds its slot')

	// A's adapter call settles, its slot is released; a later idle query pings.
	release()
	await new Promise((resolve) => setTimeout(resolve, 0))
	now = 90_000
	await db.exec('SELECT C', [])
	assert.equal(calls.filter((q) => q === 'SELECT 1 AS p').length, 1, 'the ping runs once the finished query has settled')
})

// A connect failure publishes its suspicion (via _markUsed in the finally) while the
// adapter call is still gated, so the next query - after the call settles - sees the
// suspicion and pings. This pins the order: suspicion before the slot is released.
test('a connect failure publishes suspicion before the slot is released', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	let release!: () => void
	const gate = new Promise<void>((r) => { release = r })
	const adapter = (): IDbAdapter => ({
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream: async (query: string, _p: TDbParam[], cb: (e: TDbStreamEvent) => void) => {
			calls.push(query)
			if (query === 'SELECT A') {
				cb({ kind: 'finish', error: { message: 'connection refused', kind: 'connect', code: 'ECONNREFUSED' } })
				await gate
				return
			}
			cb({ kind: 'finish' })
		},
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	})
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => adapter()

	const rA = await db.exec('SELECT A', [])
	assert.equal(rA.ok, false)
	assert.equal(rA.error?.kind, 'connect')
	// The suspicion is already published even though the adapter call is still gated.
	assert.equal((db as unknown as { _suspect: boolean })._suspect, true, 'the connect failure is published before the slot is released')

	release() // the failed query's adapter call settles, its slot is released
	await new Promise((resolve) => setTimeout(resolve, 0))

	await db.exec('SELECT B', [])
	assert.deepEqual(calls, ['SELECT A', 'SELECT 1 AS p', 'SELECT B'], 'the query after a settled connect failure pings on the suspect connection')
})

// A query hung in its driver (e.g. cancel on a dead socket) never settles its adapter call
// and would hold its slot forever, disabling the ping on every later query. The orphan
// grace releases the slot after ORPHAN_GRACE_MS (10 s), so liveness keeps working.
test('an orphaned query releases its slot after the grace so liveness is not disabled', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	const adapter = (): IDbAdapter => ({
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream: async (query: string, _p: TDbParam[], cb: (e: TDbStreamEvent) => void) => {
			calls.push(query)
			if (query === 'SELECT X') { await new Promise<never>(() => {}) } // hang forever
			cb({ kind: 'finish' })
		},
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	})
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _adapter: () => Promise<IDbAdapter>; _orphanGraceMs: number })._orphanGraceMs = 50
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => adapter()

	const r1 = await db.exec('SELECT X', [], { firstDataTimeoutMs: 50 })
	assert.equal(r1.ok, false)
	assert.equal(r1.error?.kind, 'timeout')

	// The orphan holds its slot for the (shrunk) grace, then releases it: the next query
	// pings instead of running blind into a connection that already timed out once.
	await new Promise((resolve) => setTimeout(resolve, 90))
	await db.exec('SELECT Y', [])
	assert.deepEqual(calls, ['SELECT X', 'SELECT 1 AS p', 'SELECT Y'], 'an orphaned query must not disable the ping forever')
})

// A probe whose adapter call never settles is released by the orphan grace, and its late
// settle must not release a newer probe's flight slot: the slot is owned per-probe (a
// counter), so P1 settling after its grace cannot silently drop P2's. This is the race
// from round 8 (a shared boolean let an old probe clear the next one's flag).
test('a late settle of an orphaned probe cannot release the next probe', async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	let resolveP1!: () => void
	let pingCount = 0
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	const dbAny = db as unknown as {
		_adapter: () => Promise<IDbAdapter>
		_orphanGraceMs: number
		_livenessTimeoutMs: number
		_probesInFlight: number
	}
	dbAny._livenessTimeoutMs = 20
	dbAny._orphanGraceMs = 60
	dbAny._adapter = async () => ({
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream: async (query: string, _p: TDbParam[], cb: (e: TDbStreamEvent) => void) => {
			calls.push(query)
			if (query === 'SELECT 1 AS p') {
				pingCount++
				if (pingCount === 1) { await new Promise<void>((r) => { resolveP1 = r }); return } // P1: hangs, no finish
				if (pingCount === 2) { await new Promise<never>(() => {}) } // P2: hangs forever
				cb({ kind: 'finish' }) // P3 and later: healthy
				return
			}
			cb({ kind: 'finish' })
		},
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	})

	// P1: the first idle query pings, the ping hangs and times out, then its grace starts.
	now = 31_000
	assert.equal((await db.exec('SELECT 1', [])).ok, true)
	assert.equal(dbAny._probesInFlight, 1, 'P1 is in flight until its grace passes')
	await new Promise((resolve) => setTimeout(resolve, 90))
	assert.equal(dbAny._probesInFlight, 0, 'P1 grace released its flight slot')

	// P2: a second idle query pings again; the ping hangs and times out but stays in flight
	// (its grace has not passed yet).
	now = 70_000
	assert.equal((await db.exec('SELECT 2', [])).ok, true)
	assert.equal(dbAny._probesInFlight, 1, 'P2 is in flight')

	// P1 finally settles, late. It must not release P2's slot.
	resolveP1()
	await new Promise((resolve) => setTimeout(resolve, 0))
	assert.equal(dbAny._probesInFlight, 1, 'P1 late settle must not release P2 flight slot')

	// P3 must not start while P2 is still in flight: the next query runs without a ping.
	now = 100_000
	await db.exec('SELECT 3', [])
	assert.equal(calls.filter((q) => q === 'SELECT 1 AS p').length, 2, 'no third ping while P2 is in flight')

	// Once P2's grace passes, liveness is unblocked and the next idle query pings.
	await new Promise((resolve) => setTimeout(resolve, 90))
	assert.equal(dbAny._probesInFlight, 0, 'P2 grace released its flight slot')
	now = 150_000
	await db.exec('SELECT 4', [])
	assert.equal(calls.filter((q) => q === 'SELECT 1 AS p').length, 3, 'P3 starts only after P2 grace')
})

// --- Contract: exactly one public finish, whatever the adapter does -----------------------
// The consumer must see a single terminal finish per execStream call, and a later event or
// settle must never re-answer (change the already-published outcome) or duplicate it. Each
// test drives the same DbConnection through its real execStream path with a fresh adapter.

const pgConn = (): TConnPg => ({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)

function countFinishes(events: TDbStreamEvent[]): number {
	return events.filter((e) => e.kind === 'finish').length
}

function stubAdapter(execStream: IDbAdapter['execStream']): IDbAdapter {
	return {
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream,
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	}
}

function quietDb(t: { mock: { method: (o: object, m: string, f: () => number) => unknown } }): DbConnection {
	t.mock.method(performance, 'now', () => 0) // never idle -> no liveness ping
	const db = new DbConnection(pgConn())
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => stubAdapter(adapterExec)
	return db
}

// The adapter is wired after quietDb (which stubs _adapter to return `adapterExec`), so the
// tests just set this variable to the behaviour under test before calling execStream.
let adapterExec: IDbAdapter['execStream'] = () => Promise.resolve()

test('a double finish is collapsed into one', async (t) => {
	adapterExec = async (_q, _p, cb) => { cb({ kind: 'finish' }); cb({ kind: 'finish' }) }
	const db = quietDb(t)
	const events: TDbStreamEvent[] = []
	await db.execStream('SELECT 1', [], (e) => events.push(e))
	assert.equal(countFinishes(events), 1, 'the second finish must be swallowed')
})

test('an adapter that resolves without a finish yields exactly one error finish', async (t) => {
	adapterExec = async () => {} // resolve, no finish
	const db = quietDb(t)
	const events: TDbStreamEvent[] = []
	await db.execStream('SELECT 1', [], (e) => events.push(e))
	assert.equal(countFinishes(events), 1)
	const fin = events.find((e) => e.kind === 'finish') as { error?: TDbError }
	assert.equal(fin.error?.kind, 'query')
	assert.match(fin.error?.message ?? '', /resolved without emitting/)
})

test('an adapter that rejects without a finish yields exactly one error finish', async (t) => {
	adapterExec = async () => { throw new Error('boom') }
	const db = quietDb(t)
	const events: TDbStreamEvent[] = []
	await db.execStream('SELECT 1', [], (e) => events.push(e))
	assert.equal(countFinishes(events), 1)
	assert.equal((events[0] as { error: TDbError }).error.kind, 'query')
})

test('a finish followed by a reject keeps the first (successful) outcome', async (t) => {
	adapterExec = async (_q, _p, cb) => { cb({ kind: 'finish' }); throw new Error('boom after finish') }
	const db = quietDb(t)
	const events: TDbStreamEvent[] = []
	await db.execStream('SELECT 1', [], (e) => events.push(e))
	assert.equal(countFinishes(events), 1)
	assert.equal((events[0] as { error?: TDbError }).error, undefined, 'the late reject must not re-answer the finish')
})

test('a synchronous throw from the adapter yields exactly one error finish', async (t) => {
	adapterExec = () => { throw new Error('sync boom') }
	const db = quietDb(t)
	const events: TDbStreamEvent[] = []
	await db.execStream('SELECT 1', [], (e) => events.push(e))
	assert.equal(countFinishes(events), 1)
	assert.equal((events[0] as { error: TDbError }).error.kind, 'query')
})

test('a consumer callback that throws on finish still yields a single finish', async (t) => {
	adapterExec = async (_q, _p, cb) => { cb({ kind: 'finish' }) }
	const db = quietDb(t)
	const events: TDbStreamEvent[] = []
	await db.execStream('SELECT 1', [], (e) => { events.push(e); throw new Error('consumer bug') })
	assert.equal(countFinishes(events), 1)
})

test('a late finish after a timeout does not re-answer the timeout outcome', async (t) => {
	let lateFinish!: () => void
	adapterExec = async (_q, _p, cb) => {
		await new Promise<void>((resolve) => { lateFinish = () => { cb({ kind: 'finish' }); resolve() } })
	}
	const db = quietDb(t)
	const events: TDbStreamEvent[] = []
	await db.execStream('SELECT X', [], (e) => events.push(e), { firstDataTimeoutMs: 50 })
	assert.equal(countFinishes(events), 1)
	assert.equal((events[0] as { error: TDbError }).error.kind, 'timeout')
	lateFinish() // the adapter finally emits its finish, after the timeout already won
	await new Promise((resolve) => setTimeout(resolve, 0))
	assert.equal(countFinishes(events), 1, 'the late finish must be swallowed, not published twice')
})

// A throw from liveness (before the inner try) must end the call as exactly one finish,
// never a bare rejection and never a duplicate - the surrounding catch shares the same
// settled guard as every other terminal path.
test('a throw from liveness ends the call as exactly one finish', async (t) => {
	t.mock.method(performance, 'now', () => 0)
	const db = new DbConnection(pgConn())
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => stubAdapter(async (_q, _p, cb) => cb({ kind: 'finish' }))
	;(db as unknown as { _ensureLiveness: () => Promise<void> })._ensureLiveness = async () => { throw new Error('liveness boom') }
	const events: TDbStreamEvent[] = []
	await db.execStream('SELECT 1', [], (e) => events.push(e))
	assert.equal(countFinishes(events), 1)
	assert.equal((events[0] as { error: TDbError }).error.kind, 'query')
})

// A cleanup error in the inner finally AFTER the finish is published must not re-answer it:
// _markUsed is wrapped so its throw is contained, the published outcome stands, and the
// structural slot guarantee still fires - a follow-up call gets the slot back.
test('a cleanup error after a published finish does not re-answer it', async (t) => {
	t.mock.method(performance, 'now', () => 0)
	const db = new DbConnection(pgConn())
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => stubAdapter(async (_q, _p, cb) => cb({ kind: 'finish' }))
	// _markUsed runs in the inner finally after the finish was published; make it throw to
	// prove the throw is contained and does not break the slot guarantee below it.
	;(db as unknown as { _markUsed: () => void })._markUsed = () => { throw new Error('cleanup boom') }
	const events: TDbStreamEvent[] = []
	await db.execStream('SELECT 1', [], (e) => events.push(e))
	assert.equal(countFinishes(events), 1)
	assert.equal((events[0] as { error?: TDbError }).error, undefined, 'the published success must stand')
	// The thrown cleanup must not leak the reserved slot: the structural guarantee still
	// releases it, so a follow-up call completes instead of hanging on the held slot. The
	// wait is bounded so a regression fails fast with a message rather than hanging CI.
	const events2: TDbStreamEvent[] = []
	const second = db.execStream('SELECT 2', [], (e) => events2.push(e))
	const hung = new Promise<never>((_, reject) => {
		const timer = setTimeout(() => reject(new Error('the follow-up call hung on the held slot')), 2000)
		second.then(() => clearTimeout(timer), () => clearTimeout(timer))
	})
	await Promise.race([second, hung])
	assert.equal(countFinishes(events2), 1)
})

// The try/catch around _markUsed must not only contain the throw but also let the orphan
// grace below it still arm: with a hung adapter call and a throwing _markUsed, the slot is
// released by the grace and the next query pings, exactly as when _markUsed does not throw.
// Without the wrap the throw skips outcomePublished/releaseSlot/grace, the slot leaks (_active
// never returns to 0), and the NEXT query still reaches the adapter but its liveness ping is
// blocked by the _active > 1 guard - so the observed regression is a missing ping (the
// expected array below), not a hang.
test('a throwing _markUsed does not break the orphan grace (H1 regression)', { timeout: 5000 }, async (t) => {
	let now = 0
	t.mock.method(performance, 'now', () => now)
	const calls: string[] = []
	const adapter = (): IDbAdapter => ({
		versionQuery: () => 'SELECT 1',
		pingQuery: () => 'SELECT 1 AS p',
		execStream: async (query: string, _p: TDbParam[], cb: (e: TDbStreamEvent) => void) => {
			calls.push(query)
			if (query === 'SELECT X') { await new Promise<never>(() => {}) } // hang forever
			cb({ kind: 'finish' })
		},
		execTxDynamic: async () => ({ ok: false, error: { message: 'n/a', kind: 'tx' } }),
		close: async () => {},
	})
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	;(db as unknown as { _orphanGraceMs: number })._orphanGraceMs = 50
	;(db as unknown as { _markUsed: () => void })._markUsed = () => { throw new Error('cleanup boom') }
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => adapter()

	const r1 = await db.exec('SELECT X', [], { firstDataTimeoutMs: 50 })
	assert.equal(r1.ok, false)
	assert.equal(r1.error?.kind, 'timeout')

	// The thrown _markUsed must not have aborted the orphan grace: after the (shrunk) grace
	// the slot is released and the next query pings instead of running blind. The ping fires
	// on the idle clock (the substituted _markUsed also skips _raiseSuspicion, so _suspect
	// stays false; now advances past the idle threshold instead).
	await new Promise((resolve) => setTimeout(resolve, 90))
	now = 60_000
	await db.exec('SELECT Y', [])
	assert.deepEqual(calls, ['SELECT X', 'SELECT 1 AS p', 'SELECT Y'], 'the orphan grace must still fire despite the throwing _markUsed')
})

// toDbError reads err.stack first (for an Error); a throwing stack getter must still leave
// exactly one finish via the surrounding catch's fallback, never a rejection with zero finish.
// The getter throws explicitly (rather than relying on V8's lazy stack formatting pulling the
// message getter, which is an engine detail), so the test is engine-independent; the access
// counter proves the fallback is reached through the throwing getter, not some default branch.
// Note: `hits > 0` is a change-detector on the property-read order inside toDbError. If that
// order ever changes (e.g. message before stack), a failure here means "the contract in
// helpers.ts changed", not "production broke" - so review the order before chasing a bug.
test('a throwing value from liveness still yields exactly one finish (H2 fallback)', { timeout: 5000 }, async (t) => {
	t.mock.method(performance, 'now', () => 0)
	const db = new DbConnection(pgConn())
	;(db as unknown as { _adapter: () => Promise<IDbAdapter> })._adapter = async () => stubAdapter(async (_q, _p, cb) => cb({ kind: 'finish' }))
	let hits = 0
	// Built without the Error constructor so the throwing stack getter is an own property
	// from the start: no defineProperty over an engine-created property (which assumes it is
	// configurable), and instanceof Error still holds through the prototype chain.
	const boom = Object.create(Error.prototype, {
		message: { value: 'x', configurable: true },
		stack: { get() { hits++; throw new Error('getter boom') } },
	})
	;(db as unknown as { _ensureLiveness: () => Promise<void> })._ensureLiveness = async () => { throw boom }
	const events: TDbStreamEvent[] = []
	await db.execStream('SELECT 1', [], (e) => events.push(e))
	assert.equal(events.length, 1)
	assert.equal(countFinishes(events), 1)
	assert.ok(hits > 0, 'the fallback must be reached via the throwing stack getter')
	assert.equal((events[0] as { error: TDbError }).error.kind, 'query')
	assert.equal((events[0] as { error: TDbError }).error.message, 'unknown error')
})
