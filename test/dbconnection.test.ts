import { test } from 'node:test'
import assert from 'node:assert/strict'
import { DbConnection } from '../src/db/index'
import type { TConnectionParam, TConnOra, TConnPg } from '../src/db/connection'
import type { TDbError, TDbParam, TDbRes, TDbStreamEvent } from '../src/db/types'
import type { TResult } from '../src/tresult'

const unknown = { kind: 'unknown' } as unknown as TConnectionParam

// review3 §2.2: a missing/unloadable driver (or an unknown kind) must surface through the
// normal result contract - a finish event for execStream, { ok: false } for the rest -
// never a bare rejection that escapes the DbConnection layer.
test('execStream ends with a finish event when the driver kind is unknown', async (t) => {
	const db = new DbConnection(unknown)
	const events: TDbStreamEvent[] = []
	await db.execStream('SELECT 1', [], (e) => events.push(e))
	assert.equal(events.length, 1)
	const e = events[0] as { kind: 'finish'; error: TDbError }
	assert.equal(e.kind, 'finish')
	assert.equal(e.error.kind, 'connect')
})

test('version/ping/execTxDynamic return { ok: false } when the driver kind is unknown', async (t) => {
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
