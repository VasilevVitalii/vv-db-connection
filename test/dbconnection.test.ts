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
