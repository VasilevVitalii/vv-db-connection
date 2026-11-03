import { test } from 'node:test'
import assert from 'node:assert/strict'
import { DbConnection } from '../src/db/index'
import type { TConnectionParam, TConnOra, TConnPg } from '../src/db/connection'
import type { TDbError, TDbRes, TDbStreamEvent } from '../src/db/types'
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

// review3 §2.3: a negative / NaN / infinite silence deadline is a programming error and
// must reject up front, not fire at once (or never) once the stream has started.
test('execStream rejects a negative, NaN or infinite silence timeout', async () => {
	const db = new DbConnection({ kind: 'pg', host: '127.0.0.1', database: 'x', user: 'u', password: 'p' } as TConnPg)
	await assert.rejects(db.execStream('SELECT 1', [], () => {}, { firstDataTimeoutMs: -1 }), /non-negative/)
	await assert.rejects(db.execStream('SELECT 1', [], () => {}, { idleTimeoutMs: NaN }), /non-negative/)
	await assert.rejects(db.execStream('SELECT 1', [], () => {}, { idleTimeoutMs: Infinity }), /non-negative/)
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
