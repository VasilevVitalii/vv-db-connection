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
	t.mock.method(sql.Request.prototype, 'query', () => Promise.resolve({ recordset: [], rowsAffected: [5] }))

	const adapter = new MssqlAdapter(conn)
	let affected: number | undefined
	const r = await adapter.execTxDynamic(async (exec) => {
		const res = await exec('UPDATE t SET x = 1', [])
		affected = res.rowsAffected
	})

	assert.equal(r.ok, true, r.ok ? '' : r.error.message)
	assert.equal(affected, 5)
})
