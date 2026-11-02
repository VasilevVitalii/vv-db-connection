import { test, mock } from 'node:test'
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

test('execTxDynamic tears the pool down when the deadline fires during connect', async () => {
	let release!: () => void
	const gate = new Promise<void>((resolve) => { release = resolve })
	let closed = 0

	// connect() hangs until we release it — well past the 50ms deadline.
	mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) {
		return gate.then(() => this)
	})
	mock.method(sql.ConnectionPool.prototype, 'close', () => {
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
