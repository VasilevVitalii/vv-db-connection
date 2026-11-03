import { test } from 'node:test'
import assert from 'node:assert/strict'
import sql from 'mssql'
import { MssqlAdapter } from '../src/db/adapters/mssql'
import type { TConnMssql } from '../src/db/connection'

// Regression guard for review2 1.2: on abort, execStream must tear the borrowed
// connection down hard (destroy the socket), not just send an ATTN via request.cancel()
// and wait for a server response that may never come — the tarn slot would be stranded.
const conn: TConnMssql = {
	kind: 'mssql',
	server: '127.0.0.1',
	auth: { kind: 'mssql', user: 'u', password: 'p' },
}

class FakeRequest {
	stream = false
	_currentRequest?: { connection: { close(): void } }
	constructor(
		private readonly onClose: () => void,
		private readonly onCancel: () => void,
	) {}
	input(_name: string, _value: unknown): void {}
	on(_event: string, _cb: (...args: any[]) => void): void {}
	cancel(): void { this.onCancel() }
	query(_q: string): void {
		this._currentRequest = { connection: { close: this.onClose } }
	}
}

test('execStream tears the borrowed connection down hard on abort', async (t) => {
	let closed = 0
	let canceled = 0
	let request!: FakeRequest

	t.mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) {
		return Promise.resolve(this)
	})
	t.mock.method(sql.ConnectionPool.prototype, 'request', function () {
		request = new FakeRequest(() => { closed += 1 }, () => { canceled += 1 })
		return request as unknown as sql.Request
	})

	const adapter = new MssqlAdapter(conn)
	const controller = new AbortController()
	const run = adapter.execStream('SELECT 1', [], () => {}, controller.signal)

	// Let execStream borrow the (fake) connection and call query(), so that
	// request._currentRequest is set before the abort fires.
	await new Promise((resolve) => setImmediate(resolve))
	controller.abort()
	await run

	assert.equal(closed, 1, 'socket must be destroyed')
	assert.equal(canceled, 0, 'graceful ATTN cancel must not be used')
})

test('execStream falls back to cancel() when abort fires before the connection is borrowed', async (t) => {
	let closed = 0
	let canceled = 0
	let request!: FakeRequest

	t.mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) {
		return Promise.resolve(this)
	})
	t.mock.method(sql.ConnectionPool.prototype, 'request', function () {
		request = new FakeRequest(() => { closed += 1 }, () => { canceled += 1 })
		// Simulate an abort that lands before the acquire completes: no borrowed
		// connection, so _currentRequest stays undefined.
		request.query = () => {}
		return request as unknown as sql.Request
	})

	const adapter = new MssqlAdapter(conn)
	const controller = new AbortController()
	const run = adapter.execStream('SELECT 1', [], () => {}, controller.signal)

	await new Promise((resolve) => setImmediate(resolve))
	controller.abort()
	await run

	assert.equal(canceled, 1, 'must fall back to request.cancel()')
	assert.equal(closed, 0, 'no connection to destroy')
})
