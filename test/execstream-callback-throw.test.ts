import { test, mock } from 'node:test'
import assert from 'node:assert/strict'
import sql from 'mssql'
import { DbConnection } from '../src/index'
import type { TConnMssql } from '../src/db/connection'
import type { TDbStreamEvent } from '../src/db/types'

// Regression guard for review2 1.5: the consumer's callback runs synchronously inside
// the driver's own event handlers (mssql request.on('row'), pg q.on('row'), ora
// stream.on('data'), fb sequentially). A throw from it must become the error of the
// final finish (plus an abort), never propagate into the driver's EventEmitter, where it
// would crash the process as an uncaughtException or corrupt the driver's state.
const conn: TConnMssql = {
	kind: 'mssql',
	server: '127.0.0.1',
	auth: { kind: 'mssql', user: 'u', password: 'p' },
}

class FakeRequest {
	stream = false
	private handlers = new Map<string, (...args: any[]) => void>()
	input(_name: string, _value: unknown): void {}
	on(event: string, cb: (...args: any[]) => void): void { this.handlers.set(event, cb) }
	cancel(): void {}
	query(_q: string): void {
		// Emit a row and then finish synchronously, exactly as tedious does inside its
		// own event handlers.
		this.handlers.get('row')?.({ id: 1 })
		this.handlers.get('done')?.()
	}
}

test('a throw from the row callback becomes a finish error, not an uncaughtException', async () => {
	mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) {
		return Promise.resolve(this)
	})
	mock.method(sql.ConnectionPool.prototype, 'request', function () {
		return new FakeRequest() as unknown as sql.Request
	})

	const db = new DbConnection(conn)
	const events: TDbStreamEvent[] = []
	await db.execStream('SELECT 1', [], (event) => {
		if (event.kind === 'row') throw new Error('consumer boom')
		events.push(event)
	})

	const finish = events.find((e): e is Extract<TDbStreamEvent, { kind: 'finish' }> => e.kind === 'finish')
	assert.ok(finish, 'a finish event must still be delivered after the throw')
	assert.equal(finish?.error?.message, 'consumer boom')
	assert.equal(finish?.error?.kind, 'query')
})

test('a throw from the finish callback itself is dropped, not re-thrown', async () => {
	mock.method(sql.ConnectionPool.prototype, 'connect', function (this: sql.ConnectionPool) {
		return Promise.resolve(this)
	})
	mock.method(sql.ConnectionPool.prototype, 'request', function () {
		return new FakeRequest() as unknown as sql.Request
	})

	const db = new DbConnection(conn)
	await db.execStream('SELECT 1', [], () => { throw new Error('always throws') })
	// Reaching here is the assertion: the throw on every event must not reject
	// execStream nor propagate into the driver.
})
