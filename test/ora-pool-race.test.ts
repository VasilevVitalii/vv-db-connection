import { test, mock } from 'node:test'
import assert from 'node:assert/strict'
import oracledb from 'oracledb'
import { OraAdapter } from '../src/db/adapters/ora'
import type { TConnOra } from '../src/db/connection'

// Regression guards for review2 2.1: the pool was created with `this._pool = await
// createPool(...)`, so two concurrent calls both saw an empty _pool and built two pools,
// one of which leaked forever. The pool is now cached as a promise, and close() waits
// for an in-flight createPool instead of leaving the eventual pool behind.

const conn: TConnOra = {
	kind: 'ora',
	host: '127.0.0.1',
	port: 1521,
	service: 'x',
	user: 'u',
	password: 'p',
}

const fakeConn = {
	execute: async () => ({}),
	close: async () => {},
	break: async () => {},
}

test('concurrent queries share one pool (createPool is called once)', async () => {
	let release!: () => void
	const gate = new Promise<unknown>((resolve) => { release = () => resolve(undefined) })
	let createPoolCalls = 0
	const fakePool = { getConnection: async () => fakeConn, close: async () => {} }
	mock.method(oracledb, 'createPool', () => {
		createPoolCalls += 1
		return gate.then(() => fakePool)
	})

	const adapter = new OraAdapter(conn)
	const p1 = adapter.execStream('UPDATE t SET x = 1', [], () => {})
	const p2 = adapter.execStream('UPDATE t SET x = 2', [], () => {})
	release()
	await Promise.all([p1, p2])

	assert.equal(createPoolCalls, 1)
})

test('close() during pool creation closes the eventual pool', async () => {
	let release!: () => void
	const gate = new Promise<unknown>((resolve) => { release = () => resolve(undefined) })
	let closed = 0
	const fakePool = { getConnection: async () => fakeConn, close: async () => { closed += 1 } }
	mock.method(oracledb, 'createPool', () => gate.then(() => fakePool))

	const adapter = new OraAdapter(conn)
	const p = adapter.execStream('UPDATE t SET x = 1', [], () => {})
	const closing = adapter.close()
	release()
	await p
	await closing

	assert.equal(closed, 1)
})
