import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createDbConnection } from '../src/index'
import { toDbError } from '../src/db/helpers'
import type { TConnMssql } from '../src/db/connection'

// A connection that is never actually opened: the cases below end before the adapter
// touches the network, so the fields only need to satisfy the type.
const mssql: TConnMssql = {
	kind: 'mssql',
	server: '127.0.0.1',
	auth: { kind: 'mssql', user: 'u', password: 'p' },
}

test('toDbError keeps the driver code (code / number / sqlState) and the kind', () => {
	assert.deepEqual(
		toDbError({ message: 'boom', code: '23505' }, 'query'),
		{ message: 'boom', code: '23505', kind: 'query' },
	)
	assert.deepEqual(
		toDbError({ message: 'deadlock', number: 1205 }, 'query'),
		{ message: 'deadlock', code: '1205', kind: 'query' },
	)
	assert.deepEqual(
		toDbError({ message: 'ora', sqlState: '72000' }, 'query'),
		{ message: 'ora', code: '72000', kind: 'query' },
	)
})

test('toDbError falls back to String(err) when there is no message', () => {
	assert.deepEqual(
		toDbError('plain string', 'connect'),
		{ message: 'plain string', kind: 'connect' },
	)
	assert.deepEqual(
		toDbError(undefined, 'connect'),
		{ message: 'undefined', kind: 'connect' },
	)
})

test('exec with an already-aborted signal ends as aborted without touching the database', async () => {
	const db = createDbConnection(mssql)
	const controller = new AbortController()
	controller.abort(new Error('caller cancelled'))
	const r = await db.exec('SELECT 1', [], { signal: controller.signal })
	assert.equal(r.ok, false)
	if (!r.ok) {
		assert.equal(r.error.kind, 'aborted')
		assert.equal(r.error.message, 'caller cancelled')
	}
})

test('execStream with an already-aborted signal emits a single aborted finish', async () => {
	const db = createDbConnection(mssql)
	const controller = new AbortController()
	controller.abort(new Error('cancelled'))
	const events: string[] = []
	let error: { kind?: string; message?: string } = {}
	await db.execStream('SELECT 1', [], (ev) => {
		if (ev.kind === 'finish') { events.push('finish'); error = ev.error ?? {} }
		else events.push(ev.kind)
	}, { signal: controller.signal })
	assert.deepEqual(events, ['finish'])
	assert.equal(error.kind, 'aborted')
	assert.equal(error.message, 'cancelled')
})
