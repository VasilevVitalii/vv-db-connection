import { test } from 'node:test'
import assert from 'node:assert/strict'
import { DbConnection } from '../src/index'
import { toDbError, abortError } from '../src/db/helpers'
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

test('toDbError prefers the SQL Server number over the mssql driver class code', () => {
	assert.deepEqual(
		toDbError({ message: 'deadlock', code: 'EREQUEST', number: 1205 }, 'query'),
		{ message: 'deadlock', code: '1205', kind: 'query' },
	)
	assert.deepEqual(
		toDbError({ message: 'unique violation', code: 'EREQUEST', number: 2627 }, 'query'),
		{ message: 'unique violation', code: '2627', kind: 'query' },
	)
})

test('toDbError keeps the mssql driver code when there is no number (connection error)', () => {
	assert.deepEqual(
		toDbError({ message: 'timeout', code: 'ETIMEOUT' }, 'connect'),
		{ message: 'timeout', code: 'ETIMEOUT', kind: 'connect' },
	)
})

test('toDbError keeps the Firebird gdscode', () => {
	assert.deepEqual(
		toDbError({ message: 'Dynamic SQL Error', gdscode: 335544569 }, 'query'),
		{ message: 'Dynamic SQL Error', code: '335544569', kind: 'query' },
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
	const db = new DbConnection(mssql)
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
	const db = new DbConnection(mssql)
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

// review4 §3.1: TDbError is a plain object, not an Error, so it carries no stack by
// default. A driver Error is the one input that has one - forward it so a caller can
// see where the failure came from, without widening TDbError into an Error subclass.
test('toDbError forwards the stack of a real Error', () => {
	const err = new Error('boom')
	assert.equal(toDbError(err, 'query').stack, err.stack)
})

test('toDbError leaves stack unset for a non-Error input', () => {
	assert.equal('stack' in toDbError({ message: 'boom', code: '23505' }, 'query'), false)
})

test('abortError forwards the stack of a real Error reason', () => {
	const reason = new Error('cancelled')
	const mapped = abortError(reason)
	assert.equal(mapped.kind, 'aborted')
	assert.equal(mapped.message, 'cancelled')
	assert.equal(mapped.stack, reason.stack)
})

test('abortError leaves stack unset for a non-Error reason', () => {
	assert.equal('stack' in abortError('cancelled'), false)
})
