import { test } from 'node:test'
import assert from 'node:assert/strict'
import { dbTitle, dbUser } from '../src/db/connection'

// review2 4.23: title() used to show 'DEFAULT' when no port was given, which told the
// reader nothing. It now shows the database's actual default port, so log output is
// unambiguous about what will be dialled.
test('dbTitle shows the actual default port when none is given', () => {
	assert.equal(dbTitle({ kind: 'mssql', server: 'srv', auth: { kind: 'mssql', user: 'u', password: 'p' } }), 'srv:1433 [default]')
	assert.equal(dbTitle({ kind: 'pg', host: 'h', database: 'd', user: 'u', password: 'p' }), 'h:5432 [d]')
	assert.equal(dbTitle({ kind: 'ora', host: 'h', service: 's', user: 'u', password: 'p' }), 'h:1521 [s]')
	assert.equal(dbTitle({ kind: 'fb', host: 'h', database: 'd', user: 'u', password: 'p' }), 'h:3050 [d]')
})

test('dbTitle keeps an explicit port', () => {
	assert.equal(dbTitle({ kind: 'pg', host: 'h', port: 5433, database: 'd', user: 'u', password: 'p' }), 'h:5433 [d]')
})

test('dbUser resolves ntlm to domain\\user', () => {
	assert.equal(dbUser({ kind: 'mssql', server: 'srv', auth: { kind: 'ntlm', domain: 'DOM', user: 'u', password: 'p' } }), 'DOM\\u')
	assert.equal(dbUser({ kind: 'pg', host: 'h', database: 'd', user: 'u', password: 'p' }), 'u')
})
