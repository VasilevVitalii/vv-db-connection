import { test } from 'node:test'
import assert from 'node:assert/strict'
import { DbConnection } from '../src/index'
import { loadConnections, reachable, connHost, connPort } from './helpers'

const conns = await loadConnections()
const mssql = conns?.mssql ?? null
const pg = conns?.pg ?? null

let skip: string | false = 'multi: .connect.json missing'
if (mssql && pg) {
	const okMssql = await reachable(connHost(mssql), connPort(mssql))
	const okPg = await reachable(connHost(pg), connPort(pg))
	skip = okMssql && okPg ? false : 'multi: host unreachable'
}

test('two connections in parallel (mssql + pg)', { skip }, async (t) => {
	const mssqlDb = new DbConnection(mssql!)
	const pgDb = new DbConnection(pg!)
	t.after(async () => { await mssqlDb.close(); await pgDb.close() })
	assert.equal(mssqlDb.kind(), 'mssql')
	assert.equal(pgDb.kind(), 'pg')
	const [r1, r2] = await Promise.all([mssqlDb.ping(), pgDb.ping()])
	assert.equal(r1.ok, true, r1.ok ? '' : r1.error.message)
	assert.equal(r2.ok, true, r2.ok ? '' : r2.error.message)
})
