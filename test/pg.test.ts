import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createDbConnection } from '../src/index'
import { loadConnections, reachable, runDbSuite, connHost, connPort } from './helpers'

const conns = await loadConnections()
const conn = conns?.pg ?? null
const skip = conn
	? ((await reachable(connHost(conn), connPort(conn))) ? false : 'pg: host unreachable')
	: 'pg: .connect.json missing'

runDbSuite('pg', conn, 'SELECT :v AS v', skip)

// A colon inside a string literal is text, not a parameter: the query must come back
// verbatim, not with the literal rewritten to '12@30'.
test('pg: colon inside a string literal stays literal', { skip }, async (t) => {
	const db = createDbConnection(conn!)
	t.after(async () => { await db.close() })
	const r = await db.exec("SELECT '12:30' AS t, :v AS v", [{ name: 'v', value: 'hello' }])
	assert.equal(r.ok, true, r.ok ? '' : r.error.message)
	if (!r.ok) return
	const row = r.result[0]!.rows[0] as Record<string, unknown>
	assert.equal(row.t, '12:30')
	assert.equal(row.v, 'hello')
})
