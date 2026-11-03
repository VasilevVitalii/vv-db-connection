import { test } from 'node:test'
import assert from 'node:assert/strict'
import { loadConnections, reachable, runDbSuite, connHost, connPort } from './helpers'
import { DbConnection } from '../src/index'
import type { TConnFb } from '../src/db/connection'

const conns = await loadConnections()
const conn = conns?.fb ?? null
const skip = conn
	? ((await reachable(connHost(conn), connPort(conn))) ? false : 'fb: host unreachable')
	: 'fb: .connect.json missing'

runDbSuite('fb', conn, 'SELECT CAST(:v AS VARCHAR(20)) AS v FROM rdb$database', {
	create: 'CREATE TABLE vv_ra_test (id INTEGER, name VARCHAR(20))',
	insert: "INSERT INTO vv_ra_test (id, name) VALUES (1, 'a')",
	update: "UPDATE vv_ra_test SET name = 'x' WHERE id = 1",
	del: 'DELETE FROM vv_ra_test WHERE id = 1',
	drop: 'DROP TABLE vv_ra_test',
}, skip)

// review2 §3 (group A): out/inout binds are Oracle-only. fb rejects the direction in
// buildQuery before any attach, so this runs without a live database.
test('fb: an out param is rejected', async () => {
	const db = new DbConnection({ kind: 'fb', host: 'localhost', database: '/tmp/x.fdb', user: 'u', password: 'p' } as TConnFb)
	const r = await db.exec('SELECT 1 FROM rdb$database WHERE 1 = :v', [{ name: 'v', value: 1, direction: 'out' }])
	assert.equal(r.ok, false)
	if (!r.ok) assert.match(r.error.message, /fb: out parameters are not supported/)
})

// review2 §3 (group A): LOB and BigInt for Firebird. node-firebird uploads a Buffer/string
// value to a BLOB column automatically, and sends a JS bigint as INT128. Reading a BIGINT
// beyond 2^53 exactly needs numericMode 'string'/'safe' - 'lossy' (default) decodes via
// JS Number and silently rounds.
test('fb: Buffer round-trips through a BLOB column', { skip }, async () => {
	const db = new DbConnection(conn as TConnFb)
	try {
		const created = await db.exec('CREATE TABLE vv_ra_blob (id INTEGER, data BLOB SUB_TYPE TEXT)', [])
		assert.equal(created.ok, true, created.ok ? '' : created.error.message)
		const ins = await db.exec('INSERT INTO vv_ra_blob (id, data) VALUES (1, :d)', [{ name: 'd', value: Buffer.from('hello blob') }])
		assert.equal(ins.ok, true, ins.ok ? '' : ins.error.message)
		const sel = await db.exec('SELECT data FROM vv_ra_blob WHERE id = 1', [])
		assert.equal(sel.ok, true, sel.ok ? '' : sel.error.message)
		if (!sel.ok) return
		const row = sel.result[0]!.rows[0] as Record<string, unknown>
		assert.equal(Object.values(row)[0], 'hello blob')
	} finally {
		await db.exec('DROP TABLE vv_ra_blob', [])
		await db.close()
	}
})

test('fb: bigint round-trips exactly with numericMode string', { skip }, async () => {
	const db = new DbConnection({ ...(conn as TConnFb), numericMode: 'string' } as TConnFb)
	try {
		const created = await db.exec('CREATE TABLE vv_ra_big (id INTEGER, n BIGINT)', [])
		assert.equal(created.ok, true, created.ok ? '' : created.error.message)
		const big = 9007199254740993n // 2^53 + 1, not exactly representable as a Number
		const ins = await db.exec('INSERT INTO vv_ra_big (id, n) VALUES (1, :n)', [{ name: 'n', value: big }])
		assert.equal(ins.ok, true, ins.ok ? '' : ins.error.message)
		const sel = await db.exec('SELECT n FROM vv_ra_big WHERE id = 1', [])
		assert.equal(sel.ok, true, sel.ok ? '' : sel.error.message)
		if (!sel.ok) return
		const row = sel.result[0]!.rows[0] as Record<string, unknown>
		assert.equal(Object.values(row)[0], '9007199254740993')
	} finally {
		await db.exec('DROP TABLE vv_ra_big', [])
		await db.close()
	}
})
