import { test } from 'node:test'
import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import { createRequire } from 'node:module'
import { loadConnections, reachable, runDbSuite, connHost, connPort } from './helpers'
import { DbConnection } from '../src/index'
import { FbAdapter } from '../src/db/adapters/fb'
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
test('fb: an out param is rejected', async (t) => {
	const db = new DbConnection({ kind: 'fb', host: 'localhost', database: '/tmp/x.fdb', user: 'u', password: 'p' } as TConnFb)
	const r = await db.exec('SELECT 1 FROM rdb$database WHERE 1 = :v', [{ name: 'v', value: 1, direction: 'out' }])
	assert.equal(r.ok, false)
	if (!r.ok) assert.match(r.error.message, /fb: out parameters are not supported/)
})

// review2 §3 (group A): LOB and BigInt for Firebird. node-firebird uploads a Buffer/string
// value to a BLOB column automatically, and sends a JS bigint as INT128. Reading a BIGINT
// beyond 2^53 exactly needs numericMode 'string'/'safe' - 'lossy' (default) decodes via
// JS Number and silently rounds.
test('fb: Buffer round-trips through a BLOB column', { skip }, async (t) => {
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

test('fb: bigint round-trips exactly with numericMode string', { skip }, async (t) => {
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

// review2 §4 (group C): node-firebird has no attach timeout, so a firewalled host (SYN
// dropped rather than refused) used to hang the attach for the OS's TCP timeout. With
// connectTimeout the socket is ripped and the call fails as kind 'connect' promptly.
// 192.0.2.1 (TEST-NET-1) never answers: either the SYN is dropped and the timeout fires,
// or the route is unreachable and the connect fails fast - both must end quickly.
test('fb: a stuck attach to an unreachable host is cut off by connectTimeout', async (t) => {
	const t0 = Date.now()
	const db = new DbConnection({ kind: 'fb', host: '192.0.2.1', port: 3050, database: '/tmp/x.fdb', user: 'u', password: 'p', connectTimeout: 500 } as TConnFb)
	const r = await db.exec('SELECT 1 FROM rdb$database', [])
	const elapsed = Date.now() - t0
	await db.close()
	assert.equal(r.ok, false)
	if (!r.ok) assert.equal(r.error.kind, 'connect')
	assert.ok(elapsed < 10000, `attach should be cut off, took ${elapsed}ms`)
})

// node-firebird stores the in-flight Connection on one global (exports.connection); the
// adapter reaches it through the same live require used by the code under test, so the
// attach function can be substituted and the connection handle observed.
const fbLive = createRequire(import.meta.url)('node-firebird') as {
	connection?: { _isDetach?: boolean; _retry_connection_id?: unknown; _socket?: { destroy(): void } | null }
	attach: (options: unknown, callback: (err: unknown, db: unknown) => void) => void
}

// review3 1.3: two concurrent attaches overwrite the global handle, and the old timeout
// handler read that global at fire time - so the first timeout ripped the *second* attach's
// socket and left the first one hanging. The fix captures each attach's own connection
// synchronously right after calling attach().
test('fb: a connect timeout tears down its own socket, not a concurrent attach', async (t) => {
	let destroyedA = 0
	let destroyedB = 0
	const connA = { _isDetach: false, _retry_connection_id: undefined, _socket: { destroy: () => { destroyedA += 1 } } }
	const connB = { _isDetach: false, _retry_connection_id: undefined, _socket: { destroy: () => { destroyedB += 1 } } }

	let attachCall = 0
	t.mock.method(fbLive, 'attach', () => {
		attachCall += 1
		// node-firebird assigns the in-flight Connection to exports.connection
		// synchronously inside attach() - the second attach overwrites the first.
		fbLive.connection = attachCall === 1 ? connA : connB
		// never call cb: both attaches stay stuck so the timeout tears the socket.
	})

	const adapter = new FbAdapter({ kind: 'fb', host: '127.0.0.1', database: '/tmp/x.fdb', user: 'u', password: 'p', connectTimeout: 50 } as TConnFb)
	const a = adapter.execTxDynamic(async () => { throw new Error('body must not run') })
	const b = adapter.execTxDynamic(async () => { throw new Error('body must not run') })

	const [ra, rb] = await Promise.all([a, b])
	assert.equal(ra.ok, false)
	if (!ra.ok) assert.equal(ra.error.kind, 'connect')
	assert.equal(rb.ok, false)
	if (!rb.ok) assert.equal(rb.error.kind, 'connect')

	assert.equal(destroyedA, 1, 'the first attach must tear down its own socket')
	assert.equal(destroyedB, 1, 'the second attach must tear down its own socket')
})

// If the attach does complete after the deadline (the socket survived long enough), the
// connection that came back has no one waiting for it. The fix drops it so it does not
// leak a socket; the old code ignored it (done() is a no-op once the timeout settled).
test('fb: an attach that completes after the timeout drops the connection', async (t) => {
	let dropped = 0
	let savedCb!: (err: unknown, db: unknown) => void
	const fakeDbConn = { _isDetach: false, _retry_connection_id: undefined, _socket: null as { destroy(): void } | null }

	t.mock.method(fbLive, 'attach', (_options: unknown, cb: (err: unknown, db: unknown) => void) => {
		savedCb = cb
		fbLive.connection = fakeDbConn
	})

	const adapter = new FbAdapter({ kind: 'fb', host: '127.0.0.1', database: '/tmp/x.fdb', user: 'u', password: 'p', connectTimeout: 50 } as TConnFb)
	const p = adapter.execTxDynamic(async () => { throw new Error('body must not run') })
	await new Promise((resolve) => setTimeout(resolve, 80)) // the connect timeout fires
	// the attach now completes late, with an open socket
	fakeDbConn._socket = { destroy: () => { dropped += 1 } }
	savedCb(null, { connection: fakeDbConn })
	const r = await p

	assert.equal(r.ok, false)
	if (!r.ok) assert.equal(r.error.kind, 'connect')
	assert.equal(dropped, 1, 'the late connection must be dropped, not leaked')
})

// review3 4.20 (second half): aborting during an in-flight attach must rip the socket at
// once instead of leaving it until the OS TCP timeout. The attach timeout already tore it
// down; the same teardown is now shared with the external abort signal.
test('fb: aborting execStream during a stuck attach tears the socket down', async (t) => {
	let destroyed = 0
	const fakeConn = { _isDetach: false, _retry_connection_id: undefined, _socket: { destroy: () => { destroyed += 1 } } }

	t.mock.method(fbLive, 'attach', () => {
		// node-firebird assigns the in-flight Connection synchronously; never call cb so
		// the attach stays stuck until the abort signal fires.
		fbLive.connection = fakeConn
	})

	const ac = new AbortController()
	const adapter = new FbAdapter({ kind: 'fb', host: '127.0.0.1', database: '/tmp/x.fdb', user: 'u', password: 'p' } as TConnFb)
	const events: { kind: string }[] = []
	const p = adapter.execStream('SELECT 1 FROM rdb$database', [], (e) => events.push(e), ac.signal)
	await new Promise((resolve) => setImmediate(resolve)) // let attach run and register its abort listener
	ac.abort()
	await p

	assert.equal(destroyed, 1, 'the in-flight attach socket must be ripped on abort')
	assert.equal(events[events.length - 1]?.kind, 'finish')
})

// review9 §1.2: a lost connection inside a transaction used to be re-labelled 'user'
// (the {message, kind} literal carried no code/stack, so isDbError() said false). The
// loss now carries code 'FB_CONN_LOST' and stays kind 'query', so retry logic sees it.
test('fb: a connection lost in a transaction is classified as query, not user', async (t) => {
	const fakeDb = new EventEmitter() as EventEmitter & Record<string, unknown>
	const fakeSocket = Object.assign(new EventEmitter(), { destroy: (): void => {} })
	const fakeDbConn = { _isDetach: false, _retry_connection_id: undefined, _socket: fakeSocket }
	fakeDb.connection = fakeDbConn
	const fakeTx = {
		queryAsync: async (): Promise<unknown> => ({ rows: [], recordCounts: undefined }),
		commit: (cb: (e: unknown) => void): void => cb(null),
		rollback: (cb: () => void): void => cb(),
	}
	fakeDb.transaction = (_iso: unknown, cb: (e: unknown, t: unknown) => void): void => cb(null, fakeTx)

	t.mock.method(fbLive, 'attach', (_options: unknown, cb: (err: unknown, db: unknown) => void) => {
		fbLive.connection = fakeDbConn
		cb(null, fakeDb)
	})

	const adapter = new FbAdapter({ kind: 'fb', host: '127.0.0.1', database: '/tmp/x.fdb', user: 'u', password: 'p' } as TConnFb)
	const r = await adapter.execTxDynamic(async (exec) => {
		// Lose the connection mid-transaction, then run a statement on it.
		fakeDb.emit('error', new Error('socket hang up'))
		await exec('SELECT 1 FROM rdb$database', [])
		return undefined
	})

	assert.equal(r.ok, false)
	if (!r.ok) {
		assert.equal(r.error.kind, 'query')
		assert.equal(r.error.code, 'FB_CONN_LOST')
	}
})

// review11 §4: the reconnect branch of watchConnection was untested. A driver that
// reattaches on its own mid-transaction must read as a connection loss (kind 'query',
// code 'FB_CONN_LOST'), so a subsequent exec rejects instead of running on the new socket.
test('fb: a driver reattach in a transaction is classified as query', async (t) => {
	const fakeDb = new EventEmitter() as EventEmitter & Record<string, unknown>
	const fakeSocket = Object.assign(new EventEmitter(), { destroy: (): void => {} })
	const fakeDbConn = { _isDetach: false, _retry_connection_id: undefined, _socket: fakeSocket }
	fakeDb.connection = fakeDbConn
	const fakeTx = {
		queryAsync: async (): Promise<unknown> => ({ rows: [], recordCounts: undefined }),
		commit: (cb: (e: unknown) => void): void => cb(null),
		rollback: (cb: () => void): void => cb(),
	}
	fakeDb.transaction = (_iso: unknown, cb: (e: unknown, t: unknown) => void): void => cb(null, fakeTx)

	t.mock.method(fbLive, 'attach', (_options: unknown, cb: (err: unknown, db: unknown) => void) => {
		fbLive.connection = fakeDbConn
		cb(null, fakeDb)
	})

	const adapter = new FbAdapter({ kind: 'fb', host: '127.0.0.1', database: '/tmp/x.fdb', user: 'u', password: 'p' } as TConnFb)
	const r = await adapter.execTxDynamic(async (exec) => {
		fakeDb.emit('reconnect')
		await exec('SELECT 1 FROM rdb$database', [])
		return undefined
	})

	assert.equal(r.ok, false)
	if (!r.ok) {
		assert.equal(r.error.kind, 'query')
		assert.equal(r.error.code, 'FB_CONN_LOST')
	}
})

// review11 §4: the socket 'close' branch of watchConnection was untested. An unsolicited
// socket close (not our own detach) must read as a connection loss, not be swallowed.
test('fb: a socket close in a transaction is classified as query', async (t) => {
	const fakeDb = new EventEmitter() as EventEmitter & Record<string, unknown>
	const fakeSocket = Object.assign(new EventEmitter(), { destroy: (): void => {} })
	const fakeDbConn = { _isDetach: false, _retry_connection_id: undefined, _socket: fakeSocket }
	fakeDb.connection = fakeDbConn
	const fakeTx = {
		queryAsync: async (): Promise<unknown> => ({ rows: [], recordCounts: undefined }),
		commit: (cb: (e: unknown) => void): void => cb(null),
		rollback: (cb: () => void): void => cb(),
	}
	fakeDb.transaction = (_iso: unknown, cb: (e: unknown, t: unknown) => void): void => cb(null, fakeTx)

	t.mock.method(fbLive, 'attach', (_options: unknown, cb: (err: unknown, db: unknown) => void) => {
		fbLive.connection = fakeDbConn
		cb(null, fakeDb)
	})

	const adapter = new FbAdapter({ kind: 'fb', host: '127.0.0.1', database: '/tmp/x.fdb', user: 'u', password: 'p' } as TConnFb)
	const r = await adapter.execTxDynamic(async (exec) => {
		fakeSocket.emit('close')
		await exec('SELECT 1 FROM rdb$database', [])
		return undefined
	})

	assert.equal(r.ok, false)
	if (!r.ok) {
		assert.equal(r.error.kind, 'query')
		assert.equal(r.error.code, 'FB_CONN_LOST')
	}
})

// review12 §4: the loss listeners must not outlive the query. After a successful
// transaction the error/reconnect listeners on the db and the close listener on the socket
// are removed (a future pooled Firebird would otherwise accumulate one set per query).
test('fb: the connection-loss listeners are removed when the transaction ends', async (t) => {
	const fakeDb = new EventEmitter() as EventEmitter & Record<string, unknown>
	const fakeSocket = Object.assign(new EventEmitter(), { destroy: (): void => {} })
	const fakeDbConn = { _isDetach: false, _retry_connection_id: undefined, _socket: fakeSocket }
	fakeDb.connection = fakeDbConn
	const fakeTx = {
		queryAsync: async (): Promise<unknown> => ({ rows: [], recordCounts: undefined }),
		commit: (cb: (e: unknown) => void): void => cb(null),
		rollback: (cb: () => void): void => cb(),
	}
	fakeDb.transaction = (_iso: unknown, cb: (e: unknown, t: unknown) => void): void => cb(null, fakeTx)

	t.mock.method(fbLive, 'attach', (_options: unknown, cb: (err: unknown, db: unknown) => void) => {
		fbLive.connection = fakeDbConn
		cb(null, fakeDb)
	})

	const adapter = new FbAdapter({ kind: 'fb', host: '127.0.0.1', database: '/tmp/x.fdb', user: 'u', password: 'p' } as TConnFb)
	const r = await adapter.execTxDynamic(async (exec) => {
		await exec('SELECT 1 FROM rdb$database', [])
		return undefined
	})

	assert.equal(r.ok, true)
	assert.equal(fakeDb.listenerCount('error'), 0, 'error listener must be removed')
	assert.equal(fakeDb.listenerCount('reconnect'), 0, 'reconnect listener must be removed')
	assert.equal(fakeSocket.listenerCount('close'), 0, 'socket close listener must be removed')
})

// review19 2.1: a connection lost inside a transaction dropped the socket (connLost →
// dropConnection) but the cb-throw catch still ran rollback on that dead socket — the
// driver never answers rollback on a destroyed connection, so the call hung without a
// deadline (the same class as mssql's tornDown watchdog). The rollback must be skipped
// once the connection is lost; nothing is left to roll back.
test('fb: a lost connection before a throwing callback skips rollback on the dead socket', async (t) => {
	let rollbacks = 0
	const fakeDb = new EventEmitter() as EventEmitter & Record<string, unknown>
	const fakeSocket = Object.assign(new EventEmitter(), { destroy: (): void => {} })
	const fakeDbConn = { _isDetach: false, _retry_connection_id: undefined, _socket: fakeSocket }
	fakeDb.connection = fakeDbConn
	const fakeTx = {
		queryAsync: async (): Promise<unknown> => ({ rows: [], recordCounts: undefined }),
		commit: (cb: (e: unknown) => void): void => cb(null),
		rollback: (cb: () => void): void => { rollbacks += 1; cb() },
	}
	fakeDb.transaction = (_iso: unknown, cb: (e: unknown, t: unknown) => void): void => cb(null, fakeTx)

	t.mock.method(fbLive, 'attach', (_options: unknown, cb: (err: unknown, db: unknown) => void) => {
		fbLive.connection = fakeDbConn
		cb(null, fakeDb)
	})

	const adapter = new FbAdapter({ kind: 'fb', host: '127.0.0.1', database: '/tmp/x.fdb', user: 'u', password: 'p' } as TConnFb)
	const r = await adapter.execTxDynamic(async (exec) => {
		// Lose the connection mid-transaction, then fail: the connection is already
		// dropped, so rollback must be skipped rather than sent over a dead socket.
		fakeDb.emit('error', new Error('socket hang up'))
		await exec('SELECT 1 FROM rdb$database', [])
		return undefined
	})

	assert.equal(r.ok, false)
	if (!r.ok) {
		assert.equal(r.error.kind, 'query')
		assert.equal(r.error.code, 'FB_CONN_LOST')
	}
	assert.equal(rollbacks, 0, 'rollback must not run on a connection already dropped by connLost')
})
