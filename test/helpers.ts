import { readFile } from 'node:fs/promises'
import { createConnection } from 'node:net'
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createDbConnection } from '../src/index'
import type { TConnectionParam } from '../src/db/connection'

export type TProvider = 'mssql' | 'pg' | 'ora' | 'fb'

type TConnectRaw = {
	mssql: { host: string; port: number; login: string; password: string; base: string }
	pg:    { host: string; port: number; login: string; password: string; base: string }
	ora:   { host: string; port: number; login: string; password: string; service: string }
	fb:    { host: string; port: number; login: string; password: string; base: string }
}

// Reads the local .connect.json (not committed, not published). Returns null when the
// file is missing so the suite skips instead of failing on machines without a database.
export async function loadConnections(): Promise<Record<TProvider, TConnectionParam> | null> {
	let raw: string
	try {
		raw = await readFile(new URL('../.connect.json', import.meta.url), 'utf8')
	} catch {
		return null
	}
	const c = JSON.parse(raw) as TConnectRaw
	return {
		mssql: { kind: 'mssql', server: c.mssql.host, port: c.mssql.port, database: c.mssql.base, auth: { kind: 'mssql', user: c.mssql.login, password: c.mssql.password } },
		pg:    { kind: 'pg', host: c.pg.host, port: c.pg.port, database: c.pg.base, user: c.pg.login, password: c.pg.password },
		ora:   { kind: 'ora', host: c.ora.host, port: c.ora.port, service: c.ora.service, user: c.ora.login, password: c.ora.password },
		fb:    { kind: 'fb', host: c.fb.host, port: c.fb.port, database: c.fb.base, user: c.fb.login, password: c.fb.password },
	}
}

export function reachable(host: string, port: number): Promise<boolean> {
	return new Promise((resolve) => {
		const sock = createConnection({ host, port })
		sock.setTimeout(3000)
		sock.once('connect', () => { sock.destroy(); resolve(true) })
		sock.once('error', () => { sock.destroy(); resolve(false) })
		sock.once('timeout', () => { sock.destroy(); resolve(false) })
	})
}

export function connHost(conn: TConnectionParam): string {
	return conn.kind === 'mssql' ? conn.server : conn.host
}

export function connPort(conn: TConnectionParam): number {
	switch (conn.kind) {
		case 'mssql': return conn.port ?? 1433
		case 'pg':    return conn.port ?? 5432
		case 'ora':   return conn.port ?? 1521
		case 'fb':    return conn.port ?? 3050
	}
}

// execParamQuery and txParamQuery each return exactly one row with one column.
//
// They differ because the adapters translate `:name` parameters in different places:
//   - pg / ora / fb convert `:name` in BOTH execStream and execTxDynamic;
//   - mssql converts `:name` -> `@name` ONLY inside execTxDynamic (buildMssqlQuery),
//     so its execStream/exec path expects native `@name` and rejects `:name`
//     ("Incorrect syntax near ':'"). The test mirrors that real behaviour.
//
// The returned column name also differs per database (v vs V), so the value is read
// through Object.values rather than by name.
export function runDbSuite(name: TProvider, conn: TConnectionParam | null, execParamQuery: string, txParamQuery: string, skip: string | false): void {
	if (!conn) {
		test(`${name} (no connection)`, { skip: skip || 'no connection' }, () => {})
		return
	}

	const db = createDbConnection(conn)

	test(`${name} ping`, { skip }, async () => {
		const r = await db.ping()
		assert.equal(r.ok, true, r.ok ? '' : r.error)
	})

	test(`${name} version`, { skip }, async () => {
		const r = await db.version()
		assert.equal(r.ok, true, r.ok ? '' : r.error)
		if (r.ok) assert.ok(r.result.length > 0, 'empty version string')
	})

	test(`${name} kind/title/user`, { skip }, () => {
		assert.equal(db.kind(), name)
		assert.ok(db.title().length > 0, 'empty title')
		assert.ok(db.user().length > 0, 'empty user')
	})

	test(`${name} exec with param`, { skip }, async () => {
		const r = await db.exec(execParamQuery, [{ name: 'v', value: 'hello' }])
		assert.equal(r.ok, true, r.ok ? '' : r.error)
		if (!r.ok) return
		assert.equal(r.result.length, 1)
		const row = r.result[0]!.rows[0] as Record<string, unknown>
		assert.equal(Object.values(row)[0], 'hello')
	})

	test(`${name} execStream events`, { skip }, async () => {
		const kinds: string[] = []
		let schemaLen = -1
		let rows = 0
		await db.execStream(execParamQuery, [{ name: 'v', value: 'hello' }], (ev) => {
			if (ev.kind === 'schema') { kinds.push('schema'); schemaLen = ev.schema.length }
			else if (ev.kind === 'row') { kinds.push('row'); rows++ }
			else { kinds.push('finish'); if (ev.error) kinds.push(`finish(${ev.error})`) }
		})
		assert.deepEqual(kinds, ['schema', 'row', 'finish'])
		assert.equal(schemaLen, 1)
		assert.equal(rows, 1)
	})

	test(`${name} execTxDynamic commit`, { skip }, async () => {
		const r = await db.execTxDynamic(async (exec) => {
			const res = await exec(txParamQuery, [{ name: 'v', value: 'world' }])
			assert.equal(res.rows.length, 1)
			assert.equal(Object.values(res.rows[0]!)[0], 'world')
		})
		assert.equal(r.ok, true, r.ok ? '' : r.error)
	})

	test(`${name} execTxDynamic rollback on throw`, { skip }, async () => {
		const r = await db.execTxDynamic(async () => { throw new Error('boom') })
		assert.equal(r.ok, false)
		if (!r.ok) assert.match(r.error, /boom/)
	})
}
