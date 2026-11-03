import sql from 'mssql'
import type { IDbAdapter, TDbCol, TDbError, TDbParam, TDbStreamEvent, TDbTxCb, TDbTxResult } from '../types'
import type { TConnMssql } from '../connection'
import type { TResult } from '../../tresult'
import { mapPlaceholders } from '../placeholders'
import { toDbError, isDbError, withTxTimeout } from '../helpers'

// Converts :name placeholders to @name for mssql named parameters. A colon inside a
// string literal, a quoted identifier or a comment is left as it is. A placeholder that
// has no matching param is rejected here - otherwise mssql would send an undeclared
// @name and the error would surface only as a server-side "must declare the variable".
function buildMssqlQuery(query: string, params: TDbParam[]): string {
	return mapPlaceholders(query, (name) => {
		if (!params.some((p) => p.name === name)) throw new Error(`mssql: param not found: ${name}`)
		return `@${name}`
	})
}

// A forced parameter type (TDbParam.type) maps a driver-native type name ('DateTime2',
// 'NVarChar', ...) to the mssql type factory. Without it the driver infers the type from
// the JS value - a Date becomes DateTime (losing sub-second precision) and a long string
// becomes NVarChar - so a caller can pin the exact type here. Unknown names throw before
// the query is sent rather than failing server-side.
export function mssqlType(name: string): any {
	const t = (sql.TYPES as Record<string, any>)[name]
	if (t === undefined) throw new Error(`mssql: unknown param type: ${name}`)
	return t
}

export function mssqlInput(request: sql.Request, p: TDbParam): void {
	// Only Oracle supports out/inout binds; reject them here rather than silently
	// binding an undefined value and inserting NULL.
	if (p.direction !== undefined && p.direction !== 'in') throw new Error(`mssql: ${p.direction} parameters are not supported`)
	if (p.type === undefined) request.input(p.name, p.value)
	else request.input(p.name, mssqlType(p.type), p.value)
}

export function toSchema(columns: Record<string, any>): TDbCol[] {
	return Object.values(columns).map((c: any) => ({
		name:      c.name,
		// c.type is the type constructor (sql.Int, sql.VarChar, ...): its .name is the
		// type name. Calling it, c.type(), builds a declaration object ({ type, length })
		// that has no .name - the old code did that and reported 'unknown' for every column.
		kind:      (typeof c.type === 'function' ? c.type.name : c.type?.name) ?? 'unknown',
		len:       c.length,
		scale:     c.scale,
		precision: c.precision,
	}))
}

function buildPoolConfig(p: TConnMssql): sql.config {
	const base: sql.config = {
		server:   p.server,
		port:     p.port ?? 1433,
		...(p.database ? { database: p.database } : {}),
		...(p.connectTimeout ? { connectionTimeout: p.connectTimeout } : {}),
		...(p.connectTimeout ? { pool: { acquireTimeoutMillis: p.connectTimeout, createTimeoutMillis: p.connectTimeout } } : {}),
		options: {
			trustServerCertificate: p.trustServerCertificate ?? false,
			encrypt:                p.encrypt ?? false,
			...(p.ca ? { cryptoCredentialsDetails: { ca: p.ca } } : {}),
		},
	}
	if (p.auth.kind === 'ntlm') {
		return { ...base, authentication: { type: 'ntlm', options: { domain: p.auth.domain, userName: p.auth.user, password: p.auth.password } } }
	}
	return { ...base, user: p.auth.user, password: p.auth.password }
}

export class MssqlAdapter implements IDbAdapter {
	constructor(private readonly p: TConnMssql) {}

	// One pool per adapter, created lazily and reused across execStream calls. Building
	// a fresh ConnectionPool per query renegotiated TCP/TLS + login every time; connect()
	// is idempotent on an already-connected pool. The 'error' listener swallows the
	// pool-level re-emit of a lost connection (the request ends on its own).
	private _pool?: sql.ConnectionPool
	private pool(): Promise<sql.ConnectionPool> {
		if (!this._pool) {
			this._pool = new sql.ConnectionPool(buildPoolConfig(this.p))
			this._pool.on('error', () => {})
		}
		return this._pool.connect()
	}

	async close(): Promise<void> {
		if (this._pool) {
			try { await this._pool.close() } catch {}
			this._pool = undefined
		}
	}

	kind(): 'mssql' { return 'mssql' }
	title(): string { return `${this.p.server}:${this.p.port || 'DEFAULT'} [${this.p.database ?? 'default'}]` }
	user(): string {
		if (this.p.auth.kind === 'ntlm') return `${this.p.auth.domain}\\${this.p.auth.user}`
		return this.p.auth.user
	}
	versionQuery(): string { return 'SELECT @@VERSION AS ver' }
	pingQuery(): string { return 'SELECT 1 AS p' }

	async execStream(query: string, params: TDbParam[], callback: (event: TDbStreamEvent) => void, signal?: AbortSignal): Promise<void> {
		let pool: sql.ConnectionPool
		try {
			pool = await this.pool()
		} catch (err) {
			callback({ kind: 'finish', error: toDbError(err, 'connect') })
			return
		}
		if (signal?.aborted) return

		return new Promise<void>((resolve) => {
			const request = pool.request()
			request.stream = true
			for (const p of params) {
				mssqlInput(request, p)
			}

			let error: TDbError | undefined
			let done = false
			// Whether the current statement produced a recordset. DML has none, and
			// then rowsAffected from `done` is the only thing worth reporting.
			let sawRecordset = false

			const onAbort = (): void => {
				if (done) return
				done = true
				// Tear the borrowed connection down hard. cancel() only sends an ATTN and
				// waits for a server response that may never come, which strands the tarn
				// slot forever (contract: "tear down without waiting"). Destroying the socket
				// completes the request locally and releases the connection at once.
				try {
					const conn = (request as unknown as { _currentRequest?: { connection?: { close(): void } } })._currentRequest?.connection
					if (conn) conn.close()
					else request.cancel()
				} catch {}
				resolve()
			}
			signal?.addEventListener('abort', onAbort)

			request.on('recordset', (columns: Record<string, unknown>) => {
				sawRecordset = true
				if (!done) callback({ kind: 'schema', schema: toSchema(columns) })
			})
			request.on('row', (row: object) => {
				if (!done) callback({ kind: 'row', row: row as Record<string, unknown> })
			})
			request.on('error', (err: Error) => {
				error = toDbError(err, 'query')
			})
			request.on('done', (result: { rowsAffected?: number[] }) => {
				if (done) return
				done = true
				signal?.removeEventListener('abort', onAbort)
				// A statement with no recordset is DML: give it an empty schema and the
				// number of rows it changed, so `exec` returns one result per statement
				// instead of dropping DML statements silently. The streaming driver
				// emits 'done' with an object { output, rowsAffected }, not a bare number
				// (rowsAffected is one entry per statement in the batch).
				if (!sawRecordset) {
					callback({ kind: 'schema', schema: [] })
					const affected = result?.rowsAffected?.[0]
					if (typeof affected === 'number') callback({ kind: 'rowsAffected', count: affected })
				}
				callback({ kind: 'finish', error })
				resolve()
			})

			request.query(buildMssqlQuery(query, params))
		})
	}

	async execTxDynamic(cb: TDbTxCb, timeoutMs?: number, external?: AbortSignal): Promise<TResult<void, TDbError>> {
		return withTxTimeout(timeoutMs, async (signal) => {
			// A transaction owns its connection for its whole lifetime and must be torn
			// down hard on timeout (a stuck statement never answers rollback), so it gets
			// its own pool here rather than the shared one used by execStream.
			let pool: sql.ConnectionPool
			let tx: sql.Transaction | undefined
			try {
				pool = await new sql.ConnectionPool(buildPoolConfig(this.p)).connect()
			} catch (err) {
				return { ok: false, error: toDbError(err, 'connect') }
			}
			pool.on('error', () => {})
			// The transaction owns a dedicated pool so a timeout can tear the whole thing
			// down. Hard-break first: rip the tedious socket out from under a stuck statement
			// (pool.close() alone is graceful - tarn waits for the busy connection, which a
			// stuck server never releases). _acquiredConnection and socket are not public API,
			// so this is best-effort: if mssql moves them the call is a no-op and pool.close()
			// still frees the slot once the statement ends.
			const teardown = (): void => {
				try {
					const conn = (tx as { _acquiredConnection?: { socket?: { destroy(): void } } } | undefined)?._acquiredConnection
					conn?.socket?.destroy()
				} catch {}
				void pool.close().catch(() => {})
			}
			signal.addEventListener('abort', teardown, { once: true })
			// connect() may have resolved after the deadline already fired: the listener
			// above was added too late to ever run, and the finally below skips close() on
			// an aborted signal. Tear the connection down here instead of leaking it.
			if (signal.aborted) {
				teardown()
				return { ok: false, error: { message: 'TIMEOUT', kind: 'timeout' } }
			}
			try {
				tx = new sql.Transaction(pool)
				await tx.begin()
				const transaction = tx
				const exec = (query: string, params: TDbParam[]): Promise<TDbTxResult> => {
					const req = new sql.Request(transaction)
					const q = buildMssqlQuery(query, params)
					for (const p of params) mssqlInput(req, p)
					return req.query(q).then(
						(res) => ({ rows: (res.recordset ?? []) as Record<string, unknown>[] }),
						// Tag the failure as a DB query error so it stays 'query' even when
						// it bubbles through the user's callback (which may re-throw it).
						(err) => { throw toDbError(err, 'query') },
					)
				}
				try {
					await cb(exec, signal)
				} catch (err) {
					try { await transaction.rollback() } catch {}
					return { ok: false, error: isDbError(err) ? err : toDbError(err, 'user') }
				}
				try {
					await transaction.commit()
				} catch (err) {
					try { await transaction.rollback() } catch {}
					return { ok: false, error: toDbError(err, 'tx') }
				}
				return { ok: true, result: undefined }
			} catch (err) {
				// begin() failing is the only thing that reaches here now.
				if (tx) { try { await tx.rollback() } catch {} }
				return { ok: false, error: toDbError(err, 'tx') }
			} finally {
				signal.removeEventListener('abort', teardown)
				if (!signal.aborted) {
					try { await pool.close() } catch {}
				}
			}
		}, external)
	}
}
