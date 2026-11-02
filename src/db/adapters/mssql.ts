import sql from 'mssql'
import type { IDbAdapter, TDbCol, TDbError, TDbParam, TDbStreamEvent, TDbTxCb, TDbTxResult } from '../types'
import type { TConnMssql } from '../connection'
import type { TResult } from '../../tresult'
import { mapPlaceholders } from '../placeholders'
import { toDbError, withTxTimeout } from '../helpers'

// Converts :name placeholders to @name for mssql named parameters. A colon inside a
// string literal, a quoted identifier or a comment is left as it is.
function buildMssqlQuery(query: string): string {
	return mapPlaceholders(query, (name) => `@${name}`)
}

function toSchema(columns: Record<string, any>): TDbCol[] {
	return Object.values(columns).map((c: any) => {
		const type = typeof c.type === 'function' ? c.type() : c.type
		return {
			name:      c.name,
			kind:      type?.name ?? 'unknown',
			len:       c.length,
			scale:     c.scale,
			precision: c.precision,
		}
	})
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
				request.input(p.name, p.value)
			}

			let error: TDbError | undefined
			let done = false

			const onAbort = (): void => {
				if (done) return
				done = true
				// Cancel the statement, not the pool: the connection stays usable for the
				// next call, unlike the pre-pool teardown that closed the whole pool.
				try { request.cancel() } catch {}
				resolve()
			}
			signal?.addEventListener('abort', onAbort)

			request.on('recordset', (columns: Record<string, unknown>) => {
				if (!done) callback({ kind: 'schema', schema: toSchema(columns) })
			})
			request.on('row', (row: object) => {
				if (!done) callback({ kind: 'row', row: row as Record<string, unknown> })
			})
			request.on('error', (err: Error) => {
				error = toDbError(err, 'query')
			})
			request.on('done', () => {
				if (done) return
				done = true
				signal?.removeEventListener('abort', onAbort)
				callback({ kind: 'finish', error })
				resolve()
			})

			request.query(buildMssqlQuery(query))
		})
	}

	async execTxDynamic(cb: TDbTxCb, timeoutMs?: number): Promise<TResult<void, TDbError>> {
		return withTxTimeout(timeoutMs, async (signal) => {
			// A transaction owns its connection for its whole lifetime and must be torn
			// down hard on timeout (a stuck statement never answers rollback), so it gets
			// its own pool here rather than the shared one used by execStream.
			let pool: sql.ConnectionPool
			try {
				pool = await new sql.ConnectionPool(buildPoolConfig(this.p)).connect()
			} catch (err) {
				return { ok: false, error: toDbError(err, 'connect') }
			}
			pool.on('error', () => {})
			const teardown = (): void => { void pool.close().catch(() => {}) }
			signal.addEventListener('abort', teardown, { once: true })
			let tx: sql.Transaction | undefined
			try {
				tx = new sql.Transaction(pool)
				await tx.begin()
				const transaction = tx
				const exec = (query: string, params: TDbParam[]): Promise<TDbTxResult> => {
					const req = new sql.Request(transaction)
					const q = buildMssqlQuery(query)
					for (const p of params) req.input(p.name, p.value)
					return req.query(q).then((res) => ({ rows: (res.recordset ?? []) as Record<string, unknown>[] }))
				}
				await cb(exec, signal)
				await transaction.commit()
				return { ok: true, result: undefined }
			} catch (err) {
				if (tx) { try { await tx.rollback() } catch {} }
				return { ok: false, error: toDbError(err, 'query') }
			} finally {
				signal.removeEventListener('abort', teardown)
				if (!signal.aborted) {
					try { await pool.close() } catch {}
				}
			}
		})
	}
}
