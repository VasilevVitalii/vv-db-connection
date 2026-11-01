import sql from 'mssql'
import type { IDbAdapter, TDbCol, TDbParam, TDbStreamEvent, TDbTxCb, TDbTxResult } from '../types'
import type { TConnMssql } from '../connection'
import type { TResult } from '../../tresult'
import { mapPlaceholders } from '../placeholders'

// Converts :name placeholders to @name for mssql named parameters. A colon inside a
// string literal, a quoted identifier or a comment is left as it is.
function buildMssqlQuery(query: string): string {
	return mapPlaceholders(query, (name) => `@${name}`)
}

function toSchema(columns: Record<string, any>): TDbCol[] {
	return Object.values(columns).map((col) => ({
		name:      col.name,
		kind:      'unknown',
		len:       col.length,
		scale:     col.scale,
		precision: col.precision,
	}))
}

function buildPoolConfig(p: TConnMssql): sql.config {
	const base: sql.config = {
		server:   p.server,
		port:     p.port ?? 1433,
		...(p.database ? { database: p.database } : {}),
		options: {
			trustServerCertificate: p.trustServerCertificate ?? false,
			encrypt:                p.encrypt ?? false,
		},
	}
	if (p.auth.kind === 'ntlm') {
		return { ...base, authentication: { type: 'ntlm', options: { domain: p.auth.domain, userName: p.auth.user, password: p.auth.password } } }
	}
	return { ...base, user: p.auth.user, password: p.auth.password }
}

export class MssqlAdapter implements IDbAdapter {
	constructor(private readonly p: TConnMssql) {}

	kind(): 'mssql' { return 'mssql' }
	title(): string { return `${this.p.server}:${this.p.port || 'DEFAULT'} [${this.p.database ?? 'default'}]` }
	user(): string {
		if (this.p.auth.kind === 'ntlm') return `${this.p.auth.domain}\\${this.p.auth.user}`
		return this.p.auth.user
	}
	versionQuery(): string { return 'SELECT @@VERSION AS ver' }
	pingQuery(): string { return 'SELECT 1 AS p' }

	async execStream(query: string, params: TDbParam[], callback: (event: TDbStreamEvent) => void, signal?: AbortSignal): Promise<void> {
		const pool = await new sql.ConnectionPool(buildPoolConfig(this.p)).connect()
		if (signal?.aborted) {
			try { await pool.close() } catch {}
			return
		}

		return new Promise<void>((resolve) => {
			const request = pool.request()
			request.stream = true
			for (const p of params) {
				request.input(p.name, p.value)
			}

			let error: string | undefined
			let done = false

			const onAbort = (): void => {
				if (done) return
				done = true
				try { request.cancel() } catch {}
				void pool.close().catch(() => {})
				resolve()
			}
			signal?.addEventListener('abort', onAbort)

			request.on('recordset', (columns: Record<string, unknown>) => {
				if (!done) callback({ kind: 'schema', schema: toSchema(columns) })
			})
			request.on('row', (row: object) => {
				if (!done) callback({ kind: 'row', row })
			})
			request.on('error', (err: Error) => {
				error = String(err)
			})
			request.on('done', async () => {
				if (done) return
				done = true
				signal?.removeEventListener('abort', onAbort)
				try { await pool.close() } catch {}
				callback({ kind: 'finish', error })
				resolve()
			})

			request.query(buildMssqlQuery(query))
		})
	}

	async execTxDynamic(cb: TDbTxCb, timeoutMs?: number): Promise<TResult<void>> {
		const pool = await new sql.ConnectionPool(buildPoolConfig(this.p)).connect()
		const txWork = async (): Promise<TResult<void>> => {
			const tx = new sql.Transaction(pool)
			try {
				await tx.begin()
				const exec = (query: string, params: TDbParam[]): Promise<TDbTxResult> => {
					const req = new sql.Request(tx)
					const q = buildMssqlQuery(query)
					for (const p of params) req.input(p.name, p.value)
					return new Promise<TDbTxResult>((resolve, reject) => {
						const onUncaught = (err: Error) => reject(err)
						process.once('uncaughtException', onUncaught)
						req.query(q)
							.then((res) => { process.removeListener('uncaughtException', onUncaught); resolve({ rows: (res.recordset ?? []) as Record<string, unknown>[] }) })
							.catch((err) => { process.removeListener('uncaughtException', onUncaught); reject(err) })
					})
				}
				await cb(exec)
				await tx.commit()
				return { ok: true, result: undefined }
			} catch (err) {
				try { await tx.rollback() } catch {}
				return { ok: false, error: String(err) }
			} finally {
				try { await pool.close() } catch {}
			}
		}
		if (!timeoutMs) return txWork()
		let timer: ReturnType<typeof setTimeout> | undefined
		const timeoutPromise = new Promise<TResult<void>>((resolve) => {
			timer = setTimeout(() => {
				void pool.close().catch(() => {})
				resolve({ ok: false, error: 'TIMEOUT' })
			}, timeoutMs)
		})
		try {
			return await Promise.race([txWork(), timeoutPromise])
		} finally {
			clearTimeout(timer)
		}
	}
}
