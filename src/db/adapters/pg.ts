import { Client, Pool, Query, type FieldDef, type PoolClient, type QueryResult } from 'pg'
import type { IDbAdapter, TDbCol, TDbError, TDbParam, TDbStreamEvent, TDbTxCb, TDbTxResult } from '../types'
import type { TConnPg } from '../connection'
import type { TResult } from '../../tresult'
import { mapPlaceholders, SQL_ATOM_PG } from '../placeholders'
import { toDbError, withTxTimeout } from '../helpers'

function toSchema(fields: FieldDef[]): TDbCol[] {
	return fields.map((f) => ({ name: f.name, kind: String(f.dataTypeID) }))
}

// Converts :name placeholders to $1, $2, ... and collects values.
// Same :name used multiple times maps to the same $N. A colon inside a string literal, a
// quoted identifier or a comment is left as it is.
function buildQuery(query: string, params: TDbParam[]): { text: string; values: unknown[] } {
	const nameToIndex = new Map<string, number>()
	const values: unknown[] = []
	const text = mapPlaceholders(query, (name) => {
		if (!nameToIndex.has(name)) {
			const found = params.find((p) => p.name === name)
			if (!found) throw new Error(`pg: param not found: ${name}`)
			values.push(found.value)
			nameToIndex.set(name, values.length)
		}
		return `$${nameToIndex.get(name)}`
	}, undefined, SQL_ATOM_PG)
	return { text, values }
}

export class PgAdapter implements IDbAdapter {
	constructor(private readonly p: TConnPg) {}

	kind(): 'pg' { return 'pg' }
	title(): string { return `${this.p.host}:${this.p.port || 'DEFAULT'} [${this.p.database}]` }
	user(): string { return this.p.user }
	versionQuery(): string { return 'SELECT version() AS ver' }
	pingQuery(): string { return 'SELECT 1 AS p' }

	private _pool?: Pool
	private pool(): Pool {
		if (!this._pool) {
			this._pool = new Pool(this._clientConfig())
			// An idle client that loses its connection emits "error" on the pool; with no
			// listener that would be thrown. The client in use rejects on its own.
			this._pool.on('error', () => {})
		}
		return this._pool
	}

	async close(): Promise<void> {
		if (this._pool) {
			try { await this._pool.end() } catch {}
			this._pool = undefined
		}
	}

	private _sslConfig() {
		const ssl = this.p.ssl
		if (!ssl) return undefined
		if (ssl === true) return { rejectUnauthorized: false }
		// trustServerCertificate is the same flag mssql uses: true = accept the server
		// certificate without verifying it. The driver speaks rejectUnauthorized, so invert.
		return {
			rejectUnauthorized: ssl.trustServerCertificate !== true,
			...(ssl.ca ? { ca: ssl.ca } : {}),
		}
	}

	private _clientConfig() {
		return {
			host:     this.p.host,
			port:     this.p.port ?? 5432,
			database: this.p.database,
			user:     this.p.user,
			password: this.p.password,
			ssl:      this._sslConfig(),
			...(this.p.connectTimeout ? { connectionTimeoutMillis: this.p.connectTimeout } : {}),
			options:  '-c client_encoding=UTF8',
		}
	}

	// A backend does not notice its client is gone until it has something to send, so a
	// query cut off on our side would go on to its very end on the server. It is cancelled
	// from a connection of its own; a server that does not answer that either is given up
	// on after a while - this runs in the background and nobody waits for it.
	private _cancelBackend(pid: number): void {
		const c = new Client({ ...this._clientConfig(), connectionTimeoutMillis: 30_000, query_timeout: 30_000 })
		c.on('error', () => {})
		void c.connect()
			.then(() => c.query('SELECT pg_cancel_backend($1)', [pid]))
			.catch(() => {})
			.finally(() => { void c.end().catch(() => {}) })
	}

	async execStream(query: string, params: TDbParam[], callback: (event: TDbStreamEvent) => void, signal?: AbortSignal): Promise<void> {
		let client: PoolClient
		try {
			client = await this.pool().connect()
		} catch (err) {
			callback({ kind: 'finish', error: toDbError(err, 'connect') })
			return
		}
		// The socket is destroyed rather than ended: a graceful end waits for a server
		// that may be the very thing that stopped answering. The query then fails, and the
		// client also emits "error" - with nobody listening that would be thrown.
		client.on('error', () => {})
		const onAbort = (): void => {
			const internals = client as unknown as { processID?: number; connection?: { stream?: { destroy(): void } } }
			if (internals.processID) this._cancelBackend(internals.processID)
			try { internals.connection?.stream?.destroy() } catch {}
		}
		signal?.addEventListener('abort', onAbort)
		try {
			const { text, values } = buildQuery(query, params)
			// Multi-statement works only on the simple query protocol (no parameters) and
			// comes back buffered as an array of results. A parameterized query goes over
			// the extended protocol, where Query streams rows instead of buffering them.
			if (values.length === 0) {
				const result = await client.query(text)
				const sets = (Array.isArray(result) ? result : [result]) as QueryResult[]
				for (const rs of sets) {
					callback({ kind: 'schema', schema: toSchema(rs.fields ?? []) })
					for (const row of rs.rows) {
						callback({ kind: 'row', row })
					}
				}
			} else {
				await new Promise<void>((resolve, reject) => {
					const q = new Query({ text, values })
					let schemaEmitted = false
					q.on('row', (row: Record<string, unknown>, result?: { fields?: FieldDef[] }) => {
						if (!schemaEmitted) {
							schemaEmitted = true
							callback({ kind: 'schema', schema: toSchema(result?.fields ?? []) })
						}
						callback({ kind: 'row', row })
					})
					q.on('error', (err: Error) => reject(err))
					q.on('end', (result: { fields?: FieldDef[] }) => {
						if (!schemaEmitted) {
							callback({ kind: 'schema', schema: toSchema(result.fields ?? []) })
						}
						resolve()
					})
					client.query(q)
				})
			}
			callback({ kind: 'finish' })
		} catch (err) {
			callback({ kind: 'finish', error: toDbError(err, 'query') })
		} finally {
			signal?.removeEventListener('abort', onAbort)
			client.release(signal?.aborted)
		}
	}

	async execTxDynamic(cb: TDbTxCb, timeoutMs?: number): Promise<TResult<void, TDbError>> {
		return withTxTimeout(timeoutMs, async (signal) => {
			let client: PoolClient
			try {
				client = await this.pool().connect()
			} catch (err) {
				return { ok: false, error: toDbError(err, 'connect') }
			}
			// A connection lost with no query in flight emits "error" on the client; with
			// nobody listening that would be thrown. Queries reject on their own.
			client.on('error', () => {})
			// On a timeout a graceful end() would send Terminate, which the backend only
			// reads after the running query has finished - so a stuck query would outlive
			// it. Cancel it on the server and rip the socket, as execStream's onAbort does.
			const teardown = (): void => {
				const internals = client as unknown as { processID?: number; connection?: { stream?: { destroy(): void } } }
				if (internals.processID) this._cancelBackend(internals.processID)
				try { internals.connection?.stream?.destroy() } catch {}
			}
			signal.addEventListener('abort', teardown, { once: true })
			// connect() may have resolved after the deadline already fired: the listener
			// above was added too late to ever run, and the finally below skips release()
			// on an aborted signal. Rip the socket here instead of leaking the pool client.
			if (signal.aborted) {
				teardown()
				return { ok: false, error: { message: 'TIMEOUT', kind: 'timeout' } }
			}
			try {
				await client.query('BEGIN')
				const exec = (query: string, params: TDbParam[]): Promise<TDbTxResult> => {
					const { text, values } = buildQuery(query, params)
					return client.query({ text, values }).then((res) => ({ rows: res.rows as Record<string, unknown>[] }))
				}
				await cb(exec, signal)
				await client.query('COMMIT')
				return { ok: true, result: undefined }
			} catch (err) {
				try { await client.query('ROLLBACK') } catch {}
				return { ok: false, error: toDbError(err, 'query') }
			} finally {
				signal.removeEventListener('abort', teardown)
				client.release(signal.aborted)
			}
		})
	}
}
