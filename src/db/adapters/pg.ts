import { Client, type FieldDef } from 'pg'
import type { IDbAdapter, TDbCol, TDbParam, TDbStreamEvent, TDbTxCb, TDbTxResult } from '../types'
import type { TConnPg } from '../connection'
import type { TResult } from '../../tresult'

function toSchema(fields: FieldDef[]): TDbCol[] {
	return fields.map((f) => ({ name: f.name, kind: 'unknown' }))
}

// Converts :name placeholders to $1, $2, ... and collects values.
// Same :name used multiple times maps to the same $N.
function buildQuery(query: string, params: TDbParam[]): { text: string; values: unknown[] } {
	const nameToIndex = new Map<string, number>()
	const values: unknown[] = []
	const text = query.replace(/:(\w+)/g, (_, name: string) => {
		if (!nameToIndex.has(name)) {
			const found = params.find((p) => p.name === name)
			if (!found) throw new Error(`pg: param not found: ${name}`)
			values.push(found.value)
			nameToIndex.set(name, values.length)
		}
		return `$${nameToIndex.get(name)}`
	})
	return { text, values }
}

export class PgAdapter implements IDbAdapter {
	constructor(private readonly p: TConnPg) {}

	kind(): 'pg' { return 'pg' }
	title(): string { return `${this.p.host}:${this.p.port || 'DEFAULT'} [${this.p.database}]` }
	user(): string { return this.p.user }
	versionQuery(): string { return 'SELECT version() AS ver' }
	pingQuery(): string { return 'SELECT 1 AS p' }

	private _clientConfig() {
		return {
			host:     this.p.host,
			port:     this.p.port ?? 5432,
			database: this.p.database,
			user:     this.p.user,
			password: this.p.password,
			ssl:      this.p.ssl ? { rejectUnauthorized: false } : undefined,
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
		const client = new Client(this._clientConfig())
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
			await client.connect()
			const { text, values } = buildQuery(query, params)
			const result = await client.query({ text, values })
			callback({ kind: 'schema', schema: toSchema(result.fields) })
			for (const row of result.rows) {
				callback({ kind: 'row', row })
			}
			callback({ kind: 'finish' })
		} catch (err) {
			callback({ kind: 'finish', error: String(err) })
		} finally {
			signal?.removeEventListener('abort', onAbort)
			try { await client.end() } catch {}
		}
	}

	async execTxDynamic(cb: TDbTxCb, timeoutMs?: number): Promise<TResult<void>> {
		const client = new Client({
			host:     this.p.host,
			port:     this.p.port ?? 5432,
			database: this.p.database,
			user:     this.p.user,
			password: this.p.password,
			ssl:      this.p.ssl ? { rejectUnauthorized: false } : undefined,
			options:  '-c client_encoding=UTF8',
		})
		const txWork = async (): Promise<TResult<void>> => {
			try {
				await client.connect()
				await client.query('BEGIN')
				const exec = (query: string, params: TDbParam[]): Promise<TDbTxResult> => {
					const { text, values } = buildQuery(query, params)
					return new Promise<TDbTxResult>((resolve, reject) => {
						const onUncaught = (err: Error) => reject(err)
						process.once('uncaughtException', onUncaught)
						client.query({ text, values })
							.then((res) => { process.removeListener('uncaughtException', onUncaught); resolve({ rows: res.rows as Record<string, unknown>[] }) })
							.catch((err) => { process.removeListener('uncaughtException', onUncaught); reject(err) })
					})
				}
				await cb(exec)
				await client.query('COMMIT')
				return { ok: true, result: undefined }
			} catch (err) {
				try { await client.query('ROLLBACK') } catch {}
				return { ok: false, error: String(err) }
			} finally {
				try { await client.end() } catch {}
			}
		}
		if (!timeoutMs) return txWork()
		let timer: ReturnType<typeof setTimeout> | undefined
		const timeoutPromise = new Promise<TResult<void>>((resolve) => {
			timer = setTimeout(() => {
				void client.end().catch(() => {})
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
