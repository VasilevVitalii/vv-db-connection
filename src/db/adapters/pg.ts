import { Client, Pool, Query, type FieldDef, type PoolClient, type QueryResult } from 'pg'
import type { IDbAdapter, TDbCol, TDbError, TDbParam, TDbStreamEvent, TDbTxCb, TDbTxResult } from '../types'
import type { TConnPg } from '../connection'
import type { TResult } from '../../tresult'
import { mapPlaceholders, maskPgBlockComments, SQL_ATOM_PG } from '../placeholders'
import { toDbError, isDbError, withTxTimeout, abortError, assertTimeoutMs } from '../helpers'

// PostgreSQL RowDescription carries only the type OID, not its name. Map the built-in
// scalar and array types to their canonical names so `kind` is readable instead of a
// numeric OID; anything unmapped (extension/custom types) falls back to the OID string.
const PG_TYPE_NAME: Record<number, string> = {
	16: 'bool', 17: 'bytea', 18: 'char', 19: 'name', 20: 'int8', 21: 'int2', 22: 'int2vector',
	23: 'int4', 24: 'regproc', 25: 'text', 26: 'oid', 27: 'tid', 28: 'xid', 29: 'cid',
	30: 'oidvector', 114: 'json', 142: 'xml', 194: 'pg_node_tree', 600: 'point', 601: 'lseg',
	602: 'path', 603: 'box', 604: 'polygon', 628: 'line', 650: 'cidr', 700: 'float4',
	701: 'float8', 705: 'unknown', 718: 'circle', 774: 'macaddr8', 790: 'money', 829: 'macaddr',
	869: 'inet', 1042: 'bpchar', 1043: 'varchar', 1082: 'date', 1083: 'time', 1114: 'timestamp',
	1184: 'timestamptz', 1186: 'interval', 1266: 'timetz', 1560: 'bit', 1562: 'varbit',
	1700: 'numeric', 1790: 'refcursor', 2202: 'regprocedure', 2203: 'regoper', 2204: 'regoperator',
	2205: 'regclass', 2206: 'regtype', 2950: 'uuid', 3220: 'pg_lsn', 3614: 'tsvector',
	3615: 'tsquery', 3642: 'gtsvector', 3734: 'regconfig', 3769: 'regdictionary', 3802: 'jsonb',
	3904: 'int4range', 3906: 'numrange', 3908: 'tsrange', 3910: 'tstzrange', 3912: 'daterange',
	3926: 'int8range', 4089: 'regnamespace', 4096: 'regrole',
	// built-in array types (fixed OIDs)
	199: 'json[]', 651: 'cidr[]', 1000: 'bool[]', 1001: 'bytea[]', 1002: 'char[]', 1003: 'name[]',
	1005: 'int2[]', 1007: 'int4[]', 1009: 'text[]', 1014: 'bpchar[]', 1015: 'varchar[]',
	1016: 'int8[]', 1021: 'float4[]', 1022: 'float8[]', 1040: 'macaddr8[]', 1041: 'inet[]',
	1115: 'timestamp[]', 1182: 'date[]', 1183: 'time[]', 1185: 'timestamptz[]', 1187: 'interval[]',
	1231: 'numeric[]', 2951: 'uuid[]', 3807: 'jsonb[]',
}

// RowDescription carries the type OID plus a dataTypeModifier (the catalog's atttypmod)
// that encodes the declared length/precision. Decode it for the types where it means
// something: varchar(n)/char(n)/bit(n)/varbit(n) report their length, numeric(p,s) its
// precision and scale. -1 (the modifier of an unbounded type) is left alone.
export function toSchema(fields: FieldDef[]): TDbCol[] {
	return fields.map((f) => {
		const col: TDbCol = { name: f.name, kind: PG_TYPE_NAME[f.dataTypeID] ?? String(f.dataTypeID) }
		const mod = f.dataTypeModifier
		if (mod >= 4) {
			const m = mod - 4
			// numeric: ((precision << 16) | scale) + 4
			if (f.dataTypeID === 1700) {
				col.precision = m >> 16
				col.scale = m & 0xffff
			} else if (f.dataTypeID === 1043 || f.dataTypeID === 1042 || f.dataTypeID === 18 || f.dataTypeID === 1560 || f.dataTypeID === 1562) {
				col.len = m
			}
		}
		return col
	})
}

// Converts :name placeholders to $1, $2, ... and collects values.
// Same :name used multiple times maps to the same $N. A colon inside a string literal, a
// quoted identifier or a comment is left as it is.
function buildQuery(query: string, params: TDbParam[]): { text: string; values: unknown[] } {
	const nameToIndex = new Map<string, number>()
	const values: unknown[] = []
	// pg nests block comments; mask them (nesting included) before the placeholder scan
	// so a :name inside an outer comment is not rewritten, then restore the tokens after.
	const { masked, restore } = maskPgBlockComments(query)
	const rewritten = mapPlaceholders(masked, (name) => {
		if (!nameToIndex.has(name)) {
			const found = params.find((p) => p.name === name)
			if (!found) throw new Error(`pg: param not found: ${name}`)
			if (found.direction !== undefined && found.direction !== 'in') throw new Error(`pg: ${found.direction} parameters are not supported`)
			values.push(found.value)
			nameToIndex.set(name, values.length)
		}
		return `$${nameToIndex.get(name)}`
	}, undefined, SQL_ATOM_PG)
	return { text: restore(rewritten), values }
}

// Result commands that report affected rows. Keying off this whitelist (not "!== 'SELECT'")
// keeps FETCH/COPY/CREATE TABLE AS from surfacing a spurious count and keeps
// INSERT..RETURNING reporting its real one.
const DML_COMMANDS = new Set(['INSERT', 'UPDATE', 'DELETE', 'MERGE'])
function isDmlCommand(command: string): boolean {
	return DML_COMMANDS.has(command)
}

export class PgAdapter implements IDbAdapter {
	constructor(private readonly p: TConnPg) {
		assertTimeoutMs('pg: connectTimeout', p.connectTimeout)
	}

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

	// Cancel the running query on the server and rip the socket. A graceful end() would
	// send Terminate, which the backend reads only after the running query has finished -
	// so a stuck query would outlive it. Cancel + destroy is the instant exit shared by
	// execStream's abort and execTxDynamic's timeout teardown, so the two paths cannot
	// drift apart (previously each carried its own copy of these three lines).
	private _teardownClient(client: PoolClient): void {
		const internals = client as unknown as { processID?: number; connection?: { stream?: { destroy(): void } } }
		if (internals.processID) this._cancelBackend(internals.processID)
		try { internals.connection?.stream?.destroy() } catch {}
	}

	async execStream(query: string, params: TDbParam[], callback: (event: TDbStreamEvent) => void, signal?: AbortSignal): Promise<void> {
		// Validate params and rewrite the query before borrowing a pool slot: buildQuery
		// throws on a missing param, and a throw after connect() would occupy the slot
		// (and route the error through a client that was never needed). Same order as
		// mssql, ora and fb.
		let q: ReturnType<typeof buildQuery>
		try {
			q = buildQuery(query, params)
		} catch (err) {
			try { callback({ kind: 'finish', error: toDbError(err, 'query') }) } catch { /* the consumer's finish handler threw */ }
			return
		}
		let client: PoolClient
		try {
			client = await this.pool().connect()
		} catch (err) {
			try { callback({ kind: 'finish', error: toDbError(err, 'connect') }) } catch { /* the consumer's finish handler threw */ }
			return
		}
		// The socket is destroyed rather than ended: a graceful end waits for a server
		// that may be the very thing that stopped answering. The query then fails, and the
		// client also emits "error" - with nobody listening that would be thrown. That same
		// event is the one reliable signal that the connection itself died (a SQL error
		// rejects the query but leaves the client healthy), so it also decides whether the
		// client is returned to the pool or dropped.
		let broken = false
		const onError = (): void => { broken = true }
		client.on('error', onError)
		const onAbort = (): void => {
			this._teardownClient(client)
		}
		signal?.addEventListener('abort', onAbort)
		// connect() may have resolved after the deadline already fired: the listener
		// above was added too late to ever run, and the try/finally below would otherwise
		// hand a healthy client back while its query still runs on the server. Tear the
		// client down and drop it here, exactly as execTxDynamic's early-abort branch does.
		if (signal?.aborted) {
			onAbort()
			client.removeListener('error', onError)
			client.release(true)
			try { callback({ kind: 'finish', error: abortError(signal.reason) }) } catch { /* the consumer's finish handler threw */ }
			return
		}
		try {
			const { text, values } = q
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
					// Report rows changed by a DML (whitelist, so FETCH/COPY/CTAS stay out).
					if (isDmlCommand(rs.command) && typeof rs.rowCount === 'number') {
						callback({ kind: 'rowsAffected', count: rs.rowCount })
					}
				}
			} else {
				await new Promise<void>((resolve, reject) => {
					// `stream`, not `q`: the outer `q` (the rewritten query) is still in
					// scope here, and shadowing it made the two reads of the name confusing.
					const stream = new Query({ text, values })
					let schemaEmitted = false
					stream.on('row', (row: Record<string, unknown>, result?: { fields?: FieldDef[] }) => {
						if (!schemaEmitted) {
							schemaEmitted = true
							callback({ kind: 'schema', schema: toSchema(result?.fields ?? []) })
						}
						callback({ kind: 'row', row })
					})
					stream.on('error', (err: Error) => reject(err))
					stream.on('end', (result: { fields?: FieldDef[]; rowCount?: number | null; command?: string }) => {
						if (!schemaEmitted) {
							callback({ kind: 'schema', schema: toSchema(result.fields ?? []) })
						}
						// Report rows changed by a DML (whitelist, so FETCH/COPY/CTAS stay out).
						if (result.command !== undefined && isDmlCommand(result.command) && typeof result.rowCount === 'number') {
							callback({ kind: 'rowsAffected', count: result.rowCount })
						}
						resolve()
					})
					client.query(stream)
				})
			}
			try {
				callback({ kind: 'finish' })
			} catch { /* the consumer's finish handler threw - nothing left to report */ }
		} catch (err) {
			try { callback({ kind: 'finish', error: signal?.aborted ? abortError(signal.reason) : toDbError(err, 'query') }) } catch { /* the consumer's finish handler threw */ }
		} finally {
			signal?.removeEventListener('abort', onAbort)
			// Remove the error listener before handing the client back: the pool reuses the
			// same PoolClient, so an un-removed listener accumulates one per query and trips
			// MaxListeners (and each stale closure would keep a dead `broken` alive).
			client.removeListener('error', onError)
			// release(err) drops the client from the pool: pass it on a broken connection so
			// a dead socket is not handed to the next query. A plain SQL error keeps the
			// client healthy and is released back normally.
			client.release(broken || signal?.aborted)
		}
	}

	async execTxDynamic<T = void>(cb: TDbTxCb<T>, timeoutMs?: number, external?: AbortSignal): Promise<TResult<T, TDbError>> {
		return withTxTimeout(timeoutMs, async (signal, armCommitDeadline) => {
			let client: PoolClient
			try {
				client = await this.pool().connect()
			} catch (err) {
				return { ok: false, error: toDbError(err, 'connect') }
			}
			// A connection lost with no query in flight emits "error" on the client; with
			// nobody listening that would be thrown. Queries reject on their own. The same
			// event marks the connection as broken so release() drops it instead of returning
			// a dead socket to the pool.
			let broken = false
			const onError = (): void => { broken = true }
			client.on('error', onError)
			// On a timeout a graceful end() would send Terminate, which the backend only
			// reads after the running query has finished - so a stuck query would outlive
			// it. Cancel it on the server and rip the socket, as execStream's onAbort does.
			const teardown = (): void => {
				this._teardownClient(client)
			}
			signal.addEventListener('abort', teardown, { once: true })
			// connect() may have resolved after the deadline already fired: the listener
			// above was added too late to ever run, and the finally below skips release()
			// on an aborted signal. Rip the socket here and release the (now broken) client,
			// so the pool slot is returned instead of leaking it.
			if (signal.aborted) {
				teardown()
				client.removeListener('error', onError)
				client.release(true)
				return { ok: false, error: abortError(signal.reason) }
			}
			try {
				await client.query('BEGIN')
				const exec = async (query: string, params: TDbParam[]): Promise<TDbTxResult> => {
					if (signal.aborted) throw abortError(signal.reason)
					let q: ReturnType<typeof buildQuery>
					try {
						q = buildQuery(query, params)
					} catch (err) {
						// buildQuery throws synchronously on a missing param; the async
						// wrapper turns it into a rejection so exec(...).catch(...) sees it,
						// and tagging it 'query' keeps it from being misreported as 'user'.
						throw toDbError(err, 'query')
					}
					return client.query({ text: q.text, values: q.values }).then(
						(res) => {
							// A parameterless multi-statement query runs the simple protocol,
							// whose results pg delivers as an array of per-statement results
							// (each with its own rows/command/rowCount).
							if (Array.isArray(res)) {
								// README contract: a transaction statement returns a single set —
								// the first recordset, the rest dropped (same as mssql). A DML
								// statement contributes no recordset (empty fields), so "first
								// recordset" = the first set that has columns. A set with zero
								// columns (e.g. `SELECT FROM t` without a column list) has no fields
								// and is skipped — such a shape is not a recordset callers rely on.
								const first = res.find((r) => (r.fields?.length ?? 0) > 0)
								const dml = res.filter((r) => isDmlCommand(r.command))
								const affected = dml.reduce((n, r) => n + (r.rowCount ?? 0), 0)
								return {
									rows: (first?.rows ?? []) as Record<string, unknown>[],
									...(dml.length > 0 ? { rowsAffected: affected } : {}),
								}
							}
							return {
								rows: res.rows as Record<string, unknown>[],
								// DML only (TDbRes.rowsAffected contract): report the count for
								// INSERT/UPDATE/DELETE/MERGE (RETURNING included), nothing for
								// SELECT/FETCH/COPY/CTAS.
								...(isDmlCommand(res.command) && res.rowCount != null ? { rowsAffected: res.rowCount } : {}),
							}
						},
						// Tag the failure as a DB query error so it stays 'query' even when
						// it bubbles through the user's callback (which may re-throw it).
						(err) => { throw toDbError(err, 'query') },
					)
				}
				let result!: T
				try {
					result = await cb(exec, signal)
				} catch (err) {
					if (!signal.aborted) { try { await client.query('ROLLBACK') } catch {} }
					return { ok: false, error: isDbError(err) ? err : toDbError(err, 'user') }
				}
				try {
					// The deadline may already have fired while cb was still running: if
					// the signal is aborted, do NOT commit - the caller already holds
					// 'timeout'/'aborted' and a commit now would double-apply on retry.
					// This is the deterministic backstop (teardown is best-effort).
					if (signal.aborted) return { ok: false, error: abortError(signal.reason) }
					// The COMMIT phase is bounded by a hard timer (armCommitDeadline) that
					// reports an ambiguous outcome (COMMIT_TIMEOUT) rather than 'timeout': the
					// server may already have committed, so a plain timeout would make a
					// retry double-apply. The external abort still applies.
					armCommitDeadline()
					await client.query('COMMIT')
				} catch (err) {
					if (!signal.aborted) { try { await client.query('ROLLBACK') } catch {} }
					return { ok: false, error: toDbError(err, 'tx') }
				}
				return { ok: true, result }
			} catch (err) {
				// BEGIN failing is the only thing that reaches here now.
				if (!signal.aborted) { try { await client.query('ROLLBACK') } catch {} }
				return { ok: false, error: toDbError(err, 'tx') }
			} finally {
				signal.removeEventListener('abort', teardown)
				client.removeListener('error', onError)
				client.release(broken || signal.aborted)
			}
		}, external)
	}
}
