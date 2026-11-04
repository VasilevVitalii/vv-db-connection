import sql from 'mssql'
import type { ISqlType } from 'mssql'
import type { IDbAdapter, TDbCol, TDbError, TDbParam, TDbStreamEvent, TDbTxCb, TDbTxResult } from '../types'
import type { TConnMssql } from '../connection'
import type { TResult } from '../../tresult'
import { mapPlaceholders, SQL_ATOM_MSSQL } from '../placeholders'
import { toDbError, isDbError, withTxTimeout, abortError, assertTimeoutMs } from '../helpers'

// Converts :name placeholders to @name for mssql named parameters. A colon inside a
// string literal, a quoted identifier or a comment is left as it is, and a bracket-quoted
// identifier ([a:b]) is one identifier - SQL_ATOM_MSSQL keeps the brackets an atom. A
// placeholder that has no matching param is rejected here - otherwise mssql would send an
// undeclared @name and the error would surface only as a server-side "must declare the
// variable".
function buildMssqlQuery(query: string, params: TDbParam[]): string {
	return mapPlaceholders(query, (name) => {
		if (!params.some((p) => p.name === name)) throw new Error(`mssql: param not found: ${name}`)
		return `@${name}`
	}, undefined, SQL_ATOM_MSSQL)
}

// A forced parameter type (TDbParam.type) maps a driver-native type name ('DateTime2',
// 'NVarChar', ...) to the mssql type factory. Without it the driver infers the type from
// the JS value - a Date becomes DateTime (losing sub-second precision) and a long string
// becomes NVarChar - so a caller can pin the exact type here. Unknown names throw before
// the query is sent rather than failing server-side.
export function mssqlType(name: string): () => ISqlType {
	const t = (sql.TYPES as unknown as Record<string, (() => ISqlType) | undefined>)[name]
	if (t === undefined) throw new Error(`mssql: unknown param type: ${name}`)
	return t
}

// Validates a param without needing a request, so execStream can reject bad params
// (out/inout bind, unknown type) before connecting - otherwise the throw inside the
// request executor would leave the abort listener dangling on a request that was never
// sent.
function assertMssqlParam(p: TDbParam): void {
	// Only Oracle supports out/inout binds; reject them here rather than silently
	// binding an undefined value and inserting NULL.
	if (p.direction !== undefined && p.direction !== 'in') throw new Error(`mssql: ${p.direction} parameters are not supported`)
	if (p.type !== undefined) mssqlType(p.type)
}

export function mssqlInput(request: sql.Request, p: TDbParam): void {
	assertMssqlParam(p)
	if (p.type === undefined) request.input(p.name, p.value)
	else request.input(p.name, mssqlType(p.type), p.value)
}

export function toSchema(columns: Record<string, unknown>): TDbCol[] {
	return Object.values(columns).map((raw) => {
		const c = raw as {
			name?: string
			type?: { name?: string } | (() => { name?: string })
			length?: number
			scale?: number
			precision?: number
		}
		return {
			name:      c.name ?? '',
			// c.type is the type constructor (sql.Int, sql.VarChar, ...): its .name is the
			// type name. Calling it, c.type(), builds a declaration object ({ type, length })
			// that has no .name - the old code did that and reported 'unknown' for every column.
			kind:      (typeof c.type === 'function' ? c.type.name : c.type?.name) ?? 'unknown',
			len:       c.length,
			scale:     c.scale,
			precision: c.precision,
		}
	})
}

export function buildPoolConfig(p: TConnMssql): sql.config {
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
	constructor(private readonly p: TConnMssql) {
		assertTimeoutMs('mssql: connectTimeout', p.connectTimeout)
	}

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

	versionQuery(): string { return 'SELECT @@VERSION AS ver' }
	pingQuery(): string { return 'SELECT 1 AS p' }

	async execStream(query: string, params: TDbParam[], callback: (event: TDbStreamEvent) => void, signal?: AbortSignal): Promise<void> {
		// Validate params and rewrite the query before connecting: buildMssqlQuery and
		// mssqlInput throw on a missing/out/inout/unknown-type param. Doing it here means
		// the failure never holds a pool slot, and no abort listener is left dangling on a
		// request that was never sent. Like pg and fb, a build failure ends the call with a
		// finish event rather than a rejection.
		let q: string
		try {
			q = buildMssqlQuery(query, params)
			for (const p of params) assertMssqlParam(p)
		} catch (err) {
			try { callback({ kind: 'finish', error: toDbError(err, 'query') }) } catch { /* the consumer's finish handler threw */ }
			return
		}

		let pool: sql.ConnectionPool
		try {
			pool = await this.pool()
		} catch (err) {
			try { callback({ kind: 'finish', error: toDbError(err, 'connect') }) } catch { /* the consumer's finish handler threw */ }
			return
		}
		if (signal?.aborted) {
			try { callback({ kind: 'finish', error: abortError(signal.reason) }) } catch { /* the consumer's finish handler threw */ }
			return
		}

		return new Promise<void>((resolve) => {
			const request = pool.request()
			request.stream = true
			for (const p of params) {
				mssqlInput(request, p)
			}

			let done = false

			const onAbort = (): void => {
				if (done) return
				// Tear the borrowed connection down hard. cancel() only sends an ATTN and
				// waits for a server response that may never come, which strands the tarn
				// slot forever (contract: "tear down without waiting"). Destroying the socket
				// completes the request locally and releases the connection at once.
				try {
					const conn = (request as unknown as { _currentRequest?: { connection?: { close(): void } } })._currentRequest?.connection
					if (conn) conn.close()
					else request.cancel()
				} catch {}
				// A mid-query abort must still end the call with a finish, exactly as the
				// early-abort branch does - the contract is one finish per call.
				settle(abortError(signal?.reason))
			}
			// Ends the request: hands the error (if any) to the callback and resolves the
			// stream. The abort handler and the done/error events all funnel through here.
			const settle = (err?: TDbError): void => {
				if (done) return
				done = true
				signal?.removeEventListener('abort', onAbort)
				// A throw from the consumer's finish handler must not skip resolve()
				// (the stream contract is one finish + one resolve) nor reach the driver.
				try { callback({ kind: 'finish', error: err }) } catch { /* the consumer's finish handler threw */ }
				resolve()
			}
			signal?.addEventListener('abort', onAbort)

			// mssql streams a `rowsaffected` event once per statement, in execution order:
			// a SELECT reports the number of rows it returned, a DML the rows it changed -
			// numerically indistinguishable. The reliable way to tell them apart is the
			// `recordset` event, which only a SELECT emits before its rows. So a
			// A SELECT is delimited by its `recordset`/`row` pair; the `rowsaffected` that
			// follows belongs to that SELECT and equals the rows it emitted, so it is
			// dropped (those rows were already delivered). A `rowsaffected` without a
			// pending `recordset` is a DML and becomes its own empty-schema result.
			// Under SET NOCOUNT ON a SELECT emits no rowsaffected at all (the count is
			// suppressed together with DML counts), so the next DML's count arrives while
			// `inSelect` is still set; comparing it against the emitted row count recovers
			// the DML. A SELECT's count always equals its row count, so the only ambiguous
			// case is a NOCOUNT-suppressed SELECT followed by a DML affecting exactly as
			// many rows — that resolves to the SELECT and drops the DML (accepted edge).
			let inSelect = false
			let selectRows = 0

			request.on('recordset', (columns: Record<string, unknown>) => {
				inSelect = true
				selectRows = 0
				if (!done) callback({ kind: 'schema', schema: toSchema(columns) })
			})
			request.on('row', (row: object) => {
				if (inSelect) selectRows += 1
				if (!done) callback({ kind: 'row', row: row as Record<string, unknown> })
			})
			request.on('rowsaffected', (count: number) => {
				if (done) return
				if (inSelect) {
					inSelect = false
					if (typeof count === 'number' && count !== selectRows) {
						callback({ kind: 'schema', schema: [] })
						callback({ kind: 'rowsAffected', count })
					}
				} else if (typeof count === 'number') {
					callback({ kind: 'schema', schema: [] })
					callback({ kind: 'rowsAffected', count })
				}
			})
			request.on('error', (err: Error) => {
				// `done` does not always follow an error in every mssql release; waiting
				// for it would leave the query open until the 12h watchdog fires. Fail now.
				settle(toDbError(err, 'query'))
			})
			request.on('done', () => {
				if (done) return
				settle()
			})

			request.query(q)
		})
	}

	async execTxDynamic<T = void>(cb: TDbTxCb<T>, timeoutMs?: number, external?: AbortSignal): Promise<TResult<T, TDbError>> {
		return withTxTimeout(timeoutMs, async (signal, armCommitDeadline) => {
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
				return { ok: false, error: abortError(signal.reason) }
			}
			try {
				tx = new sql.Transaction(pool)
				await tx.begin()
				const transaction = tx
				const exec = async (query: string, params: TDbParam[]): Promise<TDbTxResult> => {
					// Contract (types.ts TDbTxExec): once the signal is aborted the
					// connection is being torn down, so exec() rejects rather than run
					// a statement on it (deterministic, not just via the socket teardown).
					if (signal.aborted) throw abortError(signal.reason)
					const req = new sql.Request(transaction)
					let q: string
					try {
						q = buildMssqlQuery(query, params)
						for (const p of params) mssqlInput(req, p)
					} catch (err) {
						// buildMssqlQuery/mssqlInput throw synchronously on a missing/out/
						// unknown-type param; the async wrapper turns it into a rejection so
						// exec(...).catch(...) sees it, and tagging it 'query' keeps it from
						// being misreported as kind 'user' (which the same query via exec()
						// would never be).
						throw toDbError(err, 'query')
					}
					return req.query(q).then(
						(res) => {
							const total = res.rowsAffected?.reduce((sum, n) => sum + n, 0) ?? 0
							// mssql fills `recordsets` only for SELECT statements: a DML leaves
							// no columns, so request.js returns before pushing (see its
							// `filter empty recordsets` guard). Each SELECT entry holds its
							// returned rows, and `rowsAffected` holds one count per statement
							// that reported one (every statement under default NOCOUNT OFF).
							// So a SELECT-only batch has as many recordsets as counts and must
							// report no DML count, while a batch that also ran DML reports
							// total minus the SELECT rows — 0 when a DML touched no rows.
							const recordsets = (res.recordsets as unknown as Array<unknown[] | undefined> | undefined) ?? []
							const selectRows = recordsets.reduce((sum, rs) => sum + (rs?.length ?? 0), 0)
							const dmlStatements = Math.max(0, (res.rowsAffected?.length ?? 0) - recordsets.length)
							const rowsAffected = dmlStatements > 0 ? total - selectRows : undefined
							return {
								rows:         (res.recordset ?? []) as Record<string, unknown>[],
								rowsAffected,
							}
						},
						// Tag the failure as a DB query error so it stays 'query' even when
						// it bubbles through the user's callback (which may re-throw it).
						(err) => { throw toDbError(err, 'query') },
					)
				}
				// The callback's resolved value is the transaction's result: it is only
				// assigned on success (the catch below always returns), so the `!` is the
				// "assigned before every fall-through" assertion, not a suppressed init bug.
				let result!: T
				try {
					result = await cb(exec, signal)
				} catch (err) {
					if (!signal.aborted) { try { await transaction.rollback() } catch {} }
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
					await transaction.commit()
				} catch (err) {
					if (!signal.aborted) { try { await transaction.rollback() } catch {} }
					return { ok: false, error: toDbError(err, 'tx') }
				}
				return { ok: true, result }
			} catch (err) {
				// begin() failing is the only thing that reaches here now.
				if (tx && !signal.aborted) { try { await tx.rollback() } catch {} }
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
