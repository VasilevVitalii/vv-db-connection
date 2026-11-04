import oracledb from 'oracledb'
import type { IDbAdapter, TDbCol, TDbError, TDbParam, TDbRes, TDbStreamEvent, TDbTxCb, TDbTxResult } from '../types'
import type { TConnOra } from '../connection'
import type { TResult } from '../../tresult'
import { mapPlaceholders, SQL_ATOM_ORA } from '../placeholders'
import { toDbError, isDbError, withTxTimeout, abortError, assertTimeoutMs } from '../helpers'

type OraMetadata = { name: string; dbTypeName: string; byteSize?: number; scale?: number; precision?: number }
type OraBindValue = oracledb.BindParameter | string | number | bigint | Date | Buffer | null | undefined

// A forced parameter type (TDbParam.type) maps a driver-native type name ('NUMBER',
// 'VARCHAR2', 'DATE', 'CLOB', ...) to the oracledb DbType. Without it the driver infers
// the type from the JS value; forcing it pins the exact bind type. Unknown names throw
// before the query is sent rather than failing server-side.
const ORA_IN_TYPES: Record<string, oracledb.DbType> = {
	VARCHAR2:  oracledb.DB_TYPE_VARCHAR,
	NVARCHAR2: oracledb.DB_TYPE_NVARCHAR,
	CHAR:      oracledb.DB_TYPE_CHAR,
	NUMBER:    oracledb.DB_TYPE_NUMBER,
	DATE:      oracledb.DB_TYPE_DATE,
	TIMESTAMP: oracledb.DB_TYPE_TIMESTAMP,
	CLOB:      oracledb.DB_TYPE_CLOB,
	BLOB:      oracledb.DB_TYPE_BLOB,
	RAW:       oracledb.DB_TYPE_RAW,
	BOOLEAN:   oracledb.DB_TYPE_BOOLEAN,
	JSON:      oracledb.DB_TYPE_JSON,
}

export function oraBind(p: TDbParam): OraBindValue {
	const dir = p.direction === 'out' ? oracledb.BIND_OUT : p.direction === 'inout' ? oracledb.BIND_INOUT : oracledb.BIND_IN
	if (p.type === undefined) {
		// OUT/INOUT have no server-side type to allocate the out buffer from - the caller
		// must force one, or the driver answers NJS-0058 for the missing type at execute time.
		if (dir !== oracledb.BIND_IN) throw new Error(`ora: ${p.direction} parameter ${p.name} needs a type`)
		return p.value as OraBindValue
	}
	const t = ORA_IN_TYPES[p.type]
	if (t === undefined) throw new Error(`ora: unknown param type: ${p.type}`)
	// An OUT bind carries no input value; INOUT keeps the caller's value as its in value.
	return { dir, type: t, val: dir === oracledb.BIND_OUT ? undefined : p.value } as OraBindValue
}

function toSchema(meta: OraMetadata[]): TDbCol[] {
	return meta.map((m) => ({
		name:      m.name,
		kind:      m.dbTypeName,
		len:       m.byteSize,
		scale:     m.scale,
		precision: m.precision,
	}))
}

// res.implicitResults, with resultSet:true, is an array of ResultSets - one per
// DBMS_SQL.RETURN_RESULT in a PL/SQL block - each carrying its own metadata. Read and
// close every one (an unclosed ResultSet keeps its cursor open and the connection busy,
// and oracledb does not auto-close the resultSet:true form) and return one { schema, rows }
// per result set, so the caller can emit them as their own results instead of dropping
// the rows silently.
async function readImplicitResults(res: oracledb.Result<Record<string, unknown>>): Promise<TDbRes[]> {
	const out: TDbRes[] = []
	for (const item of res.implicitResults ?? []) {
		// With resultSet:false each element would be a plain row array (no metadata);
		// the adapter always executes with resultSet:true, so skip that shape defensively.
		if (Array.isArray(item)) continue
		try {
			out.push({ schema: toSchema(item.metaData as unknown as OraMetadata[]), rows: await item.getRows() })
		} finally {
			await item.close()
		}
	}
	return out
}

// :name stays :name for the driver, but only the bindings the SQL actually mentions are
// passed: the other adapters ignore extra parameters, and so must Oracle, or the driver
// answers NJS-098 on a bind that has no placeholder. A placeholder with no matching
// parameter is rejected here - otherwise Oracle would answer ORA-01008 from the server
// instead of a clear client-side "param not found" like pg and fb give.
export function buildQuery(query: string, params: TDbParam[]): { sql: string; binds: Record<string, OraBindValue> } {
	const used = new Set<string>()
	// SQL_ATOM_ORA additionally treats q'[...]' alternative quoting as one string, so a
	// :name inside it is plain text. :new/:old are trigger correlation names, but only
	// inside a CREATE [OR REPLACE] [EDITIONABLE|NONEDITIONABLE] TRIGGER, and without
	// regard to case (:NEW, :Old, ...). A REFERENCING clause renames them (REFERENCING
	// NEW AS n OLD AS o, AS optional) and those custom names are correlation names too.
	// Outside a trigger they are ordinary bind names, so UPDATE ... WHERE ver = :old must
	// still bind instead of being silently skipped (the server would answer ORA-01008).
	const isTrigger = /^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:EDITIONABLE\s+|NONEDITIONABLE\s+)?TRIGGER\b/i.test(query)
	const correlation = new Set(['new', 'old'])
	if (isTrigger) {
		// Bound the REFERENCING clause by the first token that starts the row/body part,
		// so a stray OLD/NEW/PARENT word in the trigger body is not mistaken for a rename.
		const clause = /\bREFERENCING\b([\s\S]*?)(?=\b(?:FOR\s+EACH\s+ROW|WHEN|BEGIN|DECLARE|CALL|COMPOUND)\b|$)/i.exec(query)?.[1]
		if (clause) {
			for (const m of clause.matchAll(/\b(?:OLD|NEW|PARENT)\s+(?:AS\s+)?([A-Za-z_]\w*)/gi)) {
				const name = m[1]
				if (name) correlation.add(name.toLowerCase())
			}
		}
	}
	const sql = mapPlaceholders(query, (name) => {
		if (isTrigger && correlation.has(name.toLowerCase())) return `:${name}`
		used.add(name)
		return `:${name}`
	}, undefined, SQL_ATOM_ORA)
	for (const name of used) {
		if (!params.some((p) => p.name.toLowerCase() === name.toLowerCase())) throw new Error(`ora: param not found: ${name}`)
	}
	// Oracle bind names are case-insensitive, so two params differing only by case (:v vs
	// :V) would silently overwrite each other's bind - reject the conflict up front.
	const seen = new Set<string>()
	for (const p of params) {
		const lower = p.name.toLowerCase()
		if (seen.has(lower)) throw new Error(`ora: duplicate param (case-insensitive): ${p.name}`)
		seen.add(lower)
	}
	const binds: Record<string, OraBindValue> = {}
	for (const p of params) {
		// Oracle bind names are case-insensitive: bind the value under every spelling the
		// query actually uses, so `:V` and `:v` (the same name) both resolve the param.
		for (const usedName of used) {
			if (usedName.toLowerCase() === p.name.toLowerCase()) binds[usedName] = oraBind(p)
		}
	}
	return { sql, binds }
}

// Whether the statement returns rows (SELECT / WITH), ignoring any leading whitespace and
// comments (/* hint */, -- comment). queryStream() only runs a query that returns rows;
// DML and PL/SQL go through execute(), which discards rows - a SELECT hidden behind a
// leading comment used to be misrouted there and return an empty result silently.
export function isRowQuery(sql: string): boolean {
	let i = 0
	const n = sql.length
	for (;;) {
		while (i < n && /\s/.test(sql.charAt(i))) i++
		if (sql.startsWith('--', i)) {
			const nl = sql.indexOf('\n', i)
			if (nl < 0) return false
			i = nl + 1
			continue
		}
		if (sql.startsWith('/*', i)) {
			const end = sql.indexOf('*/', i + 2)
			if (end < 0) return false
			i = end + 2
			continue
		}
		break
	}
	// A parenthesised subquery - (SELECT ...) or (WITH ...) - still returns rows; skip
	// the leading parens (and any whitespace between them) before deciding, or it is
	// misrouted to execute() and the rows are discarded.
	for (;;) {
		while (i < n && /\s/.test(sql.charAt(i))) i++
		if (i < n && sql.charAt(i) === '(') { i++; continue }
		break
	}
	return /^(select|with)\b/i.test(sql.slice(i))
}

export class OraAdapter implements IDbAdapter {
	constructor(private readonly p: TConnOra) {
		assertTimeoutMs('ora: connectTimeout', p.connectTimeout)
	}

	versionQuery(): string { return 'SELECT banner AS ver FROM v$version WHERE rownum = 1' }
	// v$version is hidden from users without SELECT_CATALOG_ROLE; this view is public.
	versionFallbackQuery(): string { return 'SELECT version AS ver FROM PRODUCT_COMPONENT_VERSION WHERE rownum = 1' }
	pingQuery(): string { return 'SELECT 1 AS p FROM dual' }

	private _connParams() {
		const privilegeMap: Record<string, number> = {
			SYSDBA:    oracledb.SYSDBA,
			SYSOPER:   oracledb.SYSOPER,
			SYSASM:    oracledb.SYSASM,
			SYSBACKUP: oracledb.SYSBACKUP,
			SYSDG:     oracledb.SYSDG,
			SYSKM:     oracledb.SYSKM,
			SYSRAC:    oracledb.SYSRAC,
		}
		return {
			connectString: `${this.p.host}:${this.p.port ?? 1521}/${this.p.service}`,
			user:      this.p.user,
			password:  this.p.password,
			...(this.p.privilege && { privilege: privilegeMap[this.p.privilege] }),
			...(this.p.connectTimeout ? { connectTimeout: Math.ceil(this.p.connectTimeout / 1000) } : {}),
		}
	}

	private _pool?: Promise<oracledb.Pool>
	private pool(): Promise<oracledb.Pool> {
		if (!this._pool) {
			const creating = (async () => {
				// Privileged connections (SYSDBA & co) are only supported by the pool in
				// Thin mode; a Thick-mode pool silently drops the privilege. Fail loudly
				// instead of handing back a connection that is not actually privileged.
				if (this.p.privilege && !oracledb.thin) {
					throw new Error('ora: privileged connections need Thin mode; a Thick-mode pool cannot be privileged')
				}
				return oracledb.createPool({
					...this._connParams(),
					poolMin:       this.p.poolMin ?? 0,
					poolMax:       this.p.poolMax ?? 4,
					poolIncrement: this.p.poolIncrement ?? 1,
					...(this.p.queueTimeout !== undefined ? { queueTimeout: this.p.queueTimeout } : {}),
				})
			})()
			this._pool = creating
			// A transient createPool failure (network down, misconfiguration, the
			// Thick-mode check above) must not be cached forever: clear the promise so
			// the next call retries instead of replaying the same stale rejection until
			// close(). Mirrors DbConnection._adapter's non-caching failure. Only clear it
			// while the field still points at this attempt: a close() during the
			// createPool's flight resets the field and a new pool may be created since, so
			// clearing then would drop the new pool's reference and leak it.
			creating.then(
				() => {},
				() => {
					if (this._pool === creating) this._pool = undefined
				},
			)
		}
		return this._pool
	}

	async close(): Promise<void> {
		// The pool is created lazily behind a promise; close() must wait for an in-flight
		// createPool rather than leaving the eventual pool to leak, and reset the field so
		// a later call creates a fresh one.
		const inFlight = this._pool
		this._pool = undefined
		if (inFlight) {
			try { await (await inFlight).close(0) } catch {}
		}
	}

	async execStream(query: string, params: TDbParam[], callback: (event: TDbStreamEvent) => void, signal?: AbortSignal): Promise<void> {
		// Rewrite and validate the query before touching the pool: a "param not found" (or
		// any other build error) must never hold a pool slot, matching fb validating before
		// its attach. Like pg and fb, a build failure ends the call with a finish event
		// rather than a rejection.
		let sql: string
		let binds: Record<string, OraBindValue>
		try {
			({ sql, binds } = buildQuery(query, params))
		} catch (err) {
			callback({ kind: 'finish', error: toDbError(err, 'query') })
			return
		}
		const rowQuery = isRowQuery(sql)
		let conn: oracledb.Connection
		try {
			conn = await (await this.pool()).getConnection()
		} catch (err) {
			callback({ kind: 'finish', error: toDbError(err, 'connect') })
			return
		}
		// break() interrupts the statement on the server; close() must wait for it, or the
		// driver answers NJS-032 and the connection never returns to the pool. The interrupt
		// promise is remembered here so the finally can await it before closing.
		let interrupt: Promise<void> | undefined
		try {
			await new Promise<void>((resolve) => {
				let done = false
				let stream: oracledb.QueryStream<any> | undefined
				const finish = (error?: TDbError): void => {
					if (done) return
					done = true
					signal?.removeEventListener('abort', onAbort)
					callback({ kind: 'finish', error })
					resolve()
				}
				// break() interrupts the statement running on the server, whether it is a
				// query being streamed or a long DML/PL-SQL call; the stream (if any) is
				// destroyed in case the server does not answer even that. The DML branch used
				// to skip this, so a slow UPDATE/PL-SQL block kept its connection past the
				// abort until it finished on its own.
				const onAbort = (): void => {
					interrupt = conn.break().catch(() => {})
					stream?.destroy()
					finish(abortError(signal?.reason))
				}
				// getConnection() may have resolved after the deadline already fired (the
				// listener above was never registered in time). Nothing is executing on this
				// connection yet, so there is nothing to break - finish without break(); the
				// finally below closes the connection back to the pool. break() on an idle
				// connection leaves the session cancelled and its next statement answering
				// ORA-01013 (same rule as execTxDynamic's early-abort branch).
				if (signal?.aborted) {
					finish(abortError(signal?.reason))
					return
				}
				signal?.addEventListener('abort', onAbort)

				// queryStream only runs a query that returns rows; DML and PL/SQL have to go
				// through execute(), which commits right away - execStream is not a transaction.
				if (!rowQuery) {
					// resultSet:true makes res.implicitResults an array of ResultSets - one per
					// DBMS_SQL.RETURN_RESULT - each carrying its own metadata; a plain DML is
					// unaffected and still reports rowsAffected.
					conn.execute<Record<string, unknown>>(sql, binds, { autoCommit: true, outFormat: oracledb.OUT_FORMAT_OBJECT, resultSet: true })
						.then(async (res) => {
							// A PL/SQL block with DBMS_SQL.RETURN_RESULT returns its result
							// sets here; emit each as its own schema+rows so exec() reports
							// one result per result set instead of dropping the rows.
							const implicit = await readImplicitResults(res)
							if (done) return // aborted while the implicit rows were fetched
							if (implicit.length > 0) {
								for (const r of implicit) {
									callback({ kind: 'schema', schema: r.schema })
									for (const row of r.rows) callback({ kind: 'row', row })
								}
							} else {
								// DML or a plain block: no direct rows - one empty-schema
								// result carrying the affected count.
								callback({ kind: 'schema', schema: [] })
								if (typeof res.rowsAffected === 'number') callback({ kind: 'rowsAffected', count: res.rowsAffected })
							}
							if (res.outBinds) callback({ kind: 'outParams', params: res.outBinds as Record<string, unknown> })
							finish()
						})
						.catch((err) => finish(toDbError(err, 'query')))
					return
				}
				try {
					stream = conn.queryStream(sql, binds, { outFormat: oracledb.OUT_FORMAT_OBJECT })
				} catch (err) {
					finish(toDbError(err, 'query'))
					return
				}
				stream.on('metadata', (meta: OraMetadata[]) => {
					if (done) return
					callback({ kind: 'schema', schema: toSchema(meta) })
				})
				stream.on('data', (row: object) => {
					if (done) return
					callback({ kind: 'row', row: row as Record<string, unknown> })
				})
				stream.on('error', (err: Error) => finish(toDbError(err, 'query')))
				// node-oracledb requires waiting for the stream's 'close' (after 'end') before
				// closing the connection, otherwise close() races the in-flight statement and
				// throws NJS-032. finish() resolves the interrupt awaited in finally, so doing
				// it on 'close' guarantees close() runs after the driver released the slot.
				stream.on('close', () => finish())
		})
	} finally {
		// Wait for the interrupt before closing, so close() does not race the in-flight
		// statement and get NJS-032 (which would drop the connection out of the pool).
		if (interrupt) { try { await interrupt } catch {} }
		// close() can itself throw (connection in use after a stream failure); drop it
		// rather than leak the pool slot silently.
		try { await conn.close() } catch { try { await conn.close({ drop: true }) } catch {} }
	}
}

	async execTxDynamic<T = void>(cb: TDbTxCb<T>, timeoutMs?: number, external?: AbortSignal): Promise<TResult<T, TDbError>> {
		return withTxTimeout(timeoutMs, async (signal, clearDeadline) => {
			let conn: oracledb.Connection
			try {
				conn = await (await this.pool()).getConnection()
			} catch (err) {
				return { ok: false, error: toDbError(err, 'connect') }
			}
			// break() interrupts the statement running on the server, close() releases the
			// connection. close() must wait for break() to land, or the driver answers
			// NJS-032 and the connection never returns to the pool (poolMax is small enough
			// to exhaust). The interrupt promise is remembered for both the abort handler and
			// the early-abort branch, and a close() that still fails drops the connection.
			let interrupt: Promise<void> | undefined
			const release = async (): Promise<void> => {
				if (interrupt) { try { await interrupt } catch {} }
				// close() blocks until the server-side cancel of the interrupted statement
				// has fully landed, so the connection returns to the pool clean (a broken
				// cancel would answer ORA-01013 to its next statement). If close() still
				// fails, drop the connection instead of leaking it.
				try { await conn.close() } catch {
					try { await conn.close({ drop: true }) } catch {}
				}
			}
			const teardown = (): void => {
				interrupt = conn.break().catch(() => {})
				void release()
			}
			signal.addEventListener('abort', teardown, { once: true })
			// getConnection() may have resolved after the deadline already fired: the
			// listener above was added too late to ever run, and the finally below skips
			// close() on an aborted signal. Release the idle connection here, awaiting the
			// close so the pool slot is returned before the timeout is reported.
			if (signal.aborted) {
				// getConnection() resolved after the deadline fired (the listener above
				// was added too late to run). Nothing is executing on this connection yet,
				// so there is nothing to break - just return it to the pool. break() on an
				// idle connection would leave the session cancelled and its next statement
				// answering ORA-01013.
				await release()
				return { ok: false, error: abortError(signal.reason) }
			}
			try {
				const exec = (query: string, params: TDbParam[]): Promise<TDbTxResult> => {
					let q: ReturnType<typeof buildQuery>
					try {
						q = buildQuery(query, params)
					} catch (err) {
						// buildQuery throws synchronously on a missing param; the async
						// wrapper turns it into a rejection so exec(...).catch(...) sees it,
						// and tagging it 'query' keeps it from being misreported as 'user'.
						return Promise.reject(toDbError(err, 'query'))
					}
					return conn.execute<Record<string, unknown>>(q.sql, q.binds, { autoCommit: false, outFormat: oracledb.OUT_FORMAT_OBJECT, resultSet: true })
						.then(async (res) => {
							// resultSet:true moves a SELECT's rows to res.resultSet (res.rows
							// stays undefined); read and close that ResultSet so the connection
							// stays clean. DML and PL/SQL blocks leave res.resultSet undefined.
							let rows: Record<string, unknown>[] = []
							if (res.resultSet) {
								try {
									rows = await res.resultSet.getRows()
								} finally {
									await res.resultSet.close()
								}
							} else {
								rows = (res.rows ?? []) as Record<string, unknown>[]
							}
							// A PL/SQL block with DBMS_SQL.RETURN_RESULT returns its result
							// sets in res.implicitResults; surface each as its own schema+rows.
							const implicit = await readImplicitResults(res)
							const out: TDbTxResult = {
								rows,
								rowsAffected: res.rowsAffected,
								outParams:    res.outBinds as Record<string, unknown> | undefined,
							}
							if (implicit.length > 0) out.implicitResults = implicit
							return out
						})
						.catch((err) => { throw toDbError(err, 'query') })
				}
				let result!: T
				try {
					result = await cb(exec, signal)
				} catch (err) {
					// On an abort the teardown already owns the connection (break + close in
					// flight); rolling back here would race its close() and, if the slot has
					// already been handed to the next caller, corrupt that caller's statement.
					if (!signal.aborted) { try { await conn.rollback() } catch {} }
					return { ok: false, error: isDbError(err) ? err : toDbError(err, 'user') }
				}
				try {
					// The COMMIT outcome must not be raced by the deadline: a 'timeout' here
					// would be ambiguous (the server may have committed), and a caller retrying
					// would double-apply the transaction. The external abort still applies.
					clearDeadline()
					await conn.commit()
				} catch (err) {
					// Same race as the cb path above: on an abort the teardown already owns
					// the connection (break + close in flight), so rolling back here would
					// race its close(). Only roll back a real commit failure.
					if (!signal.aborted) { try { await conn.rollback() } catch {} }
					return { ok: false, error: toDbError(err, 'tx') }
				}
				return { ok: true, result }
			} finally {
				signal.removeEventListener('abort', teardown)
				if (!signal.aborted) {
					try { await conn.close() } catch {
						try { await conn.close({ drop: true }) } catch {}
					}
				}
			}
		}, external)
	}
}
