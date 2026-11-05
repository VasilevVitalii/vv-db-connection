import oracledb from 'oracledb'
import type { IDbAdapter, TDbCol, TDbError, TDbParam, TDbRes, TDbStreamEvent, TDbTxCb, TDbTxResult } from '../types'
import type { TConnOra } from '../connection'
import type { TResult } from '../../tresult'
import { mapPlaceholders, SQL_ATOM_ORA } from '../placeholders'
import { toDbError, isDbError, withTxTimeout, abortError, assertTimeoutMs, assertOraPool } from '../helpers'

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

// How long close() waits for an in-flight statement/stream/fetch to settle before it
// gives up and proceeds. If close() races a still-running statement the driver answers
// NJS-032 and drops the connection out of the pool instead of returning it, so this is a
// bound on a genuinely stuck statement, not a normal wait.
const ORA_CLOSE_WAIT_FALLBACK_MS = 1000

// Await a promise that must settle before close() (an interrupted statement, a stream's
// 'close', a DML/PL-SQL fetch), bounded by ORA_CLOSE_WAIT_FALLBACK_MS so a truly stuck
// promise cannot hang connection release. After the race settles, clear the timer so a
// fast settle does not leave a 1s timer holding the event loop (short-lived scripts and
// tests would exit ~1s late).
async function boundedWait(p: Promise<unknown>): Promise<void> {
	let timer: ReturnType<typeof setTimeout> | undefined
	const fallback = new Promise<void>((res) => { timer = setTimeout(res, ORA_CLOSE_WAIT_FALLBACK_MS) })
	await Promise.race([p, fallback])
	if (timer !== undefined) clearTimeout(timer)
}

// res.implicitResults, with resultSet:true, is an array of ResultSets - one per
// DBMS_SQL.RETURN_RESULT in a PL/SQL block - each carrying its own metadata. Read and
// close every one (an unclosed ResultSet keeps its cursor open and the connection busy,
// and oracledb does not auto-close the resultSet:true form) and return one { schema, rows }
// per result set, so the caller can emit them as their own results instead of dropping
// the rows silently.
async function readImplicitResults(res: oracledb.Result<Record<string, unknown>>): Promise<TDbRes[]> {
	const out: TDbRes[] = []
	const items = res.implicitResults ?? []
	for (let i = 0; i < items.length; i++) {
		const item = items[i]
		// With resultSet:false each element would be a plain row array (no metadata);
		// the adapter always executes with resultSet:true, so skip that shape defensively.
		if (item === undefined || Array.isArray(item)) continue
		try {
			out.push({ schema: toSchema(item.metaData as unknown as OraMetadata[]), rows: await item.getRows() })
			await item.close()
		} catch (err) {
			// getRows() threw mid-way: close this and every ResultSet still open behind it,
			// or their cursors stay open and the connection stays busy. Best-effort - a
			// close() that throws must not mask the original error.
			for (let j = i; j < items.length; j++) {
				const rest = items[j]
				if (rest !== undefined && !Array.isArray(rest)) { try { await rest.close() } catch {} }
			}
			throw err
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
		assertOraPool(p)
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
			try { callback({ kind: 'finish', error: toDbError(err, 'query') }) } catch { /* the consumer's finish handler threw */ }
			return
		}
		const rowQuery = isRowQuery(sql)
		let conn: oracledb.Connection
		try {
			conn = await (await this.pool()).getConnection()
		} catch (err) {
			try { callback({ kind: 'finish', error: toDbError(err, 'connect') }) } catch { /* the consumer's finish handler threw */ }
			return
		}
		// break() interrupts the statement on the server; close() must wait for it, or the
		// driver answers NJS-032 and the connection never returns to the pool. The interrupt
		// promise is remembered here so the finally can await it before closing.
		let interrupt: Promise<void> | undefined
		let stream: oracledb.QueryStream<Record<string, unknown>> | undefined
		let streamClosed: Promise<void> | undefined
		let dmlDone: Promise<void> | undefined
		try {
			await new Promise<void>((resolve) => {
				let done = false
				const finish = (error?: TDbError): void => {
					if (done) return
					done = true
					signal?.removeEventListener('abort', onAbort)
					// A throw from the consumer's finish handler must not skip resolve()
					// (the stream contract is one finish + one resolve) nor reach the driver.
					try { callback({ kind: 'finish', error }) } catch { /* the consumer's finish handler threw */ }
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
					// unaffected and still reports rowsAffected. The handler is remembered as
					// dmlDone so the finally below can wait for its in-flight fetch before
					// close() (see the finally comment).
					dmlDone = conn.execute<Record<string, unknown>>(sql, binds, { autoCommit: true, outFormat: oracledb.OUT_FORMAT_OBJECT, resultSet: true })
						.then(async (res) => {
							// A PL/SQL block with DBMS_SQL.RETURN_RESULT returns its result
							// sets here; emit each as its own schema+rows so exec() reports
							// one result per result set instead of dropping the rows.
							const implicit = await readImplicitResults(res)
							// Aborted while the implicit rows were fetched: the caller has
							// given up, but the unread cursor (isRowQuery may have misjudged
							// a SELECT, leaving res.resultSet non-empty) must still be closed
							// best-effort, or its server cursor leaks - the same close the
							// normal path performs at the bottom of this handler.
							if (done) {
								try { await res.resultSet?.close() } catch { /* best-effort */ }
								return
							}
							// The block's own result (affected count and out-parameters)
							// comes BEFORE the implicit sets: exec() attaches outParams to
							// the current result, and if they fired after the sets they would
							// stick to the last implicit one instead of the block itself.
							const emitOwn = (): void => {
								callback({ kind: 'schema', schema: [] })
								if (typeof res.rowsAffected === 'number') callback({ kind: 'rowsAffected', count: res.rowsAffected })
								if (res.outBinds) callback({ kind: 'outParams', params: res.outBinds as Record<string, unknown> })
							}
							if (implicit.length > 0) {
								if (typeof res.rowsAffected === 'number' || res.outBinds !== undefined) emitOwn()
								for (const r of implicit) {
									callback({ kind: 'schema', schema: r.schema })
									for (const row of r.rows) callback({ kind: 'row', row })
								}
							} else {
								// DML or a plain block: no direct rows - one empty-schema
								// result carrying the affected count and out-parameters.
								emitOwn()
							}
							// If isRowQuery misjudged (a SELECT behind an unrecognized prefix)
							// res.resultSet holds an unread cursor. Close it best-effort so rows
							// are not silently dropped and the server cursor does not leak
							// (mirrors the tx path, which always closes its result set).
							try { await res.resultSet?.close() } catch { /* best-effort */ }
							finish()
						})
						.catch((err) => finish(toDbError(err, 'query')))
					return
				}
				try {
					stream = conn.queryStream(sql, binds, { outFormat: oracledb.OUT_FORMAT_OBJECT })
					// Remember when the driver releases the slot, so finally can wait for it
					// (not just break()) before close() - on abort/error the stream's 'close'
					// fires after destroy/error, and close() before it would NJS-032 and drop
					// the connection instead of returning it to the pool.
					streamClosed = new Promise<void>((res) => { stream!.once('close', () => res()) })
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
		// On abort/error the stream's 'close' fires after destroy/error; wait for it (bounded
		// so a stuck stream cannot hang release) before close(), or the driver answers
		// NJS-032 and the connection is dropped instead of pooled.
		if (streamClosed) await boundedWait(streamClosed)
		// The DML/PL-SQL path keeps fetching implicit rows (readImplicitResults) after
		// execute() settles. An abort tears the connection down while that fetch is
		// still in flight; close() before the fetch settles answers NJS-032 and drops
		// the connection out of the pool instead of returning it - the same race the
		// the row path guards against via streamClosed. Wait for the DML handler, bounded
		// like streamClosed so a truly stuck fetch cannot hang release.
		if (dmlDone) await boundedWait(dmlDone)
		// close() can itself throw (connection in use after a stream failure); drop it
		// rather than leak the pool slot silently. A drop discards the session and any
		// uncommitted work without surfacing it - an accepted trade-off (a lost session is
		// cheaper than a permanently leaked pool slot), and there is no logging channel in
		// this library to report it through.
		try { await conn.close() } catch { try { await conn.close({ drop: true }) } catch {} }
	}
}

	async execTxDynamic<T = void>(cb: TDbTxCb<T>, timeoutMs?: number, external?: AbortSignal): Promise<TResult<T, TDbError>> {
		return withTxTimeout(timeoutMs, async (signal, armCommitDeadline) => {
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
			// Operations (execute() or commit()) are in flight on this connection while
			// their promise is pending (the statement plus any result-set reads and closes).
			// break() only interrupts a statement that is actually running: issuing it on
			// an idle connection leaves the session cancelled and its next statement
			// answering ORA-01013 (the same rule the early-abort branch below follows).
			// A counter (not a boolean) so overlapping statements cannot be hidden by the
			// first one settling and clearing the flag: the abort teardown must break() as
			// long as ANY statement is still running.
			let operationsInFlight = 0
			// Every in-flight execute() promise (each a never-rejecting mirror); close()
			// must wait for all of them to settle or the driver answers NJS-032 and drops
			// the connection out of the pool instead of returning it — the same race
			// execStream guards against via dmlDone. A Set (not a single "latest" slot)
			// so a still-running earlier execute() cannot be hidden by a later one
			// settling first (review17 2.3).
			const inFlightExecs = new Set<Promise<unknown>>()
			const release = async (): Promise<void> => {
				if (interrupt) { try { await interrupt } catch {} }
				if (inFlightExecs.size > 0) await boundedWait(Promise.all([...inFlightExecs]))
				// close() blocks until the server-side cancel of the interrupted statement
				// has fully landed, so the connection returns to the pool clean (a broken
				// cancel would answer ORA-01013 to its next statement). If close() still
				// fails, drop the connection instead of leaking it.
				try { await conn.close() } catch {
					try { await conn.close({ drop: true }) } catch {}
				}
			}
			const teardown = (): void => {
				// Only interrupt a statement that is actually running; break() on an idle
				// connection would leave the session cancelled and its next statement
				// answering ORA-01013.
				if (operationsInFlight > 0) interrupt = conn.break().catch(() => {})
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
					// ora's exec is not async, so the abort rejection is explicit.
					if (signal.aborted) return Promise.reject(abortError(signal.reason))
					let q: ReturnType<typeof buildQuery>
					try {
						q = buildQuery(query, params)
					} catch (err) {
						// buildQuery throws synchronously on a missing param; the async
						// wrapper turns it into a rejection so exec(...).catch(...) sees it,
						// and tagging it 'query' keeps it from being misreported as 'user'.
						return Promise.reject(toDbError(err, 'query'))
					}
					// A statement is in flight from execute() until the whole exec settles
					// (row reads + implicit-result reads + result-set closes), so the abort
					// teardown knows there is something to break().
					operationsInFlight += 1
					const p = conn.execute<Record<string, unknown>>(q.sql, q.binds, { autoCommit: false, outFormat: oracledb.OUT_FORMAT_OBJECT, resultSet: true })
						.then(async (res) => {
							// resultSet:true moves a SELECT's rows to res.resultSet (res.rows
							// stays undefined); read and close that ResultSet so the connection
							// stays clean. DML and PL/SQL blocks leave res.resultSet undefined.
							let rows: Record<string, unknown>[] = []
							if (res.resultSet) {
								try {
									rows = await res.resultSet.getRows()
								} finally {
									// A close() failure must not mask a getRows() error (the row
									// read is the real failure); best-effort close, as in
									// readImplicitResults.
									try { await res.resultSet.close() } catch { }
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
					// Never-rejecting mirror of this in-flight execute(): release()/finally
					// await every mirror (bounded) so close() does not race a still-running
					// statement into NJS-032. Each exec removes its own mirror on settle, so
					// a settled exec never adds a pointless boundedWait to the release path.
					const mirror = p.catch(() => {})
					inFlightExecs.add(mirror)
					const clearInFlight = (): void => {
						operationsInFlight -= 1
						inFlightExecs.delete(mirror)
					}
					p.then(clearInFlight, clearInFlight)
					return p
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
					// COMMIT is a statement on the connection too: mark it in flight so an
					// abort that lands during commit() is interrupted with break() rather than
					// leaving the session cancelled for its next statement. Its promise also
					// joins inFlightExecs (a never-rejecting mirror), so release() waits for the
					// commit to settle before close() — otherwise close() races a commit that has
					// not yet reacted to break() into NJS-032 and drops the slot (review18 2.2).
					// await the ORIGINAL promise, so a commit error still propagates to the
					// 'tx' catch below.
					operationsInFlight += 1
					const commitP = conn.commit()
					const commitMirror = commitP.catch(() => {})
					inFlightExecs.add(commitMirror)
					try {
						await commitP
					} finally {
						operationsInFlight -= 1
						inFlightExecs.delete(commitMirror)
					}
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
					// A callback that did not await its last exec (fire-and-forget) leaves the
					// execute() in flight here; wait for every one (bounded) before close(),
					// the same race execStream guards via dmlDone, or close() answers NJS-032
					// and drops the connection.
					if (inFlightExecs.size > 0) await boundedWait(Promise.all([...inFlightExecs]))
					try { await conn.close() } catch {
						try { await conn.close({ drop: true }) } catch {}
					}
				}
			}
		}, external)
	}
}
