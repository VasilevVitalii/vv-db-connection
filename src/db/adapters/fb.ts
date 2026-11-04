import { createRequire } from 'node:module'
import * as Firebird from 'node-firebird'
import Connection from 'node-firebird/lib/wire/connection'
import Const from 'node-firebird/lib/wire/const'
import { parseRecordCounts } from 'node-firebird/lib/wire/xsqlvar'
import type Transaction from 'node-firebird/lib/wire/transaction'
import type { IDbAdapter, TDbCol, TDbError, TDbParam, TDbStreamEvent, TDbTxCb, TDbTxResult } from '../types'
import type { TConnFb } from '../connection'
import type { TResult } from '../../tresult'
import { mapPlaceholders, SQL_ATOM } from '../placeholders'
import { toDbError, isDbError, withTxTimeout, abortError, assertTimeoutMs } from '../helpers'

// ---- monkey-patch: node-firebird@2 leaks the TCP socket on a failed attach ----
// Connection.attach() answers an attach error with doError(err, cb) and never
// disconnects, and the Firebird server does not close the connection after a
// failed op_attach either - so a failed attach (bad credentials, wrong path, DB
// down) leaves a live socket behind. A long-running process then leaks file
// descriptors up to EMFILE, and short-lived scripts/tests never exit. The driver
// does not hand the connection out on the error path, so this is patched at the
// prototype, with the same teardown dropConnection() uses. The driver version is
// pinned in package.json exactly because of this patch; re-check `Connection.attach`
// and the teardown fields on any driver upgrade (see the Firebird note in the README).
type FbConnInternal = {
	_isDetach?: boolean
	_retry_connection_id?: ReturnType<typeof setTimeout>
	_socket?: { destroy?: () => void } | null
}
type FbAttachFn = (
	this: FbConnInternal,
	options: unknown,
	callback?: (err: unknown, ret: unknown) => void,
	db?: unknown,
) => unknown

const fbAttachProto = Connection.prototype as unknown as { attach: FbAttachFn & { __vvPatched?: true } }
if (!fbAttachProto.attach.__vvPatched) {
	const origAttach = fbAttachProto.attach
	const patched = function (this: FbConnInternal, options: unknown, callback?: (err: unknown, ret: unknown) => void, db?: unknown): unknown {
		const self = this
		return origAttach.call(this, options, function (err: unknown, ret: unknown): void {
			if (err) {
				// Mark the close as wanted (skips the driver's self-reconnect) and
				// rip the socket so the descriptor is released at once.
				try {
					self._isDetach = true
					clearTimeout(self._retry_connection_id)
					self._socket?.destroy?.()
				} catch { /* cleanup must never mask the attach error */ }
			}
			if (callback) callback(err, ret)
		}, db)
	} as FbAttachFn & { __vvPatched?: true }
	patched.__vvPatched = true
	fbAttachProto.attach = patched
}

// node-firebird assigns the in-flight Connection to `exports.connection` inside
// attach(); an `import * as` namespace is a snapshot of the CJS exports and does
// not see that late assignment, so reach it through a live require instead. attach()
// is reached the same way so tests can substitute it and the connection race is
// observable; it is the very same function as Firebird.attach at require time.
const fbLive = createRequire(import.meta.url)('node-firebird') as {
	connection?: FbConnInternal
	attach: typeof Firebird.attach
}


// Firebird reports each column's type as a numeric SQL code; map it back to the
// driver's type name ('VARYING', 'INT64', ...) for TDbCol.kind - the same names the
// driver's own withMeta fields deliver. SQL_TYPES is the public name -> code table.
const FB_TYPE_NAME: Record<number, string> = {}
for (const [key, code] of Object.entries(Firebird.SQL_TYPES)) {
	FB_TYPE_NAME[code] = key.replace(/^SQL_(TYPE_)?/, '')
}

// The describe metadata of one output column, as node-firebird exposes it on
// statement.output after prepare.
type FbColMeta = {
	type: number
	subType?: number
	scale?: number
	length?: number
	nativeLength?: number
	field?: string
	alias?: string
}

function fbSchema(output: FbColMeta[]): TDbCol[] {
	return output.map((m) => {
		const col: TDbCol = {
			name: m.alias ?? m.field ?? '',
			kind: FB_TYPE_NAME[m.type] ?? String(m.type),
		}
		const len = m.nativeLength ?? m.length
		if (len !== undefined) col.len = len
		if (m.scale !== undefined) col.scale = m.scale
		return col
	})
}


// Index of the AS that closes the header of an EXECUTE BLOCK, or -1. Both lists the
// header may carry - the input parameters and RETURNS - are parenthesized, so the AS
// we want is the first one outside any parentheses, and outside literals and comments.
function execBlockBodyAt(query: string): number {
	if (!/^\s*EXECUTE\s+BLOCK\b/i.test(query)) return -1
	const scan = new RegExp(`${SQL_ATOM.source}|[()]|\\bAS\\b`, 'gi')
	let depth  = 0
	for (let m = scan.exec(query); m !== null; m = scan.exec(query)) {
		const token = m[0]
		if (token === '(')                  { depth++; continue }
		if (token === ')')                  { depth--; continue }
		if (depth === 0 && /^as$/i.test(token)) return m.index
	}
	return -1
}

// Converts :name placeholders to ? (positional) and collects values in order.
// For EXECUTE BLOCK statements only the header is processed - past the AS begins PSQL,
// where :name is a reference to a local variable or to a parameter of the block itself
// and must be left alone. That AS is not always followed by BEGIN: declarations of local
// variables sit between the two, and a header that ends with "as declare variable x"
// used to be missed, which turned every variable reference in the body into a parameter
// nobody had passed and failed the query with "param not found".
function buildQuery(query: string, params: TDbParam[]): { sql: string; values: unknown[] } {
	const values: unknown[] = []
	const bodyAt = execBlockBodyAt(query)
	const sql = mapPlaceholders(query, (name) => {
		const found = params.find((p) => p.name === name)
		if (!found) throw new Error(`fb: param not found: ${name}`)
		if (found.direction !== undefined && found.direction !== 'in') throw new Error(`fb: ${found.direction} parameters are not supported`)
		values.push(found.value)
		return '?'
	}, bodyAt < 0 ? undefined : bodyAt)
	return { sql, values }
}

// A lost connection must end the query with an error. node-firebird 2 rejects the
// pending operations on a closed socket (so a running query does end), but then still
// schedules a self-reconnect a second later - a fresh attach that nobody asked for and
// nobody tracks, on which the statement handles of the dropped connection are never
// valid. Here the close is seen right after the driver has scheduled that reconnect,
// the timer is cancelled, and the query ends with an error. The "error" and "reconnect"
// events of the database are taken as the same loss. The "error" listener stays for
// good while the query runs: the driver emits it from the socket handler, and without a
// listener the loss is silently swallowed instead of surfacing. The returned cleanup
// detaches all three listeners once the query is done. Today each attach is closed by
// its own query, so the db (and its listeners) is simply garbage-collected, but removing
// them explicitly keeps a future pooled Firebird from accumulating one set per query and
// tripping MaxListeners.
/* eslint-disable @typescript-eslint/no-explicit-any */
function watchConnection(db: Firebird.Database, onLost: (error: TDbError) => void): () => void {
	// The typings leave out that Database is an EventEmitter.
	const events: any = db
	const conn: any   = events.connection
	// A `code` tags these as real database errors: a bare {message, kind} literal would
	// fail isDbError() (no code/stack) and be re-labelled 'user', which breaks the retry
	// contract (query errors are retried, user errors are not).
	const onError = (err: unknown): void => onLost({ message: `fb: connection lost: ${String(err)}`, kind: 'query', code: 'FB_CONN_LOST' })
	const onReconnect = (): void => onLost({ message: 'fb: connection lost, the driver reattached on its own', kind: 'query', code: 'FB_CONN_LOST' })
	const onClose = (): void => {
		// Set by our own detach at the end of the query - a close that was asked for.
		if (conn._isDetach) return
		clearTimeout(conn._retry_connection_id)
		onLost({ message: 'fb: connection lost', kind: 'query', code: 'FB_CONN_LOST' })
	}
	events.on('error', onError)
	events.on('reconnect', onReconnect)
	conn?._socket?.on('close', onClose)
	return (): void => {
		events.removeListener('error', onError)
		events.removeListener('reconnect', onReconnect)
		conn?._socket?.removeListener('close', onClose)
	}
}

// Drops the connection at once, without a detach the server would have to answer.
// A real detach() sends op_detach and waits for the reply, which hangs exactly like
// attach() when the peer is gone (dropped SYN / dead server); ripping the socket ends
// it immediately. The server sees the socket close and frees the attachment itself.
function dropConnection(db: Firebird.Database): void {
	const conn: any = (db as any).connection
	if (!conn) return
	conn._isDetach = true
	clearTimeout(conn._retry_connection_id)
	try { conn._socket?.destroy() } catch { }
}
/* eslint-enable @typescript-eslint/no-explicit-any */

export class FbAdapter implements IDbAdapter {
	constructor(private readonly p: TConnFb) {
		assertTimeoutMs('fb: connectTimeout', p.connectTimeout)
	}

	// node-firebird has no connection pool; each attach is closed by its own query, so
	// nothing is held between calls to release.
	async close(): Promise<void> {}

	versionQuery(): string { return "SELECT rdb$get_context('SYSTEM', 'ENGINE_VERSION') AS ver FROM rdb$database" }
	pingQuery(): string { return 'SELECT 1 AS p FROM rdb$database' }

	private _options(): Firebird.Options {
		return {
			host:     this.p.host,
			port:     this.p.port ?? 3050,
			database: this.p.database,
			user:     this.p.user,
			password: this.p.password,
			encoding: (this.p.charset ?? 'UTF8') as Firebird.SupportedCharacterSet,
			numericMode: this.p.numericMode,
			// Without it a text blob arrives as an async reader function instead of a
			// value, and JSON.stringify silently drops such a field from the row.
			blobAsText: true,
		}
	}

	// Attaches with an optional timeout. node-firebird keeps the in-flight connection on
	// exports.connection and ripping that socket is the only way to abort a TCP connect /
	// handshake that is still stuck; without it a firewalled host (SYN dropped rather than
	// refused) hangs the attach for the OS's TCP timeout. Because that handle is a single
	// global, each attach captures its own connection right after calling attach(), before
	// a concurrent attach can overwrite it - the timeout then tears that socket, not
	// whichever connection a later attach happened to create. The same teardown is shared
	// with an external abort signal so an abort() during a stuck attach also releases the
	// descriptor instead of leaving it until the OS TCP timeout.
	private attach(options: Firebird.Options, timeoutMs?: number, signal?: AbortSignal): Promise<Firebird.Database> {
		return new Promise((resolve, reject) => {
			let settled = false
			let timer: ReturnType<typeof setTimeout> | undefined
			let conn: FbConnInternal | undefined
			const done = <T>(fn: (v: T) => void) => (v: T): void => {
				if (settled) return
				settled = true
				if (timer) clearTimeout(timer)
				signal?.removeEventListener('abort', onAbort)
				fn(v)
			}
			// Tear the in-flight Connection down the same way a failed attach does
			// (_isDetach skips the driver's self-reconnect; the socket is not yet opened
			// while a TCP connect is stuck, so destroy() exits cleanly).
			const tear = (): void => {
				if (conn) {
					try {
						conn._isDetach = true
						clearTimeout(conn._retry_connection_id)
						conn._socket?.destroy?.()
					} catch { /* best effort */ }
				}
			}
			const onAbort = (): void => {
				if (settled) return
				// Reject with the full TDbError, not a bare Error: kind 'aborted' and code
				// 'ABORTED' must survive so a future reuse of this attach does not misreport
				// the abort as a generic connect failure.
				done(reject)(abortError(signal?.reason))
				tear()
			}
			fbLive.attach(options, (err, db) => {
				if (err) done(reject)(err)
				// Given up while the attach was on its way: the connection that just came
				// back has no one waiting for it - drop it instead of leaking the socket.
				else if (settled) dropConnection(db)
				else done(resolve)(db)
			})
			// Capture the Connection this attach just created (node-firebird assigns it to
			// exports.connection synchronously inside attach()) before the timer is armed.
			conn = fbLive.connection
			// Register only if the attach did not already settle synchronously: when it
			// did, done() has removed the listener and re-adding it here would leak a
			// listener on a long-lived signal forever.
			if (signal && !settled) {
				signal.addEventListener('abort', onAbort)
				if (signal.aborted) onAbort()
			}
			if (timeoutMs) {
				timer = setTimeout(() => {
					// If the attach already settled there is nothing left to abort.
					if (settled) return
					done(reject)(new Error(`fb: connect timeout after ${timeoutMs}ms`))
					tear()
				}, timeoutMs)
			}
		})
	}

	async execStream(query: string, params: TDbParam[], callback: (event: TDbStreamEvent) => void, signal?: AbortSignal): Promise<void> {
		let sql: string
		let values: unknown[]
		try {
			const parsed = buildQuery(query, params)
			sql = parsed.sql
			values = parsed.values
		} catch (err) {
			try { callback({ kind: 'finish', error: toDbError(err, 'query') }) } catch { /* the consumer's finish handler threw */ }
			return
		}
		const options = this._options()
		return new Promise<void>((resolve) => {
			let done = false
			let db: Firebird.Database | undefined
			let unwatch: (() => void) | undefined
			const finish = (error?: TDbError): void => {
				if (done) return
				done = true
				// Detach the connection-loss listeners now that the query is settled -
				// they are no longer needed and must not outlive the attach.
				unwatch?.()
				signal?.removeEventListener('abort', onAbort)
				// A throw from the consumer's finish handler must not skip resolve()
				// (the stream contract is one finish + one resolve) nor reach the driver.
				try { callback({ kind: 'finish', error }) } catch { /* the consumer's finish handler threw */ }
				resolve()
			}
			const onAbort = (): void => {
				if (db) dropConnection(db)
				finish(abortError(signal?.reason))
			}
			if (signal?.aborted) { finish(abortError(signal.reason)); return }
			signal?.addEventListener('abort', onAbort)

			this.attach(options, this.p.connectTimeout, signal).then(
				(attached) => {
					// Given up while the attach was on its way - nobody is waiting for this one.
					if (done) { dropConnection(attached); return }
					db = attached
					unwatch = watchConnection(db, (error) => {
						dropConnection(attached)
						finish(error)
					})
					// One prepared statement serves the schema, the streamed rows and the
					// affected-row count. The high-level sequentially() re-prepares the
					// statement internally, so this drives the low-level Transaction/Statement
					// API directly.
					void this._execStream(attached, sql, values, (event) => { if (!done) callback(event) }).then(
						() => { dropConnection(attached); finish() },
						(err) => { dropConnection(attached); finish(err) },
					)
				},
				(attachErr) => finish(toDbError(attachErr, 'connect')),
			)
		})
	}

	/**
	 * Streams one query on a freshly attached database over the low-level
	 * Transaction/Statement API. The statement is prepared exactly once and its
	 * describe supplies the schema, so an empty result still carries columns.
	 *
	 * SELECT/WITH stream rows one fetch at a time; a statement without output
	 * columns is DML/DDL, and DML reports the affected-row count via
	 * isc_info_sql_records. EXECUTE PROCEDURE with a RETURNS clause delivers its
	 * single row on execute (no cursor).
	 */
	private async _execStream(db: Firebird.Database, sql: string, values: unknown[], callback: (event: TDbStreamEvent) => void): Promise<void> {
		const conn = (db as unknown as { connection: Connection }).connection
		let tx: Transaction | undefined
		try {
			// A transaction is mandatory (Firebird has no autocommit), but the driver
			// only exposes the low-level Transaction object through the Connection; the
			// high-level Database.transaction() returns the public facade that hides
			// output/type/statementInfo this method needs.
			const transaction: Transaction = await new Promise((resolve, reject) => {
				conn.startTransaction(Const.ISOLATION_READ_COMMITTED, (err: unknown, t: Transaction) => (err ? reject(err) : resolve(t)))
			})
			tx = transaction
			const statement = await transaction.newStatementAsync(sql)
			try {
				const output = statement.output ?? []
				const hasOutput = output.length > 0
				callback({ kind: 'schema', schema: hasOutput ? fbSchema(output) : [] })
				const ret = (await statement.executeAsync(transaction, values, { asObject: true })) as { data?: unknown[] } | undefined
				if (hasOutput) {
					// EXECUTE PROCEDURE with RETURNS delivers its single row on execute;
					// a cursor (SELECT/WITH) streams the rows one fetch at a time.
					if (statement.type === Const.isc_info_sql_stmt_exec_procedure && ret?.data && ret.data.length > 0) {
						await new Promise<void>((resolve, reject) => statement.connection.resolveTextBlobs(transaction, ret, (err?: unknown) => (err ? reject(err) : resolve())))
						for (const row of ret.data) callback({ kind: 'row', row: row as Record<string, unknown> })
					} else {
						for (;;) {
							const fret = (await statement.fetchAsync(transaction, Const.DEFAULT_FETCHSIZE)) as { data?: unknown[]; fetched?: boolean }
							const rows = fret?.data ?? []
							if (rows.length > 0) {
								await new Promise<void>((resolve, reject) => statement.connection.resolveTextBlobs(transaction, fret, (err?: unknown) => (err ? reject(err) : resolve())))
								for (const row of rows) callback({ kind: 'row', row: row as Record<string, unknown> })
							}
							// fetched === true means the cursor is exhausted (fetch status 100).
							if (fret?.fetched) break
						}
					}
				} else {
					// No output columns: DML/DDL. Only the data-changing verbs report a
					// count; DDL (CREATE/ALTER/DROP) has no affected-row number.
					const t = statement.type
					if (t === Const.isc_info_sql_stmt_insert || t === Const.isc_info_sql_stmt_update || t === Const.isc_info_sql_stmt_delete || t === Const.isc_info_sql_stmt_exec_procedure) {
						const info = await new Promise<{ buffer?: Buffer }>((resolve, reject) => {
							statement.connection.statementInfo(statement, Const.RECORDS_INFO, (err?: unknown, r?: { buffer?: Buffer }) => (err ? reject(err) : resolve(r ?? {})))
						})
						const counts = parseRecordCounts(info.buffer)
						callback({ kind: 'rowsAffected', count: counts.insertCount + counts.updateCount + counts.deleteCount })
					}
				}
				await statement.releaseAsync()
				await transaction.commitAsync()
			} catch (err) {
				try { await statement.releaseAsync() } catch { /* connection is gone; nothing left to release */ }
				throw err
			}
		} catch (err) {
			if (tx) { try { await tx.rollbackAsync() } catch { /* connection is gone */ } }
			throw toDbError(err, 'query')
		}
	}

	async execTxDynamic<T = void>(cb: TDbTxCb<T>, timeoutMs?: number, external?: AbortSignal): Promise<TResult<T, TDbError>> {
		const options = this._options()
		return withTxTimeout(timeoutMs, async (signal, armCommitDeadline) => {
			let db: Firebird.Database
			try {
				db = await this.attach(options, this.p.connectTimeout, signal)
			} catch (err) {
				return { ok: false, error: toDbError(err, 'connect') }
			}
			// detach() waits for the server's answer, which a transaction the server is
			// still running will not send; drop the connection outright instead.
			const teardown = (): void => { dropConnection(db) }
			signal.addEventListener('abort', teardown, { once: true })
			// Watch for the driver's self-reconnect exactly as execStream does: without it,
			// a connection dropped mid-transaction schedules a fresh attach nobody tracks,
			// on which the dead statement handles are never valid. Dropping the socket here
			// cancels that reconnect; the next statement / commit then fails loudly instead
			// of reattaching in the background.
			let connLost: TDbError | undefined
			const unwatch = watchConnection(db, (error) => { connLost = error; dropConnection(db) })
			// attach() may have resolved after the deadline already fired: the listener
			// above was added too late to ever run, and the finally below skips detach() on
			// an aborted signal. Drop the socket here instead of leaking it.
			if (signal.aborted) {
				unwatch()
				teardown()
				return { ok: false, error: abortError(signal.reason) }
			}
			let tx: Firebird.Transaction | undefined
			try {
				tx = await new Promise<Firebird.Transaction>((resolve, reject) => {
					db.transaction(Firebird.ISOLATION_READ_COMMITTED, (err, t) => { if (err) reject(err); else resolve(t) })
				})
				const transaction = tx
				const exec = async (query: string, params: TDbParam[]): Promise<TDbTxResult> => {
					if (signal.aborted) throw abortError(signal.reason)
					// A connection already lost has no valid statement handle: fail now
					// rather than let queryAsync reattach in the background.
					if (connLost) throw connLost
					let q: ReturnType<typeof buildQuery>
					try {
						q = buildQuery(query, params)
					} catch (err) {
						// buildQuery throws synchronously on a missing param; the async
						// wrapper turns it into a rejection so exec(...).catch(...) sees it,
						// and tagging it 'query' keeps it from being misreported as 'user'.
						throw toDbError(err, 'query')
					}
					try {
						// withMeta requests the per-verb DML counts (one lightweight
						// statementInfo round-trip) so a DML statement reports rowsAffected
						// exactly as execStream does; recordCounts is set only for DML, so a
						// SELECT still leaves rowsAffected unset (the "DML only" contract).
						const res = await transaction.queryAsync<Record<string, unknown>>(q.sql, q.values, { withMeta: true })
						return {
							rows: res.rows == null ? [] : (Array.isArray(res.rows) ? res.rows : [res.rows]),
							...(res.recordCounts ? { rowsAffected: res.affectedRows } : {}),
						}
					} catch (err) {
						// Tag the failure as a DB query error so it stays 'query' even
						// when it bubbles through the user's callback.
						throw toDbError(err, 'query')
					}
				}
				let result!: T
				try {
					result = await cb(exec, signal)
				} catch (err) {
					if (!signal.aborted) { try { await new Promise<void>((resolve) => { transaction.rollback(() => resolve()) }) } catch {} }
					return { ok: false, error: isDbError(err) ? err : toDbError(err, 'user') }
				}
				if (connLost) {
					// The connection died before COMMIT; there is nothing to roll back and
					// the commit cannot be sent. Report the loss instead of sending COMMIT
					// over a dead socket.
					return { ok: false, error: connLost }
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
					await new Promise<void>((resolve, reject) => {
						transaction.commit((err: Error | null) => { if (err) reject(err); else resolve() })
					})
				} catch (err) {
					if (!signal.aborted) { try { await new Promise<void>((resolve) => { transaction.rollback(() => resolve()) }) } catch {} }
					return { ok: false, error: toDbError(err, 'tx') }
				}
				return { ok: true, result }
			} catch (err) {
				// db.transaction() failing is the only thing that reaches here now.
				if (tx && !signal.aborted) {
					try { await new Promise<void>((resolve) => { tx!.rollback(() => resolve()) }) } catch {}
				}
				return { ok: false, error: toDbError(err, 'tx') }
			} finally {
				unwatch()
				signal.removeEventListener('abort', teardown)
				if (!signal.aborted) {
					// detach() waits for the server's answer and would hang on a server that
					// is still running the (now rolled back) work; drop the socket outright.
					dropConnection(db)
				}
			}
		}, external)
	}
}
