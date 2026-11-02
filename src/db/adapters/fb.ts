import * as Firebird from 'node-firebird'
import Connection from 'node-firebird/lib/wire/connection'
import type { IDbAdapter, TDbCol, TDbError, TDbParam, TDbStreamEvent, TDbTxCb, TDbTxResult } from '../types'
import type { TConnFb } from '../connection'
import type { TResult } from '../../tresult'
import { mapPlaceholders, SQL_ATOM } from '../placeholders'
import { toDbError, withTxTimeout } from '../helpers'

// ---- monkey-patch: node-firebird@2 leaks the TCP socket on a failed attach ----
// Connection.attach() answers an attach error with doError(err, cb) and never
// disconnects, and the Firebird server does not close the connection after a
// failed op_attach either - so a failed attach (bad credentials, wrong path, DB
// down) leaves a live socket behind. A long-running process then leaks file
// descriptors up to EMFILE, and short-lived scripts/tests never exit. The driver
// does not hand the connection out on the error path, so this is patched at the
// prototype, with the same teardown dropConnection() uses. The driver version is
// pinned in package.json; see .hermes.md and re-check this patch on upgrade.
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
// good: the driver emits it from the socket handler, and without a listener the loss is
// silently swallowed instead of surfacing.
/* eslint-disable @typescript-eslint/no-explicit-any */
function watchConnection(db: Firebird.Database, onLost: (error: TDbError) => void): void {
	// The typings leave out that Database is an EventEmitter.
	const events: any = db
	const conn: any   = events.connection
	events.on('error', (err: unknown) => onLost({ message: `fb: connection lost: ${String(err)}`, kind: 'query' }))
	events.on('reconnect', () => onLost({ message: 'fb: connection lost, the driver reattached on its own', kind: 'query' }))
	conn?._socket?.on('close', () => {
		// Set by our own detach at the end of the query - a close that was asked for.
		if (conn._isDetach) return
		clearTimeout(conn._retry_connection_id)
		onLost({ message: 'fb: connection lost', kind: 'query' })
	})
}

// Drops the connection at once, without a detach the server would have to answer.
function dropConnection(db: Firebird.Database): void {
	const conn: any = (db as any).connection
	if (!conn) return
	conn._isDetach = true
	clearTimeout(conn._retry_connection_id)
	try { conn._socket?.destroy() } catch { }
}
/* eslint-enable @typescript-eslint/no-explicit-any */

export class FbAdapter implements IDbAdapter {
	constructor(private readonly p: TConnFb) {}

	// node-firebird has no connection pool; each attach is closed by its own query, so
	// nothing is held between calls to release.
	async close(): Promise<void> {}

	kind(): 'fb' { return 'fb' }
	title(): string { return `${this.p.host}:${this.p.port || 'DEFAULT'} [${this.p.database}]` }
	user(): string { return this.p.user }
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
			// Without it a text blob arrives as an async reader function instead of a
			// value, and JSON.stringify silently drops such a field from the row.
			blobAsText: true,
		}
	}

	async execStream(query: string, params: TDbParam[], callback: (event: TDbStreamEvent) => void, signal?: AbortSignal): Promise<void> {
		let sql: string
		let values: unknown[]
		try {
			const parsed = buildQuery(query, params)
			sql = parsed.sql
			values = parsed.values
		} catch (err) {
			callback({ kind: 'finish', error: toDbError(err, 'query') })
			return
		}
		const options = this._options()
		return new Promise<void>((resolve) => {
			let done = false
			let db: Firebird.Database | undefined
			const finish = (error?: TDbError): void => {
				if (done) return
				done = true
				signal?.removeEventListener('abort', onAbort)
				callback({ kind: 'finish', error })
				resolve()
			}
			const onAbort = (): void => {
				if (db) dropConnection(db)
				finish({ message: String(signal?.reason ?? 'aborted'), kind: 'aborted' })
			}
			if (signal?.aborted) { finish({ message: String(signal.reason ?? 'aborted'), kind: 'aborted' }); return }
			signal?.addEventListener('abort', onAbort)

			Firebird.attach(options, (attachErr, attached) => {
				if (attachErr) { finish(toDbError(attachErr, 'connect')); return }
				// Given up while the attach was on its way - nobody is waiting for this one.
				if (done) { dropConnection(attached); return }
				db = attached
				watchConnection(db, (error) => {
					dropConnection(attached)
					finish(error)
				})
				// Schema comes from the statement's describe, not from the first row: it is
				// there for an empty result too, and carries the real column types. The
				// streaming APIs do not hand the describe back, so it is asked for on its
				// own statement, then the rows are streamed separately.
				attached.newStatement(sql, (stmtErr, statement) => {
					if (stmtErr) {
						attached.detach()
						finish(toDbError(stmtErr, 'query'))
						return
					}
					if (done) { dropConnection(attached); return }
					callback({ kind: 'schema', schema: fbSchema((statement as { output?: FbColMeta[] }).output ?? []) })
					statement.drop(() => {})
					// Rows arrive one by one; the schema above has already been emitted, so
					// the columns no longer depend on a first row.
					attached.sequentially(
						sql,
						values,
						(row: Record<string, unknown>) => {
							if (!done) callback({ kind: 'row', row })
						},
						(seqErr?: Error) => {
							if (done) return
							attached.detach()
							finish(seqErr ? toDbError(seqErr, 'query') : undefined)
						},
					)
				})
			})
		})
	}

	async execTxDynamic(cb: TDbTxCb, timeoutMs?: number): Promise<TResult<void, TDbError>> {
		const options = this._options()
		return withTxTimeout(timeoutMs, async (signal) => {
			let db: Firebird.Database
			try {
				db = await new Promise<Firebird.Database>((resolve, reject) => {
					Firebird.attach(options, (err, d) => { if (err) reject(err); else resolve(d) })
				})
			} catch (err) {
				return { ok: false, error: toDbError(err, 'connect') }
			}
			// detach() waits for the server's answer, which a transaction the server is
			// still running will not send; drop the connection outright instead.
			const teardown = (): void => { dropConnection(db) }
			signal.addEventListener('abort', teardown, { once: true })
			// attach() may have resolved after the deadline already fired: the listener
			// above was added too late to ever run, and the finally below skips detach() on
			// an aborted signal. Drop the socket here instead of leaking it.
			if (signal.aborted) {
				teardown()
				return { ok: false, error: { message: 'TIMEOUT', kind: 'timeout' } }
			}
			let tx: Firebird.Transaction | undefined
			try {
				tx = await new Promise<Firebird.Transaction>((resolve, reject) => {
					db.transaction(Firebird.ISOLATION_READ_COMMITTED, (err, t) => { if (err) reject(err); else resolve(t) })
				})
				const transaction = tx
				const exec = (query: string, params: TDbParam[]): Promise<TDbTxResult> => {
					const { sql: sqlStr, values } = buildQuery(query, params)
					return new Promise<TDbTxResult>((resolve, reject) => {
						transaction.query(sqlStr, values, (err: Error | null, rows: Record<string, unknown>[]) => {
							if (err) reject(err)
							else resolve({ rows: rows ?? [] })
						})
					})
				}
				await cb(exec, signal)
				await new Promise<void>((resolve, reject) => {
					transaction.commit((err: Error | null) => { if (err) reject(err); else resolve() })
				})
				return { ok: true, result: undefined }
			} catch (err) {
				if (tx) {
					try { await new Promise<void>((resolve) => { tx!.rollback(() => resolve()) }) } catch {}
				}
				return { ok: false, error: toDbError(err, 'query') }
			} finally {
				signal.removeEventListener('abort', teardown)
				if (!signal.aborted) {
					try { db.detach() } catch {}
				}
			}
		})
	}
}
