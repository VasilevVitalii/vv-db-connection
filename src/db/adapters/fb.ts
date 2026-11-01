import Firebird from 'node-firebird'
import { createRequire } from 'module'
import type { IDbAdapter, TDbCol, TDbParam, TDbStreamEvent, TDbTxCb, TDbTxResult } from '../types'
import type { TConnFb } from '../connection'
import type { TResult } from '../../tresult'

// Windows-1251 → Unicode code point map for bytes 0x80–0xFF
const WIN1251: readonly number[] = [
	0x0402, 0x0403, 0x201A, 0x0453, 0x201E, 0x2026, 0x2020, 0x2021, // 0x80-0x87
	0x20AC, 0x2030, 0x0409, 0x2039, 0x040A, 0x040C, 0x040B, 0x040F, // 0x88-0x8F
	0x0452, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014, // 0x90-0x97
	0xFFFD, 0x2122, 0x0459, 0x203A, 0x045A, 0x045C, 0x045B, 0x045F, // 0x98-0x9F
	0x00A0, 0x040E, 0x045E, 0x0408, 0x00A4, 0x0490, 0x00A6, 0x00A7, // 0xA0-0xA7
	0x0401, 0x00A9, 0x0404, 0x00AB, 0x00AC, 0x00AD, 0x00AE, 0x0407, // 0xA8-0xAF
	0x00B0, 0x00B1, 0x0406, 0x0456, 0x0491, 0x00B5, 0x00B6, 0x00B7, // 0xB0-0xB7
	0x0451, 0x2116, 0x0454, 0x00BB, 0x0458, 0x0405, 0x0455, 0x0457, // 0xB8-0xBF
	0x0410, 0x0411, 0x0412, 0x0413, 0x0414, 0x0415, 0x0416, 0x0417, // 0xC0-0xC7
	0x0418, 0x0419, 0x041A, 0x041B, 0x041C, 0x041D, 0x041E, 0x041F, // 0xC8-0xCF
	0x0420, 0x0421, 0x0422, 0x0423, 0x0424, 0x0425, 0x0426, 0x0427, // 0xD0-0xD7
	0x0428, 0x0429, 0x042A, 0x042B, 0x042C, 0x042D, 0x042E, 0x042F, // 0xD8-0xDF
	0x0430, 0x0431, 0x0432, 0x0433, 0x0434, 0x0435, 0x0436, 0x0437, // 0xE0-0xE7
	0x0438, 0x0439, 0x043A, 0x043B, 0x043C, 0x043D, 0x043E, 0x043F, // 0xE8-0xEF
	0x0440, 0x0441, 0x0442, 0x0443, 0x0444, 0x0445, 0x0446, 0x0447, // 0xF0-0xF7
	0x0448, 0x0449, 0x044A, 0x044B, 0x044C, 0x044D, 0x044E, 0x044F, // 0xF8-0xFF
]

function win1251Decode(buf: Buffer): string {
	let s = ''
	for (let i = 0; i < buf.length; i++) {
		// eslint-disable-next-line @typescript-eslint/no-non-null-assertion
		const b = buf[i]!
		s += String.fromCodePoint(b < 0x80 ? b : WIN1251[b - 0x80]!)
	}
	return s
}

// Module-level charset state — set once per adapter init, used by XdrReader patch below
let fbDecodeCharset: 'UTF8' | 'WIN1251' = 'UTF8'

// node-firebird bug with Firebird 6 (protocol >= V11): Connection.prototype.closeBlob
// uses _queueEvent(callback, defer=true), which calls the callback immediately without
// pushing to the response queue. The server still sends op_response for op_close_blob,
// leaving an unconsumed response in the XDR stream. This shifts all subsequent response
// reads by one: allocateAndPrepareStatement's lazy_count=2 consumes the close_blob
// response + allocate response instead of allocate + prepare, so ret.buffer is undefined
// and describe() crashes with "Cannot read properties of undefined (reading 'length')".
// Fix: override closeBlob to queue the response so it is consumed before the next op.
//
// XdrReader.readText patch: node-firebird hardcodes DEFAULT_ENCODING='UTF8' for all text
// columns, ignoring the column charset. Firebird databases with NONE-charset columns send
// raw bytes (e.g. WIN1251). We intercept readText to decode with the correct charset
// instead of blindly calling buffer.toString('utf8'), which produces U+FFFD for WIN1251.
//
// Connection.fetchAll patch: with blobAsText the driver reads every text blob of a
// fetch batch (200 rows) at once - Promise.all over arrBlob - so up to 200 sequences of
// open_blob / get_segment... / close_blob are interleaved on one connection. The driver
// is not built for that: one FIFO of response callbacks and one shared write buffer for
// all of them. Responses start going missing, the fetch loop is never called again, and
// the read hangs for good with no error at all - the export just stops after the first
// batches and the job never ends. Here the blobs of a batch are read one after another,
// which is not only sound but far faster (3000 rows with a 16 KB blob: hangs vs 3.7 s).
// Rows are also no longer accumulated in a growing array in stream mode - execStream
// throws that array away, and on a large export it is a copy of the whole data set,
// blobs included, held in memory next to the file being written.
//
// BlrReader.readSegment patch: text blobs never reach readText - node-firebird reads them
// segment by segment and decodes every segment with the same hardcoded UTF8. Here we
// cannot change the decoding, so we re-encode WIN1251 bytes into UTF-8 and let that
// hardcoded decode produce the right text. readSegment is used for blobs only; on a
// WIN1251 connection a binary blob would be re-encoded too, but those are not read - they
// come as reader functions and are dropped before they reach the data files.
/* eslint-disable @typescript-eslint/no-explicit-any */
;((): void => {
	const _require        = createRequire(import.meta.url)
	const FbWireConn: any = _require('node-firebird/lib/wire/connection')
	const FbConst: any    = _require('node-firebird/lib/wire/const')
	FbWireConn.prototype.closeBlob = function (blob: any, callback: any): void {
		const msg: any = this._msg
		msg.pos = 0
		msg.addInt(FbConst.op_close_blob)
		msg.addInt(blob.handle)
		this._queueEvent(callback) // no defer — op_close_blob response must be consumed
	}

	const FbSerialize: any = _require('node-firebird/lib/wire/serialize')
	FbSerialize.XdrReader.prototype.readText = function (len: number, _encoding: string): string {
		if (len <= 0) return ''
		const bytes: Buffer = this.buffer.slice(this.pos, this.pos + len)
		this.pos += (len + 3) & ~3
		return fbDecodeCharset === 'WIN1251' ? win1251Decode(bytes) : bytes.toString('utf8')
	}

	const readSegment = FbSerialize.BlrReader.prototype.readSegment
	FbSerialize.BlrReader.prototype.readSegment = function (): Buffer {
		const segment: Buffer = readSegment.call(this)
		if (fbDecodeCharset !== 'WIN1251' || !Buffer.isBuffer(segment)) return segment
		return Buffer.from(win1251Decode(segment), 'utf8')
	}

	// fetchAll patch, see the note above the block for why it is here.
	FbWireConn.prototype.fetchAll = function (statement: any, transaction: any, callback: any): void {
		const self     = this
		const asStream = Boolean(statement.custom?.asStream && statement.custom?.on)
		const all: any[] = []
		let index        = 0

		const next = (): void => self.fetch(statement, transaction, FbConst.DEFAULT_FETCHSIZE, loop)

		const loop = (err: any, ret: any): void => {
			if (err) { callback(err); return }
			if (!ret?.data?.length) {
				if (ret?.fetched) callback(undefined, all)
				else              next()
				return
			}

			void (async (): Promise<void> => {
				try {
					for (const readBlob of (ret.arrBlob ?? [])) {
						const blob = await readBlob(transaction)
						ret.data[blob.row][blob.column] = blob.value
					}
					for (const row of ret.data) {
						if (asStream) statement.custom.on(row, index++)
						else          all.push(row)
					}
					if (ret.fetched) callback(undefined, all)
					else             next()
				} catch (e) {
					callback(e)
				}
			})()
		}

		next()
	}
})()
/* eslint-enable @typescript-eslint/no-explicit-any */

// A string literal, a quoted identifier, a line or a block comment - the places where
// what looks like a placeholder is just text. Everything else is code.
const FB_LITERAL = /'(?:[^']|'')*'|"(?:[^"]|"")*"|--[^\n]*|\/\*[\s\S]*?\*\//

// Index of the AS that closes the header of an EXECUTE BLOCK, or -1. Both lists the
// header may carry - the input parameters and RETURNS - are parenthesized, so the AS
// we want is the first one outside any parentheses, and outside literals and comments.
function execBlockBodyAt(query: string): number {
	if (!/^\s*EXECUTE\s+BLOCK\b/i.test(query)) return -1
	const scan = new RegExp(`${FB_LITERAL.source}|[()]|\\bAS\\b`, 'gi')
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
	const codeTo = bodyAt < 0 ? query.length : bodyAt
	const scan   = new RegExp(`${FB_LITERAL.source}|:(\\w+)`, 'g')

	const sql = query.replace(scan, (token: string, name: string | undefined, at: number) => {
		if (name === undefined || at >= codeTo) return token
		const found = params.find((p) => p.name === name)
		if (!found) throw new Error(`fb: param not found: ${name}`)
		values.push(found.value)
		return '?'
	})
	return { sql, values }
}

// A lost connection must end the query with an error, and node-firebird does not let it.
// On a closed socket it waits a second, quietly attaches anew and moves the callbacks
// still waiting for an answer onto the new connection - but the requests behind them
// were sent on the old one, so the answer never comes. The read stops without an error
// and the job never ends: a restart of the server, an attachment killed on its side, a
// network failure - it is the same silent hang every time. Here the close is seen right
// after the driver has scheduled that reconnect, the reconnect is cancelled, and the
// query ends with an error. The "error" and "reconnect" events of the database are taken
// as the same loss. The "error" listener also stays for good: the driver emits it from
// the socket handler, and with nobody listening it would be thrown from there.
/* eslint-disable @typescript-eslint/no-explicit-any */
function watchConnection(db: Firebird.Database, onLost: (error: string) => void): void {
	// The typings leave out that Database is an EventEmitter.
	const events: any = db
	const conn: any   = events.connection
	events.on('error', (err: unknown) => onLost(`fb: connection lost: ${String(err)}`))
	events.on('reconnect', () => onLost('fb: connection lost, the driver reattached on its own'))
	conn?._socket?.on('close', () => {
		// Set by our own detach at the end of the query - a close that was asked for.
		if (conn._isDetach) return
		clearTimeout(conn._retry_connection_id)
		onLost('fb: connection lost')
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
	constructor(private readonly p: TConnFb) {
		fbDecodeCharset = p.charset ?? 'UTF8'
	}

	kind(): 'fb' { return 'fb' }
	title(): string { return `${this.p.host}:${this.p.port || 'DEFAULT'} [${this.p.database}]` }
	user(): string { return this.p.user }
	versionQuery(): string { return "SELECT rdb$get_context('SYSTEM', 'ENGINE_VERSION') AS ver FROM rdb$database" }
	pingQuery(): string { return 'SELECT 1 AS p FROM rdb$database' }

	async execStream(query: string, params: TDbParam[], callback: (event: TDbStreamEvent) => void, signal?: AbortSignal): Promise<void> {
		const { sql, values } = buildQuery(query, params)
		const options: Firebird.Options = {
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
		return new Promise<void>((resolve) => {
			let done = false
			let db: Firebird.Database | undefined
			const finish = (error?: string): void => {
				if (done) return
				done = true
				signal?.removeEventListener('abort', onAbort)
				callback({ kind: 'finish', error })
				resolve()
			}
			const onAbort = (): void => {
				if (db) dropConnection(db)
				finish(String(signal?.reason ?? 'aborted'))
			}
			if (signal?.aborted) { finish(String(signal.reason ?? 'aborted')); return }
			signal?.addEventListener('abort', onAbort)

			Firebird.attach(options, (attachErr, attached) => {
				if (attachErr) { finish(String(attachErr)); return }
				// Given up while the attach was on its way - nobody is waiting for this one.
				if (done) { dropConnection(attached); return }
				db = attached
				watchConnection(db, (error) => {
					dropConnection(attached)
					finish(error)
				})
				let schemaEmitted = false
				db.sequentially(
					sql,
					values,
					(row: Record<string, unknown>, _index: number) => {
						if (done) return
						if (!schemaEmitted) {
							schemaEmitted = true
							const schema: TDbCol[] = Object.keys(row).map((name) => ({ name, kind: 'unknown' }))
							callback({ kind: 'schema', schema })
						}
						callback({ kind: 'row', row })
					},
					(seqErr?: Error) => {
						if (done) return
						attached.detach()
						finish(seqErr ? String(seqErr) : undefined)
					},
				)
			})
		})
	}

	async execTxDynamic(cb: TDbTxCb, timeoutMs?: number): Promise<TResult<void>> {
		const options: Firebird.Options = {
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
		const db = await new Promise<Firebird.Database>((resolve, reject) => {
			Firebird.attach(options, (err, d) => { if (err) reject(err); else resolve(d) })
		})
		const txWork = async (): Promise<TResult<void>> => {
			try {
				const transaction = await new Promise<Firebird.Transaction>((resolve, reject) => {
					db.transaction(Firebird.ISOLATION_READ_COMMITTED, (err, tx) => { if (err) reject(err); else resolve(tx) })
				})
				try {
					const exec = (query: string, params: TDbParam[]): Promise<TDbTxResult> => {
						const { sql: sqlStr, values } = buildQuery(query, params)
						return new Promise<TDbTxResult>((resolve, reject) => {
							// node-firebird may throw inside its native code before calling our
							// callback (e.g. when processing BLOB params). Catch that here so
							// the promise always settles and the try/catch above can rollback.
							const onUncaught = (err: Error) => reject(err)
							process.once('uncaughtException', onUncaught)
							transaction.query(sqlStr, values, (err: Error | null, rows: Record<string, unknown>[]) => {
								process.removeListener('uncaughtException', onUncaught)
								if (err) reject(err)
								else resolve({ rows: rows ?? [] })
							})
						})
					}
					await cb(exec)
					await new Promise<void>((resolve, reject) => {
						transaction.commit((err: Error | null) => { if (err) reject(err); else resolve() })
					})
					db.detach()
					return { ok: true, result: undefined }
				} catch (err) {
					try { await new Promise<void>((resolve) => { transaction.rollback(() => resolve()) }) } catch {}
					db.detach()
					return { ok: false, error: String(err) }
				}
			} catch (err) {
				try { db.detach() } catch {}
				return { ok: false, error: String(err) }
			}
		}
		if (!timeoutMs) return txWork()
		let timer: ReturnType<typeof setTimeout> | undefined
		const timeoutPromise = new Promise<TResult<void>>((resolve) => {
			timer = setTimeout(() => {
				try { db.detach() } catch {}
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
