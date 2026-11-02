import type { TResult } from '../tresult'
import type { TConnectionParam, TDbKind } from './connection'
import type { IDbAdapter, TDbError, TDbParam, TDbRes, TDbStreamEvent, TDbTxCb, TExecOptions } from './types'
import { toDbError } from './helpers'
import { MssqlAdapter } from './adapters/mssql'
import { PgAdapter } from './adapters/pg'
import { OraAdapter } from './adapters/ora'
import { FbAdapter } from './adapters/fb'

// How long a streaming query may stay silent by default: until the first data after it
// was sent, and between one portion of data and the next. Both are overridable per call
// through TExecOptions.
const FIRST_DATA_TIMEOUT_MS = 12 * 60 * 60 * 1000
const NEXT_ROW_TIMEOUT_MS   = 12 * 60 * 60 * 1000

function hours(ms: number): string {
	return `${ms / 3_600_000} h`
}

export class DbConnection {
	private _adapter!: IDbAdapter

	constructor(connection: TConnectionParam) {
		switch (connection.kind) {
			case 'mssql': this._adapter = new MssqlAdapter(connection); break
			case 'pg':    this._adapter = new PgAdapter(connection);    break
			case 'ora':   this._adapter = new OraAdapter(connection);   break
			case 'fb':    this._adapter = new FbAdapter(connection);    break
			default:
				throw new Error(`unsupported db kind: ${JSON.stringify((connection as { kind: unknown }).kind)}`)
		}
	}

	public async exec(query: string, params: TDbParam[], options?: TExecOptions): Promise<TResult<TDbRes[], TDbError>> {
		const results: TDbRes[] = []
		let current: TDbRes | undefined
		let execError: TDbError | undefined
		try {
			await this.execStream(query, params, (event) => {
				if (event.kind === 'schema') {
					current = { schema: event.schema, rows: [] }
					results.push(current)
				} else if (event.kind === 'row') {
					current?.rows.push(event.row)
				} else if (event.kind === 'finish') {
					execError = event.error
				}
			}, options)
		} catch (err) {
			return { ok: false, error: toDbError(err, 'query') }
		}
		if (execError !== undefined) return { ok: false, error: execError }
		return { ok: true, result: results }
	}

	// A query that has stopped sending data is cut off here, whatever the reason and
	// whatever the database. A lost connection the driver did not notice, a frozen server,
	// a statement waiting on a lock - none of them ends on its own, and the job above
	// would count as running forever and hold its pipe and its queue. The finish with
	// the error is given from here and the call returns at once; the adapter is only
	// asked to drop its connection, so a driver stuck in its own teardown cannot hold
	// it either.
	public async execStream(query: string, params: TDbParam[], callback: (event: TDbStreamEvent) => void, options?: TExecOptions): Promise<void> {
		const abort = new AbortController()
		let settled = false
		let timer: ReturnType<typeof setTimeout> | undefined
		let giveUp!: () => void
		const gaveUp = new Promise<void>((resolve) => { giveUp = resolve })

		const firstTimeout = options?.firstDataTimeoutMs ?? FIRST_DATA_TIMEOUT_MS
		const idleTimeout  = options?.idleTimeoutMs ?? NEXT_ROW_TIMEOUT_MS

		const timeoutError = (phase: 'first' | 'idle', ms: number): TDbError => ({
			message: phase === 'first'
				? `timeout: no data from the database within ${hours(ms)} after the query was sent`
				: `timeout: no new data from the database within ${hours(ms)}`,
			kind: 'timeout',
		})
		const abortedError = (reason: unknown): TDbError => ({
			message: reason instanceof Error ? reason.message : String(reason ?? 'aborted'),
			kind: 'aborted',
		})

		// Ends the call from here: tells the adapter to tear its connection down, hands
		// the error to the callback and releases the race, all at once.
		const finishEarly = (error: TDbError): void => {
			if (settled) return
			settled = true
			clearTimeout(timer)
			abort.abort(new Error(error.message))
			callback({ kind: 'finish', error })
			giveUp()
		}

		const arm = (ms: number, error: TDbError): void => {
			clearTimeout(timer)
			timer = setTimeout(() => finishEarly(error), ms)
		}
		arm(firstTimeout, timeoutError('first', firstTimeout))

		// An external signal the caller owns: forward it to the internal controller, which
		// is what the adapter listens to, and end the call as aborted.
		let onExternalAbort: (() => void) | undefined
		if (options?.signal) {
			if (options.signal.aborted) {
				finishEarly(abortedError(options.signal.reason))
				return
			}
			onExternalAbort = (): void => finishEarly(abortedError(options.signal!.reason))
			options.signal.addEventListener('abort', onExternalAbort, { once: true })
		}

		const run = this._adapter.execStream(query, params, (event) => {
			if (settled) return
			if (event.kind === 'finish') {
				settled = true
				clearTimeout(timer)
			} else {
				arm(idleTimeout, timeoutError('idle', idleTimeout))
			}
			callback(event)
		}, abort.signal)
		// The adapter promise is raced against the timeout; when the timeout wins it is no
		// longer awaited, so a rejection it produces later must not become an
		// unhandledRejection that takes the process down on Node >= 15.
		run.catch(() => {})

		try {
			await Promise.race([run, gaveUp])
		} finally {
			clearTimeout(timer)
			if (options?.signal && onExternalAbort) options.signal.removeEventListener('abort', onExternalAbort)
		}
	}

	public async version(): Promise<TResult<string, TDbError>> {
		const res = await this.exec(this._adapter.versionQuery(), [])
		if (!res.ok) return { ok: false, error: res.error }
		const firstRow = res.result[0]?.rows[0]
		if (!firstRow) return { ok: false, error: { message: 'no version returned', kind: 'query' } }
		const val = Object.values(firstRow)[0]
		return { ok: true, result: String(val) }
	}

	public kind(): TDbKind {
		return this._adapter.kind()
	}

	public title(): string {
		return this._adapter.title()
	}

	public user(): string {
		return this._adapter.user()
	}

	public async execTxDynamic(cb: TDbTxCb, timeoutMs?: number): Promise<TResult<void, TDbError>> {
		return this._adapter.execTxDynamic(cb, timeoutMs)
	}

	// Releases whatever the adapter holds between calls - the connection pool where the
	// driver has one, nothing for Firebird (each attach is closed by its own query).
	public async close(): Promise<void> {
		return this._adapter.close()
	}

	public async ping(): Promise<TResult<null, TDbError>> {
		const res = await this.exec(this._adapter.pingQuery(), [])
		if (!res.ok) return { ok: false, error: res.error }
		return { ok: true, result: null }
	}

}

export function createDbConnection(connection: TConnectionParam): DbConnection {
	return new DbConnection(connection)
}
