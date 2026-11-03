import type { TResult } from '../tresult'
import type { TConnectionParam, TDbKind } from './connection'
import { dbTitle, dbUser } from './connection'
import type { IDbAdapter, TDbError, TDbParam, TDbRes, TDbStreamEvent, TDbTxCb, TExecOptions, TTxOptions } from './types'
import { toDbError } from './helpers'

// How long a streaming query may stay silent by default: until the first data after it
// was sent, and between one portion of data and the next. Both are overridable per call
// through TExecOptions.
const FIRST_DATA_TIMEOUT_MS = 12 * 60 * 60 * 1000
const NEXT_ROW_TIMEOUT_MS   = 12 * 60 * 60 * 1000

function humanMs(ms: number): string {
	if (ms < 1000) return `${ms} ms`
	if (ms < 60_000) return `${ms / 1000} s`
	if (ms < 3_600_000) return `${ms / 60_000} min`
	return `${ms / 3_600_000} h`
}

export class DbConnection {
	private readonly _connection: TConnectionParam
	private _adapterPromise: Promise<IDbAdapter> | undefined

	constructor(connection: TConnectionParam) {
		this._connection = connection
	}

	// Loads the adapter for this connection's driver on first use. The driver module is
	// imported only when its database is actually used - a consumer who talks to one
	// database never loads the other three drivers, which live in optional peer
	// dependencies and may not even be installed. Loading happens lazily (not in the
	// constructor) so that importing the package itself never requires any driver.
	private async _adapter(): Promise<IDbAdapter> {
		if (!this._adapterPromise) {
			this._adapterPromise = this._createAdapter(this._connection)
		}
		return this._adapterPromise
	}

	private async _createAdapter(connection: TConnectionParam): Promise<IDbAdapter> {
		switch (connection.kind) {
			case 'mssql': {
				const { MssqlAdapter } = await import('./adapters/mssql')
				return new MssqlAdapter(connection)
			}
			case 'pg': {
				const { PgAdapter } = await import('./adapters/pg')
				return new PgAdapter(connection)
			}
			case 'ora': {
				const { OraAdapter } = await import('./adapters/ora')
				return new OraAdapter(connection)
			}
			case 'fb': {
				const { FbAdapter } = await import('./adapters/fb')
				return new FbAdapter(connection)
			}
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
				} else if (event.kind === 'rowsAffected') {
					if (current) current.rowsAffected = event.count
				} else if (event.kind === 'outParams') {
					if (current) current.outParams = event.params
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
		const adapter = await this._adapter()
		const abort = new AbortController()
		let settled = false
		let timer: ReturnType<typeof setTimeout> | undefined
		let giveUp!: () => void
		const gaveUp = new Promise<void>((resolve) => { giveUp = resolve })

		const firstTimeout = options?.firstDataTimeoutMs ?? FIRST_DATA_TIMEOUT_MS
		const idleTimeout  = options?.idleTimeoutMs ?? NEXT_ROW_TIMEOUT_MS

		const timeoutError = (phase: 'first' | 'idle', ms: number): TDbError => ({
			message: phase === 'first'
				? `timeout: no data from the database within ${humanMs(ms)} after the query was sent`
				: `timeout: no new data from the database within ${humanMs(ms)}`,
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
			// The finish reaches the consumer's callback too. A throw from that handler
			// has nowhere left to report and must not reach the driver either, so it is
			// dropped. This also covers finishEarly being called from the catch below.
			try {
				callback({ kind: 'finish', error })
			} catch { /* the consumer's own finish handler threw */ }
			giveUp()
		}

		// Re-arming the silence timer per row used to be clearTimeout + setTimeout, which
		// allocates a fresh Timeout object on every row - measurable on millions of rows.
		// The deadline and the error text stay the same between rows, so refresh() the
		// existing timer (O(1), no allocation) and only rebuild it when the deadline itself
		// changes (first data -> idle). pendingError is read at fire time so the timer always
		// reports the current phase, not the one it was first armed with.
		let timerMs: number | undefined
		let pendingError: TDbError | undefined
		const arm = (ms: number, error: TDbError): void => {
			pendingError = error
			if (timer !== undefined && timerMs === ms) {
				timer.refresh()
			} else {
				clearTimeout(timer)
				timerMs = ms
				timer = setTimeout(() => finishEarly(pendingError!), ms)
			}
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

		const run = adapter.execStream(query, params, (event) => {
			if (settled) return
			if (event.kind === 'finish') {
				settled = true
				clearTimeout(timer)
			} else {
				arm(idleTimeout, timeoutError('idle', idleTimeout))
			}
			// The consumer's callback runs synchronously inside the driver's own event
			// handlers (mssql request.on('row'), pg q.on('row'), ora stream.on('data'),
			// fb sequentially). A throw here would otherwise reach the driver's
			// EventEmitter and become an uncaughtException or corrupt the driver, so it
			// is caught and turned into the error of the final finish.
			try {
				callback(event)
			} catch (err) {
				finishEarly(toDbError(err, 'query'))
			}
		}, abort.signal)
		// The adapter promise is raced against the timeout; when the timeout wins it is no
		// longer awaited, so a rejection it produces later must not become an
		// unhandledRejection that takes the process down on Node >= 15.
		run.catch(() => {})

		try {
			await Promise.race([run, gaveUp])
		} catch (err) {
			// A synchronous throw from the adapter (e.g. queryStream on Oracle) rejects run
			// without emitting a finish. exec() catches this above, but a direct execStream
			// call must also end with a finish rather than a bare rejection.
			finishEarly(toDbError(err, 'query'))
		} finally {
			clearTimeout(timer)
			if (options?.signal && onExternalAbort) options.signal.removeEventListener('abort', onExternalAbort)
		}
	}

	public async version(options?: TExecOptions): Promise<TResult<string, TDbError>> {
		const adapter = await this._adapter()
		const res = await this.exec(adapter.versionQuery(), [], options)
		if (!res.ok) return { ok: false, error: res.error }
		const firstRow = res.result[0]?.rows[0]
		if (!firstRow) return { ok: false, error: { message: 'no version returned', kind: 'query' } }
		const val = Object.values(firstRow)[0]
		return { ok: true, result: String(val) }
	}

	public kind(): TDbKind {
		return this._connection.kind
	}

	public title(): string {
		return dbTitle(this._connection)
	}

	public user(): string {
		return dbUser(this._connection)
	}

	// Accepts either the legacy positional timeout in milliseconds or a TTxOptions
	// object ({ timeoutMs, signal }). A number is kept for backward compatibility - a
	// bare timeout is the old call shape, and a signal alone is now expressible via
	// { signal }.
	public async execTxDynamic(cb: TDbTxCb, options?: number | TTxOptions): Promise<TResult<void, TDbError>> {
		const adapter = await this._adapter()
		const timeoutMs = typeof options === 'number' ? options : options?.timeoutMs
		const signal = typeof options === 'number' ? undefined : options?.signal
		return adapter.execTxDynamic(cb, timeoutMs, signal)
	}

	// Releases whatever the adapter holds between calls - the connection pool where the
	// driver has one, nothing for Firebird (each attach is closed by its own query). If
	// the adapter was never loaded, there is nothing to release.
	public async close(): Promise<void> {
		if (!this._adapterPromise) return
		const adapter = await this._adapterPromise
		return adapter.close()
	}

	public async ping(options?: TExecOptions): Promise<TResult<null, TDbError>> {
		const adapter = await this._adapter()
		const res = await this.exec(adapter.pingQuery(), [], options)
		if (!res.ok) return { ok: false, error: res.error }
		return { ok: true, result: null }
	}

}

