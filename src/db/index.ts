import type { TResult } from '../tresult'
import type { TConnectionParam, TDbKind } from './connection'
import { dbTitle, dbUser } from './connection'
import type { IDbAdapter, TDbError, TDbParam, TDbRes, TDbStreamEvent, TDbTxCb, TExecOptions, TTxOptions } from './types'
import { toDbError, assertTimeoutMs, abortError, assertOraPool } from './helpers'

// How long a streaming query may stay silent by default: until the first data after it
// was sent, and between one portion of data and the next. Both are overridable per call
// through TExecOptions.
const FIRST_DATA_TIMEOUT_MS = 12 * 60 * 60 * 1000
const NEXT_ROW_TIMEOUT_MS   = 12 * 60 * 60 * 1000

function humanMs(ms: number): string {
	const round = (n: number): number => Math.round(n * 100) / 100
	if (ms < 1000) return `${ms} ms`
	if (ms < 60_000) return `${round(ms / 1000)} s`
	if (ms < 3_600_000) return `${round(ms / 60_000)} min`
	return `${round(ms / 3_600_000)} h`
}

export class DbConnection {
	private readonly _connection: TConnectionParam
	private _adapterPromise: Promise<IDbAdapter> | undefined

	constructor(connection: TConnectionParam) {
		// A bad connectTimeout is a programming error, the same class as the per-call
		// silence deadlines (exec/execStream validate those up front). Validating here
		// makes it reject at construction instead of surfacing later as a 'connect'
		// error after a driver has already been loaded.
		assertTimeoutMs('connectTimeout', (connection as { connectTimeout?: number }).connectTimeout)
		// Oracle pool sizing is the same class of error, and must reject at construction
		// before the oracledb driver is loaded. OraAdapter re-validates through the same
		// helper for consumers who build an adapter directly - deliberate double check.
		if (connection.kind === 'ora') assertOraPool(connection)
		this._connection = connection
	}

	// Loads the adapter for this connection's driver on first use. The driver module is
	// imported only when its database is actually used - a consumer who talks to one
	// database never loads the other three drivers, which live in optional peer
	// dependencies and may not even be installed. Loading happens lazily (not in the
	// constructor) so that importing the package itself never requires any driver.
	private async _adapter(): Promise<IDbAdapter> {
		if (!this._adapterPromise) {
			// A failed load (missing driver / unknown kind) must not be cached forever:
			// clear the promise so the next call retries instead of replaying the same
			// rejection against a driver that may have been installed in the meantime.
			this._adapterPromise = this._createAdapter(this._connection).catch((err) => {
				this._adapterPromise = undefined
				throw err
			})
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
		// rowsAffected / outParams may legally arrive with no preceding schema (a DML
		// statement, or an out-only bind), so give them a result to attach to instead of
		// silently dropping them. A row with no schema is a driver bug and stays dropped.
		const ensureCurrent = (): TDbRes => {
			if (!current) {
				current = { schema: [], rows: [] }
				results.push(current)
			}
			return current
		}
		// Validate the silence deadlines before running: a bad value is a programming
		// error and must reject from exec() too. execStream already validates its own
		// deadlines, but with an 'execStream:' prefix; exec re-checks up front so the
		// TypeError carries its own 'exec:' prefix.
		assertTimeoutMs('exec: firstDataTimeoutMs', options?.firstDataTimeoutMs ?? FIRST_DATA_TIMEOUT_MS)
		assertTimeoutMs('exec: idleTimeoutMs', options?.idleTimeoutMs ?? NEXT_ROW_TIMEOUT_MS)
		try {
			await this.execStream(query, params, (event) => {
				if (event.kind === 'schema') {
					current = { schema: event.schema, rows: [] }
					results.push(current)
				} else if (event.kind === 'row') {
					current?.rows.push(event.row)
				} else if (event.kind === 'rowsAffected') {
					ensureCurrent().rowsAffected = event.count
				} else if (event.kind === 'outParams') {
					ensureCurrent().outParams = event.params
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
		const firstTimeout = options?.firstDataTimeoutMs ?? FIRST_DATA_TIMEOUT_MS
		const idleTimeout  = options?.idleTimeoutMs ?? NEXT_ROW_TIMEOUT_MS
		// Validate the silence deadlines first, before any driver is loaded: a negative,
		// NaN or infinite value would otherwise fire at once (or never) instead of failing
		// as the programming error it is. 0 means "no deadline for this phase". A bad value
		// is a programming error, so it throws a TypeError exactly like execTxDynamic's
		// timeout validation instead of being reported as a 'query' error.
		assertTimeoutMs('execStream: firstDataTimeoutMs', firstTimeout)
		assertTimeoutMs('execStream: idleTimeoutMs', idleTimeout)
		// An already-aborted external signal must not even load the driver: end the call
		// as aborted before any driver module is imported.
		if (options?.signal?.aborted) {
			// The finish reaches the consumer's callback too; a throw from that handler must
			// not reject execStream (symmetric with the other finish paths).
			try {
				callback({ kind: 'finish', error: abortError(options.signal.reason) })
			} catch { /* the consumer's own finish handler threw */ }
			return
		}
		let adapter: IDbAdapter
		try {
			adapter = await this._adapter()
		} catch (err) {
			// The driver is missing (an optional peer dependency) or the kind is unknown:
			// end with a finish instead of a bare rejection, matching exec()'s contract.
			try {
				callback({ kind: 'finish', error: toDbError(err, 'connect') })
			} catch { /* the consumer's own finish handler threw */ }
			return
		}
		const abort = new AbortController()
		let settled = false
		let timer: ReturnType<typeof setTimeout> | undefined
		let giveUp!: () => void
		const gaveUp = new Promise<void>((resolve) => { giveUp = resolve })

		const timeoutError = (phase: 'first' | 'idle', ms: number): TDbError => ({
			message: phase === 'first'
				? `timeout: no data from the database within ${humanMs(ms)} after the query was sent`
				: `timeout: no new data from the database within ${humanMs(ms)}`,
			kind: 'timeout',
			// An identifier keeps this synthetic error recognizable to isDbError() (the
			// message is dynamic and there is no stack to fall back on).
			code: 'TIMEOUT',
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
			if (ms === 0) {
				// 0 means "no deadline for this phase": disarm the timer entirely.
				clearTimeout(timer)
				timer = undefined
				timerMs = ms
				return
			}
			if (timer !== undefined && timerMs === ms) {
				timer.refresh()
			} else {
				clearTimeout(timer)
				timerMs = ms
				timer = setTimeout(() => finishEarly(pendingError!), ms)
			}
		}
		// Both the deadline and the error text stay fixed between rows, so build the
		// two error objects once instead of allocating one per row (timeoutError()
		// used to run on every idle re-arm); arm() only refreshes the timer.
		const firstError = timeoutError('first', firstTimeout)
		const idleError = timeoutError('idle', idleTimeout)
		arm(firstTimeout, firstError)

		// The external signal may have aborted while _adapter() was loading the driver
		// (the early check above ran before that await): re-check it now so an abort that
		// fired during load still ends the call as aborted instead of running a query on a
		// signal the caller already gave up on. Otherwise forward it to the internal
		// controller, which is what the adapter listens to.
		let onExternalAbort: (() => void) | undefined
		if (options?.signal) {
			if (options.signal.aborted) {
				finishEarly(abortError(options.signal.reason))
				return
			}
			onExternalAbort = (): void => finishEarly(abortError(options.signal!.reason))
			options.signal.addEventListener('abort', onExternalAbort, { once: true })
		}

		const run = adapter.execStream(query, params, (event) => {
			if (settled) return
			if (event.kind === 'finish') {
				settled = true
				clearTimeout(timer)
			} else {
				arm(idleTimeout, idleError)
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
			// A well-behaved adapter ends with a finish event (which set `settled` above).
			// If the adapter resolved without one (an adapter bug), the consumer would never
			// see a finish; deliver it here so the one-finish stream contract always holds.
			if (!settled) {
				settled = true
				try { callback({ kind: 'finish' }) } catch { /* consumer's finish handler threw */ }
			}
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
		// Validate the silence deadlines before loading the adapter: an unknown `kind`
		// together with a bad timeout must fail as the programming error it is (TypeError),
		// not as a 'connect' error from a driver that never should have been loaded.
		assertTimeoutMs('version: firstDataTimeoutMs', options?.firstDataTimeoutMs ?? FIRST_DATA_TIMEOUT_MS)
		assertTimeoutMs('version: idleTimeoutMs', options?.idleTimeoutMs ?? NEXT_ROW_TIMEOUT_MS)
		// An already-aborted signal must not load the driver: fail as aborted (symmetric
		// with exec/execStream/execTxDynamic) instead of a missing driver surfacing as 'connect'.
		if (options?.signal?.aborted) return { ok: false, error: abortError(options.signal.reason) }
		let adapter: IDbAdapter
		try {
			adapter = await this._adapter()
		} catch (err) {
			return { ok: false, error: toDbError(err, 'connect') }
		}
		const res = await this.exec(adapter.versionQuery(), [], options)
		if (res.ok) {
			const firstRow = res.result[0]?.rows[0]
			if (firstRow) {
				const v = Object.values(firstRow)[0]
				// A NULL (or an empty row) is "no version", not the string "null"/"undefined".
				if (v == null) return { ok: false, error: { message: 'no version returned', kind: 'query', code: 'NO_VERSION' } }
				return { ok: true, result: String(v) }
			}
			return { ok: false, error: { message: 'no version returned', kind: 'query', code: 'NO_VERSION' } }
		}
		// Some servers hide the primary version source from unprivileged users (Oracle's
		// v$version needs SELECT_CATALOG_ROLE). Fall back to a query anyone can run - but
		// only when the primary failed as a query: a timeout, abort or connect failure
		// would just repeat the same failure against the fallback and wait it out again.
		if (res.error.kind === 'query' && adapter.versionFallbackQuery) {
			// An abort that fired between the primary query and the fallback must not run
			// the fallback: it would come back 'aborted' and get folded into the primary
			// 'query' error as a confusing "fallback also failed" message. Report it directly.
			if (options?.signal?.aborted) return { ok: false, error: abortError(options.signal.reason) }
			const fb = await this.exec(adapter.versionFallbackQuery(), [], options)
			if (fb.ok) {
				const firstRow = fb.result[0]?.rows[0]
				if (firstRow) {
					const v = Object.values(firstRow)[0]
					if (v == null) return { ok: false, error: { message: 'no version returned (fallback returned NULL)', kind: 'query', code: 'NO_VERSION' } }
					return { ok: true, result: String(v) }
				}
				// The fallback ran (so the primary failure is not the whole story) but also
				// returned no rows: report that instead of the primary error, which would
				// hide the fact that the fallback actually succeeded and came back empty.
				return { ok: false, error: { message: 'no version returned (fallback returned no rows)', kind: 'query', code: 'NO_VERSION' } }
			} else {
				// Both the primary and the fallback failed: report the primary error and
				// keep the fallback's own reason too, instead of hiding it.
				return { ok: false, error: { ...res.error, message: `${res.error.message} (fallback also failed: ${fb.error.message})` } }
			}
		}
		return { ok: false, error: res.error }
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
	// { signal }. The callback's resolved value is returned as the result, so a value
	// computed inside the transaction does not need a closure to escape.
	public async execTxDynamic<T = void>(cb: TDbTxCb<T>, options?: TTxOptions): Promise<TResult<T, TDbError>>
	/** @deprecated Pass { timeoutMs } instead of a bare number of milliseconds. */
	public async execTxDynamic<T = void>(cb: TDbTxCb<T>, timeoutMs?: number): Promise<TResult<T, TDbError>>
	public async execTxDynamic<T = void>(cb: TDbTxCb<T>, options?: number | TTxOptions): Promise<TResult<T, TDbError>> {
		const timeoutMs = typeof options === 'number' ? options : options?.timeoutMs
		const signal = typeof options === 'number' ? undefined : options?.signal
		// Validate the deadline before touching any driver: an invalid timeoutMs is a
		// programming error (a TypeError, per README) and must not be masked by a
		// connect error when the driver is missing.
		assertTimeoutMs('execTxDynamic: timeoutMs', timeoutMs)
		// An already-aborted signal should fail the transaction without loading a driver,
		// symmetric with execStream's early abort.
		if (signal?.aborted) return { ok: false, error: abortError(signal.reason) }
		let adapter: IDbAdapter
		try {
			adapter = await this._adapter()
		} catch (err) {
			return { ok: false, error: toDbError(err, 'connect') }
		}
		return adapter.execTxDynamic(cb, timeoutMs, signal)
	}

	// Releases whatever the adapter holds between calls - the connection pool where the
	// driver has one, nothing for Firebird (each attach is closed by its own query). If
	// the adapter was never loaded, there is nothing to release.
	public async close(): Promise<void> {
		if (!this._adapterPromise) return
		// A load still in flight can reject (missing driver / unknown kind); there is
		// nothing to close then, so swallow it instead of making close() throw on a
		// machine that never had the driver installed.
		const adapter = await this._adapterPromise.catch(() => undefined)
		if (adapter) await adapter.close()
	}

	public async ping(options?: TExecOptions): Promise<TResult<null, TDbError>> {
		// Same as version(): a bad timeout must fail as a TypeError before any driver is
		// loaded, not surface as a 'connect' error.
		assertTimeoutMs('ping: firstDataTimeoutMs', options?.firstDataTimeoutMs ?? FIRST_DATA_TIMEOUT_MS)
		assertTimeoutMs('ping: idleTimeoutMs', options?.idleTimeoutMs ?? NEXT_ROW_TIMEOUT_MS)
		// An already-aborted signal must not load the driver: fail as aborted (symmetric
		// with exec/execStream/execTxDynamic) instead of a missing driver surfacing as 'connect'.
		if (options?.signal?.aborted) return { ok: false, error: abortError(options.signal.reason) }
		let adapter: IDbAdapter
		try {
			adapter = await this._adapter()
		} catch (err) {
			return { ok: false, error: toDbError(err, 'connect') }
		}
		const res = await this.exec(adapter.pingQuery(), [], options)
		if (!res.ok) return { ok: false, error: res.error }
		return { ok: true, result: null }
	}

}

