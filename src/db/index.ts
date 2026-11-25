import type { TResult } from '../tresult'
import type { TConnectionParam, TDbKind } from './connection'
import { dbTitle, dbUser } from './connection'
import type { IDbAdapter, TDbError, TDbErrorKind, TDbParam, TDbRes, TDbStreamEvent, TDbTxCb, TExecOptions, TTxOptions } from './types'
import { toDbError, assertTimeoutMs, abortError, assertOraPool, awaitWithSignal, StreamAbort } from './helpers'

// How long a streaming query may stay silent by default: until the first data after it
// was sent, and between one portion of data and the next. Both are overridable per call
// through TExecOptions.
// FIRST_DATA is the bound against a query that never produces its first byte at all - a
// half-open socket the driver did not notice, a frozen server, a statement stuck on a
// lock. It defaults to 10 minutes rather than 12 hours: a query that needs longer than
// that to yield its FIRST byte is rare, and such a caller overrides it per call through
// firstDataTimeoutMs. NEXT_ROW stays at 12h on purpose - a pause between portions of an
// ongoing stream is legitimately long (a report produced row by row).
const FIRST_DATA_TIMEOUT_MS = 10 * 60 * 1000
const NEXT_ROW_TIMEOUT_MS   = 12 * 60 * 60 * 1000

// A connection left idle past this threshold is probed with a short liveness ping before
// the next query runs: the server (or a NAT/firewall on the way) may have dropped the
// idle TCP socket without the client noticing (half-open), and the first real query would
// then wait out its whole FIRST_DATA deadline before failing. The ping detects the dead
// socket and tears it down, so the query that follows goes out on a fresh connection. It
// is a best-effort fast recovery - not a substitute for the query's own deadline - so the
// idle threshold is conservative (30s) and normal traffic never pings.
const LIVENESS_IDLE_THRESHOLD_MS = 30_000
// The ping is a SELECT 1 that must answer fast; 5s is enough for a healthy connection and
// short enough not to add a visible stall on a loaded-but-alive server.
const LIVENESS_TIMEOUT_MS = 5_000

// A query that ended in a timeout/abort/connect failure tells its driver to tear the
// connection down, but a driver stuck in that cancel on a dead socket may never settle its
// promise (mssql ATTENTION/ack, Oracle break(), and Firebird's behaviour are all bounded
// only by TCP retransmission). Without this the orphaned call would hold its liveness slot
// forever and every later query would skip the ping - the very hang the ping exists to
// catch. After this grace the orphan is released and stops blocking the ping; the grace is
// long enough for a healthy driver's teardown (milliseconds) and short enough not to starve
// liveness on the pool's other, equally idle sockets.
const ORPHAN_GRACE_MS = 10_000

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
	private _lastUseAt = performance.now()
	private _livenessPromise: Promise<void> | undefined
	// How many liveness pings currently have their adapter call physically running. The
	// ping's 5s wait deadline and the operation's lifetime are different: after the
	// deadline the waiter is released, but the adapter may still be tearing the socket
	// down (a cancel that hangs until the driver's own cancelTimeout) and holding a pool
	// slot. A counter rather than a boolean: each probe releases itself exactly once (on
	// settle or on the orphan grace), so a late settle of an already-orphaned probe cannot
	// release the next one. No second probe is started while any probe is still running.
	private _probesInFlight = 0
	// The ping deadline and the orphan grace, kept as fields so tests can shrink them;
	// the module constants are the production defaults.
	private _livenessTimeoutMs = LIVENESS_TIMEOUT_MS
	private _orphanGraceMs = ORPHAN_GRACE_MS
	// The connection is under suspicion: a query failed as timeout/connect/aborted, or a
	// liveness ping did not get an answer. The next query must ping even if the idle clock
	// is fresh. Cleared by a successful ping, or by a successful query that started after
	// the last suspicion was raised (see _suspectEpoch) - a live query is the best proof
	// there is, and only relying on the ping would loop forever when pings are slow.
	private _suspect = false
	// Bumped whenever a new suspicion is raised. A ping may clear _suspect only if no new
	// suspicion was raised while it was in flight; a successful query clears it only if it
	// started after the last bump. Guards against a stale success erasing newer evidence.
	private _suspectEpoch = 0
	// Calls admitted to a query, from reserving their slot (before the liveness decision)
	// until their adapter call finishes - including the time they spend waiting on a shared
	// ping. A ping is started only when this very call is the sole admitted one
	// (_active === 1): the ping probes the pool's *next* socket, not the one a concurrent
	// query already holds, so it must not run alongside a real query. Reserving the slot
	// before the liveness decision closes the window where the ping's shared promise is
	// cleared but its waiters have not yet re-entered the adapter - without it a new call
	// would see _active === 0 and start a second ping while the first was still in flight.
	private _active = 0

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

	// Probes the connection when it has been idle past LIVENESS_IDLE_THRESHOLD_MS or the
	// connection is under suspicion (a previous timeout/connect/abort, or a ping that did
	// not get an answer). The ping is a SELECT 1 with its own short deadline
	// (LIVENESS_TIMEOUT_MS); on a half-open socket it times out, the adapter tears the
	// socket down (its abort path), and the query that follows acquires a fresh connection.
	// Concurrent callers share one in-flight ping, so a burst after an idle does not fire
	// one ping per call. The caller's signal only cuts short THIS caller's wait on the
	// shared ping - the ping itself is not cancelled, other callers may still be waiting.
	private async _ensureLiveness(adapter: IDbAdapter, signal?: AbortSignal): Promise<void> {
		// An already-aborted caller must not start the ping (it has already given up on the
		// query) nor wait out a shared one - execStream re-checks the signal right after and
		// ends the call as aborted. Checked first so a cancelled request does not even warm
		// the pool.
		if (signal?.aborted) return
		if (!this._livenessPromise) {
			// Never start a ping while another query is in flight: this call's own slot is
			// already reserved (execStream bumped _active before calling), so "no other query
			// running" is _active === 1. The ping probes the pool's *next* socket, not the
			// one a concurrent query holds, so it would test the wrong thing and add a
			// needless roundtrip. _probesInFlight additionally blocks a second probe while any
			// previous probe's adapter call is still running (or within its orphan grace).
			if (this._active > 1 || this._probesInFlight > 0) return
			const idle = performance.now() - this._lastUseAt > LIVENESS_IDLE_THRESHOLD_MS
			if (!idle && !this._suspect) return
			// The ping may clear _suspect only if no newer suspicion was raised while it ran;
			// a suspicion raised during the flight survives a later successful ping.
			const epoch = this._suspectEpoch
			this._livenessPromise = this._runLivenessPing(adapter)
				.then((alive) => {
					if (!alive) this._raiseSuspicion()
					else if (this._suspectEpoch === epoch) this._suspect = false
				})
				.catch(() => { this._raiseSuspicion() })
				.finally(() => {
					this._lastUseAt = performance.now()
					this._livenessPromise = undefined
				})
		}
		// Join the in-flight ping BEFORE deciding whether one is needed: a ping already
		// started (for idle or suspicion) must be awaited by every new caller, otherwise a
		// caller could slip past it on a freshly updated idle clock and hit the very socket
		// the ping was about to condemn.
		const p = this._livenessPromise
		if (!signal) { await p; return }
		// The listener is removed on both exits so a long-lived signal reused across queries
		// (e.g. an application shutdown signal) does not accumulate abort listeners.
		await new Promise<void>((resolve) => {
			// Re-check aborted inside the synchronous executor: the ping's own synchronous
			// adapter call above could have aborted the external signal after the check at the
			// top of this method but before this listener was registered, and addEventListener
			// does not deliver an abort that already happened.
			if (signal.aborted) { resolve(); return }
			const settle = (): void => { signal.removeEventListener('abort', settle); resolve() }
			signal.addEventListener('abort', settle)
			p.then(settle, settle)
		})
	}

	// The ping runs straight through the adapter (not through execStream) so it cannot
	// trigger liveness recursively. Its failure is swallowed on purpose: whether the ping
	// times out or the driver throws, the connection is already being torn down by the
	// adapter's abort path, and the caller's real query must not be failed by the ping
	// itself - it goes out on a fresh connection instead. Returns whether the connection
	// actually answered: true only for a finish WITHOUT an error. For a liveness probe any
	// error means "not healthy": a plain query error on a SELECT 1 is still evidence the
	// ping could not be served, so the socket is not to be trusted either way.
	private async _runLivenessPing(adapter: IDbAdapter): Promise<boolean> {
		// The probe occupies a flight slot from here until its adapter promise settles or
		// the orphan grace passes - not merely until the 5s wait deadline below. A driver
		// stuck in cancel after the deadline still holds a pool slot, and _ensureLiveness
		// must not start a second probe over it.
		this._probesInFlight++
		let released = false
		// Each probe releases its own slot exactly once - on settle or on grace. The
		// release is idempotent and closure-local, so a late settle of an already-orphaned
		// probe cannot release a newer probe's slot: the counter, not a shared boolean,
		// decides whether a ping may start. It also clears the grace timer, so a probe that
		// settles promptly does not leave a dangling 10 s timer (and its closure over this)
		// behind.
		let probeGrace: ReturnType<typeof setTimeout> | undefined
		const releaseProbe = (): void => {
			if (released) return
			released = true
			if (probeGrace) { clearTimeout(probeGrace); probeGrace = undefined }
			this._probesInFlight--
		}
		// The grace bounds only the teardown after the probe logically ended (finish or
		// timeout), not the probe itself - the 5s deadline already bounds that. It is
		// started at the logical end, so a driver hung in cancel still has the full grace
		// before it stops blocking the next probe.
		const startProbeGrace = (): void => {
			if (probeGrace) return
			probeGrace = setTimeout(releaseProbe, this._orphanGraceMs)
			probeGrace.unref?.()
		}
		const ctrl = new AbortController()
		// The terminal result, locked at the first event that settles the ping: a finish
		// (alive only if it carried no error) or the 5s timeout (alive=false,
		// locked BEFORE the abort goes out so a late finish after the abort can't turn the
		// timeout back into a success). A later event is ignored.
		let settled = false
		let alive = false
		let giveUp!: () => void
		const gaveUp = new Promise<void>((resolve) => { giveUp = resolve })
		const abortTimer = setTimeout(() => {
			if (settled) return
			settled = true
			alive = false
			// Fix the outcome before the abort so a late finish can never turn this timeout
			// back into a success. The waiter is released on the same line; the abort below
			// follows it (the adapter's synchronous abort handler runs before the await
			// continues, which is fine - the result is already terminal).
			giveUp()
			startProbeGrace()
			ctrl.abort(new StreamAbort({
				message: `timeout: liveness ping got no data within ${humanMs(this._livenessTimeoutMs)}`,
				kind: 'timeout',
				code: 'TIMEOUT',
			}))
		}, this._livenessTimeoutMs)
		try {
			const run: Promise<void> = (async (): Promise<void> => {
				await adapter.execStream(adapter.pingQuery(), [], (event) => {
					if (event.kind !== 'finish' || settled) return
					settled = true
					alive = event.error === undefined
					// The finish ends the wait on its own; clear the timeout so a healthy
					// connection is not torn down because the adapter resolves late (or never)
					// in its own teardown after the finish was already delivered. That teardown
					// still holds a pool slot until settle, so its grace starts now too.
					clearTimeout(abortTimer)
					giveUp()
					startProbeGrace()
				}, ctrl.signal)
			})()
			// Settling (either side) ends the probe's terminal state if nothing else has,
			// then releases its slot. Locking settled here means a resolve without a finish
			// cannot be re-answered by a late finish (which would otherwise start a
			// pointless grace), and the reject branch also swallows the rejection so a
			// later reject (after the race settled) never becomes an unhandledRejection.
			run.then(
				() => { if (!settled) { settled = true; alive = false }; releaseProbe() },
				() => { if (!settled) { settled = true; alive = false }; releaseProbe() },
			)
			await Promise.race([run, gaveUp])
		} catch {
			// A ping crash must not fail the caller's query; it leaves the result as
			// "not alive" so the next query pings again.
			if (!settled) { settled = true; alive = false }
		} finally {
			clearTimeout(abortTimer)
		}
		return alive
	}

	// Raises the suspicion in one place so the epoch bump can never be missed. The
	// invariant "the epoch grows on every raise" is what lets a success that started
	// before the raise leave a newer suspicion alone (see _markUsed).
	private _raiseSuspicion(): void {
		this._suspect = true
		this._suspectEpoch++
	}

	// Records a query's end for the idle clock. A timeout, connect failure or abort leaves
	// the connection in an unknown state (it may be exactly the half-open socket the ping
	// is meant to catch): flag it suspect and bump the epoch. A successful query that
	// started after the last suspicion was raised (startEpoch unchanged) clears it - a live
	// query is the best proof there is, and relying only on the ping would loop forever on
	// slow-but-healthy connections. A query error does neither: the server answered, but a
	// failed statement is not proof the socket is fine.
	private _markUsed(errorKind?: TDbErrorKind, startEpoch?: number): void {
		if (errorKind === 'timeout' || errorKind === 'connect' || errorKind === 'aborted') {
			this._raiseSuspicion()
		} else if (errorKind === undefined && startEpoch === this._suspectEpoch) {
			this._suspect = false
		}
		this._lastUseAt = performance.now()
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
			// The adapter load (driver import, first pool connect) is shared and must not be
			// cancelled for everyone, but a cancellation while it is still loading must end
			// THIS call as aborted now rather than waiting out the load and then mislabelling
			// its outcome. awaitWithSignal lets this waiter go early, the load keeps running.
			adapter = await awaitWithSignal(this._adapter(), options?.signal)
		} catch (err) {
			// The driver is missing (an optional peer dependency), the kind is unknown, or the
			// caller aborted during the load: end with a finish instead of a bare rejection,
			// matching exec()'s contract.
			try {
				callback({ kind: 'finish', error: options?.signal?.aborted ? abortError(options.signal.reason) : toDbError(err, 'connect') })
			} catch { /* the consumer's own finish handler threw */ }
			return
		}
		// Reserve this call's slot BEFORE the liveness decision: _ensureLiveness starts a ping
		// only when this call is the sole admitted one (_active === 1), and holding the slot
		// from here (rather than after the ping) closes the window where the ping's shared
		// promise is cleared but its waiters have not re-entered the adapter - without it a new
		// call would see _active === 0 and start a second ping while the first was still in
		// flight. The slot is released when the adapter call settles (run.then below), or by an
		// explicit decrement on the paths that never launch it (abort while waiting on the ping,
		// a synchronous throw): holding it until the adapter call settles - not until this public
		// call stops waiting - mirrors the probe's flight slot (_probesInFlight), because a driver
		// stuck in cancel after a timeout still holds a pool slot and a concurrent ping would
		// test the wrong socket.
		this._active++
		// Whether the adapter call below was launched, and whether it has settled. The slot
		// is released only after BOTH the call's outcome is published (_markUsed in the
		// finally) AND the adapter call has settled - or the orphan grace below expired -
		// so a concurrent ping can never read a stale suspicion nor probe a socket a driver
		// is still tearing down.
		let runStarted = false
		let runSettled = false
		let outcomePublished = false
		let orphaned = false
		let slotReleased = false
		// Declared outside the try so the surrounding catch shares the terminal guard with
		// every other publication path: exactly one finish, whatever throws on any path.
		let settled = false
		let endErrorKind: TDbErrorKind | undefined
		let orphanTimer: ReturnType<typeof setTimeout> | undefined
		// Idempotent and throw-free by construction: it only guards on two booleans and
		// decrements a counter, so the surrounding finally can rely on it never breaking
		// the structural slot guarantee (unlike _markUsed, which is wrapped defensively).
		const releaseSlot = (): void => {
			if (slotReleased || !outcomePublished) return
			if (runStarted && !runSettled && !orphaned) return
			slotReleased = true
			this._active--
		}
		// The slot reserved above is released in the surrounding finally on every path -
		// the abort below, a throw before the inner try/finally, and the normal run through
		// releaseSlot - so _active can never leak and leave liveness permanently disabled.
		try {
			// Before a query that follows a long idle, probe the connection with a short
			// liveness ping: a server/NAT/firewall may have dropped the idle socket without
			// the client noticing (half-open), and the real query would otherwise hang until
			// its silence deadline. The ping tears such a socket down so the query runs on a
			// fresh one. Skipped when the caller already aborted.
			await this._ensureLiveness(adapter, options?.signal)
			// The epoch the query's own outcome is compared against when deciding whether its
			// success may clear the suspicion (a suspicion raised later must not be erased).
			const startEpoch = this._suspectEpoch
			// A cancel that arrived while the call waited on the shared ping: _ensureLiveness
			// resolves on abort, but the query itself must not run on a signal the caller already
			// gave up on. End as aborted without touching the connection (the adapter call below
			// never launched, so the surrounding finally releases the slot).
			if (options?.signal?.aborted) {
				try {
					callback({ kind: 'finish', error: abortError(options.signal.reason) })
				} catch { /* the consumer's own finish handler threw */ }
				return
			}
			const abort = new AbortController()
			let timer: ReturnType<typeof setTimeout> | undefined
			let giveUp!: () => void
			const gaveUp = new Promise<void>((resolve) => { giveUp = resolve })
			// How the call ended, for _markUsed: a timeout, connect failure or abort leaves the
			// connection in an unknown state (it may be exactly the half-open socket the ping is
			// meant to catch) and flags it suspect; a success or query error marks it live.

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
				endErrorKind = error.kind
				// The signal carries the very error that is reported here (StreamAbort), not a
				// bare Error around its message: an adapter - or any other subscriber - that maps
				// the reason back through abortError() gets this error with its own kind and code,
				// so a timeout stays a timeout instead of turning into a generic abort.
				abort.abort(new StreamAbort(error))
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

			// Forward the external signal to the internal controller, which is what the adapter
			// listens to. The two awaits above (the adapter load and the shared ping) each had an
			// aborted re-check on their other side, so by here the signal is not yet aborted.
			let onExternalAbort: (() => void) | undefined
			if (options?.signal) {
				onExternalAbort = (): void => finishEarly(abortError(options.signal!.reason))
				options.signal.addEventListener('abort', onExternalAbort, { once: true })
			}

			// From here the connection is demonstrably busy: concurrent callers see _active > 0
			// and skip the ping while a real query is running.
			try {
				const run = adapter.execStream(query, params, (event) => {
					if (settled) return
					if (event.kind === 'finish') {
						settled = true
						clearTimeout(timer)
						endErrorKind = event.error?.kind
						// The finish is the terminal event and ends the caller's wait on its own:
						// the adapter must not strand the caller by resolving late (or never) in
						// its own teardown after the finish was already delivered.
						giveUp()
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
				runStarted = true
				// The adapter promise is raced against the timeout; when the timeout wins it is no
				// longer awaited, so a rejection it produces later must not become a
				// unhandledRejection that takes the process down on Node >= 15.
				run.catch(() => {})
				// The reserved slot is released only after the adapter call settles (either side)
				// AND the outcome is published in the finally below - see releaseSlot. Settling
				// here only records the fact; the finally decides when both halves are done. Both
				// handlers resolve (the returned promise never rejects), so no unhandledRejection
				// comes from this chain.
				run.then(
					() => { runSettled = true; if (orphanTimer) clearTimeout(orphanTimer); releaseSlot() },
					() => { runSettled = true; if (orphanTimer) clearTimeout(orphanTimer); releaseSlot() },
				)

				await Promise.race([run, gaveUp])
				// A well-behaved adapter ends with a finish event (which set `settled` above). If
				// the adapter resolved without one (an adapter bug), the consumer must not mistake
				// an incomplete stream for a complete one - report the contract violation as an
				// error finish, not a successful empty finish.
				if (!settled) {
					settled = true
					endErrorKind = 'query'
					try {
						callback({ kind: 'finish', error: toDbError(new Error('adapter resolved without emitting a finish event'), 'query') })
					} catch { /* consumer's finish handler threw */ }
				}
			} catch (err) {
				// A synchronous throw from the adapter - a non-async implementation throwing
				// before it returns its promise, or queryStream on Oracle rejecting run without
				// emitting a finish. exec() catches this above, but a direct execStream call must
				// also end with a finish rather than a bare rejection, and the cleanup below must
				// still run (the external-abort listener was already added above). The adapter
				// call never launched, so releaseSlot below frees the reserved slot right after
				// the outcome is published.
				finishEarly(toDbError(err, 'query'))
			} finally {
				clearTimeout(timer)
				if (options?.signal && onExternalAbort) options.signal.removeEventListener('abort', onExternalAbort)
				// A throw from _markUsed must not break the structural slot guarantee below;
				// it only reads/writes instance state, so a failure there never changes the
				// public outcome either way.
				try { this._markUsed(endErrorKind, startEpoch) } catch { /* never break the structural slot guarantee */ }
				// The outcome is now visible to the next caller (suspicion, idle clock). Only
				// then may the slot go: releaseSlot needs both this and the adapter call having
				// settled, so a concurrent ping can never read a stale suspicion or probe a
				// socket a driver is still tearing down.
				outcomePublished = true
				releaseSlot()
				// A launched run still unsettled after the outcome is published is a driver
				// tearing the socket down (or hung in cancel on a dead socket): give it the
				// grace, then release the slot anyway so one orphan does not disable liveness
				// forever on the pool's other idle sockets.
				if (runStarted && !runSettled && !orphaned) {
					orphanTimer = setTimeout(() => { orphaned = true; releaseSlot() }, this._orphanGraceMs)
					orphanTimer.unref?.()
				}
			}
		} catch (err) {
			// A throw from liveness or from the setup before the inner try must still end
			// the call as a finish, never a bare rejection. But this catch also sees throws
			// from the inner catch/finally AFTER the finish was already published, so it
			// must go through the same terminal guard as every other path: publish only if
			// no outcome is out yet; a cleanup error after the outcome must not re-answer it.
			// The error is built BEFORE settled is locked, with a fallback that does not call
			// toDbError, so a throw while reading an exotic thrown value (a throwing
			// getter/toString) can never leave the call with zero finish.
			if (!settled) {
				let error: TDbError
				try { error = toDbError(err, 'query') } catch { error = { message: 'unknown error', kind: 'query' } }
				settled = true
				try { callback({ kind: 'finish', error }) } catch { /* consumer's finish handler threw */ }
			}
		} finally {
			// Structural guarantee: whatever happens between _active++ above and here - a
			// throw from _ensureLiveness or from the setup before the inner try - the slot
			// still goes back. The normal paths have already released it (slotReleased) or
			// deliberately hold it until the adapter call settles / the grace passes
			// (runStarted && !runSettled && !orphaned), which must not be pre-empted here.
			if (!slotReleased && !(runStarted && !runSettled && !orphaned)) {
				slotReleased = true
				this._active--
			}
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
		// The fallback is a call of its own, so its silence deadline starts afresh (the
		// total can reach about twice the configured one) - documented in the README.
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
