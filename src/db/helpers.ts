import type { TResult } from '../tresult'
import type { TDbError, TDbErrorKind } from './types'

// Builds a TDbError from whatever a driver throws. Each driver reports its own code in a
// different field, and the most specific one must win: mssql's `number` (1205, 2627) is
// the real SQL Server number while its `code` is only the driver class ('EREQUEST' /
// 'ELOGIN' / 'ETIMEOUT'); Firebird's `gdscode` is its own error code; pg's `code` is the
// SQLSTATE; Oracle's `code` is 'ORA-xxxxx' / 'NJS-xxxx'. `number`/`gdscode` therefore
// come before the generic `code`, with `sqlState` as the last fallback - this keeps a
// driver's real error identifier instead of the boilerplate wrapper around it.
export function toDbError(err: unknown, kind: TDbErrorKind): TDbError {
	const e = err as { message?: unknown; code?: unknown; number?: unknown; sqlState?: unknown; gdscode?: unknown } | null | undefined
	const code = e?.number ?? e?.gdscode ?? e?.code ?? e?.sqlState
	return {
		message: typeof e?.message === 'string' ? e.message : String(err),
		kind,
		...(code != null ? { code: String(code) } : {}),
		...(err instanceof Error ? { stack: err.stack } : {}),
	}
}

// Whether err is already a structured TDbError (as produced by toDbError). execTxDynamic
// uses it to tell a query error that bubbled through the user's transaction callback
// apart from the callback's own throw: the exec closure rejects with a tagged TDbError,
// so a raw throw that is not a TDbError is the user's code.
export function isDbError(err: unknown): err is TDbError {
	if (typeof err !== 'object' || err === null) return false
	const e = err as { message?: unknown; kind?: unknown; code?: unknown; stack?: unknown }
	return typeof e.message === 'string'
		&& (e.kind === 'connect' || e.kind === 'query' || e.kind === 'tx' || e.kind === 'user' || e.kind === 'timeout' || e.kind === 'aborted')
		// A real TDbError carries an error identifier (toDbError folds number/gdscode/
		// sqlState into `code`) or a stack from the underlying Error. A bare {message,
		// kind} literal is a user throw, not a database error.
		&& (e.code != null || e.stack != null)
}

// Runs a transaction body against a deadline and/or an external AbortSignal. The body
// receives an AbortSignal that fires when the deadline passes or the external signal is
// aborted; it is the body's job to tear its connection down hard on abort (cancel the
// backend / destroy the socket), so a query stuck on the server does not outlive the
// deadline. connect() lives inside the body, so it is covered by the same signal. A body
// result that arrives after the deadline is ignored - the race has already been settled.
export function withTxTimeout<T>(
	timeoutMs: number | undefined,
	body: (signal: AbortSignal) => Promise<TResult<T, TDbError>>,
	external?: AbortSignal,
): Promise<TResult<T, TDbError>> {
	// Validate the deadline first, synchronously: a negative value would fire the timer
	// at once, NaN / Infinity would disable it silently. Neither is a meaningful
	// deadline: reject it as the programming error it is rather than pretending the
	// transaction has timed out (or never will).
	assertTimeoutMs('withTxTimeout: timeoutMs', timeoutMs)

	const controller = new AbortController()
	const hasDeadline = timeoutMs !== undefined && timeoutMs !== 0

	// Neither a deadline nor an external signal: run the body directly, as before.
	if (!hasDeadline && !external) return body(controller.signal)

	// An already-aborted external signal settles the race at once without running the
	// body - its abort event has already fired and will not fire again.
	if (external?.aborted) {
		return Promise.resolve({ ok: false, error: abortedError(external.reason) })
	}

	return new Promise<TResult<T, TDbError>>((resolve) => {
		let settled = false
		let timer: ReturnType<typeof setTimeout> | undefined
		const cleanup = (): void => {
			if (timer) clearTimeout(timer)
			external?.removeEventListener('abort', onExternalAbort)
		}
		const settle = (r: TResult<T, TDbError>): void => {
			if (settled) return
			settled = true
			cleanup()
			resolve(r)
		}
		const onExternalAbort = (): void => {
			controller.abort(external!.reason)
			settle({ ok: false, error: abortedError(external!.reason) })
		}
		if (hasDeadline) {
			timer = setTimeout(() => {
				controller.abort(new Error('TIMEOUT'))
				settle({ ok: false, error: { message: 'TIMEOUT', kind: 'timeout' } })
			}, timeoutMs)
		}
		external?.addEventListener('abort', onExternalAbort, { once: true })
		body(controller.signal).then(
			(r) => settle(r),
			// The body itself rejected - a real failure of the transaction work, not a
			// deadline. Labelling it TIMEOUT would hide the actual error from the caller.
			(err) => settle({ ok: false, error: toDbError(err, 'query') }),
		)
	})
}

function abortedError(reason: unknown): TDbError {
	return {
		message: reason instanceof Error ? reason.message : String(reason ?? 'aborted'),
		kind: 'aborted',
		...(reason instanceof Error ? { stack: reason.stack } : {}),
	}
}

// Maps an AbortSignal's reason to a TDbError. The internal transaction deadline aborts
// with the sentinel Error('TIMEOUT') (see withTxTimeout), so that case reports kind
// 'timeout'; every other reason is an external abort and reports kind 'aborted'. The
// adapters call this from their abort handlers and early-abort branches, so an external
// abort is never mislabelled 'timeout' and an Error reason is never stringified into
// "Error: <message>".
export function abortError(reason: unknown): TDbError {
	if (reason instanceof Error && reason.message === 'TIMEOUT') {
		return { message: 'TIMEOUT', kind: 'timeout' }
	}
	return abortedError(reason)
}

// Validates a timeout option: undefined and 0 are the "no deadline" sentinel, anything
// else must be a finite non-negative number of milliseconds. A negative value would fire
// the timer at once, NaN too, and Infinity would disable it - none is a meaningful
// deadline. Throws a TypeError naming the option, so the caller fails as the programming
// error it is instead of getting "instant" or "never".
export function assertTimeoutMs(label: string, ms: number | undefined): void {
	if (ms !== undefined && ms !== 0 && (!Number.isFinite(ms) || ms < 0)) {
		throw new TypeError(`${label} must be a non-negative finite number of milliseconds, got ${ms}`)
	}
}
