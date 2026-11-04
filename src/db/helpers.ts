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
	const e = err as { message?: unknown; code?: unknown; number?: unknown; sqlState?: unknown; gdscode?: unknown; stack?: unknown } | null | undefined
	const code = e?.number ?? e?.gdscode ?? e?.code ?? e?.sqlState
	const stack = err instanceof Error ? err.stack : (typeof e?.stack === 'string' ? e.stack : undefined)
	return {
		message: typeof e?.message === 'string' ? e.message : String(err),
		kind,
		...(code != null ? { code: String(code) } : {}),
		...(stack != null ? { stack } : {}),
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

// Validates Oracle pool-sizing parameters the way connectTimeout is: a negative, fractional
// or NaN poolMin/poolMax/poolIncrement/queueTimeout is a programming error that must reject
// at construction, before the oracledb driver is loaded (see DbConnection). OraAdapter
// re-validates through this same function for consumers who build an adapter directly.
export function assertOraPool(p: {
	poolMin?: number
	poolMax?: number
	poolIncrement?: number
	queueTimeout?: number
}): void {
	const nonNegInt = (name: string, value: number | undefined, min: number): void => {
		if (value !== undefined && (!Number.isInteger(value) || value < min)) {
			throw new TypeError(`ora: ${name} must be an integer >= ${min}`)
		}
	}
	nonNegInt('poolMin', p.poolMin, 0)
	nonNegInt('poolMax', p.poolMax, 1)
	nonNegInt('poolIncrement', p.poolIncrement, 1)
	nonNegInt('queueTimeout', p.queueTimeout, 0)
	if (p.poolMin !== undefined && p.poolMax !== undefined && p.poolMin > p.poolMax) {
		throw new TypeError('ora: poolMin must not exceed poolMax')
	}
}

// Runs a transaction body against a deadline and/or an external AbortSignal. The body
// receives an AbortSignal that fires when the deadline passes or the external signal is
// aborted; it is the body's job to tear its connection down hard on abort (cancel the
// backend / destroy the socket), so a query stuck on the server does not outlive the
// deadline. connect() lives inside the body, so it is covered by the same signal. A body
// result that arrives after the deadline is ignored - the race has already been settled.
export function withTxTimeout<T>(
	timeoutMs: number | undefined,
	body: (signal: AbortSignal, armCommitDeadline: () => void) => Promise<TResult<T, TDbError>>,
	external?: AbortSignal,
): Promise<TResult<T, TDbError>> {
	// Validate the deadline first, synchronously: a negative value would fire the timer
	// at once, NaN / Infinity would disable it silently. Neither is a meaningful
	// deadline: reject it as the programming error it is rather than pretending the
	// transaction has timed out (or never will).
	assertTimeoutMs('withTxTimeout: timeoutMs', timeoutMs)

	const controller = new AbortController()
	const hasDeadline = timeoutMs !== undefined && timeoutMs !== 0

	// Neither a deadline nor an external signal: run the body directly, but keep the
	// rejection symmetric with the deadline path. A rejecting body is an adapter bug
	// (the body contract is to resolve TResult), so report it as 'tx' - never a 'query'
	// the caller might retry, and never a bare rejection.
	if (!hasDeadline && !external) {
		return body(controller.signal, () => {}).then(
			(r) => r,
			(err) => ({ ok: false, error: toDbError(err, 'tx') }),
		)
	}

	// An already-aborted external signal settles the race at once without running the
	// body - its abort event has already fired and will not fire again.
	if (external?.aborted) {
		return Promise.resolve({ ok: false, error: abortedError(external.reason) })
	}

	return new Promise<TResult<T, TDbError>>((resolve) => {
		let settled = false
		let timer: ReturnType<typeof setTimeout> | undefined
		// Lifts the main deadline before the COMMIT teardown and, in its place, arms a
		// "hard" timer of the same duration that bounds the commit phase itself. A deadline
		// firing mid-COMMIT must not report 'timeout' (the server may already have
		// committed - a retry would double-apply), so this hard timer reports an explicit
		// ambiguous outcome (kind 'tx', code 'COMMIT_TIMEOUT') instead. The external abort
		// signal is NOT cleared - the caller can still abort the transaction.
		const armCommitDeadline = (): void => {
			// The race is already settled (the deadline or an external abort fired while cb
			// was still running): do not arm a fresh timer - cleanup() has run, so it would
			// outlive the call, keep the event loop alive for timeoutMs and re-abort a dead
			// controller.
			if (settled) return
			if (timer) clearTimeout(timer)
			if (!hasDeadline) { timer = undefined; return }
			timer = setTimeout(() => {
				controller.abort(new CommitTimeout())
				settle({ ok: false, error: { message: 'COMMIT outcome unknown', kind: 'tx', code: 'COMMIT_TIMEOUT' } })
			}, timeoutMs)
		}
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
				controller.abort(new TxTimeout())
				settle({ ok: false, error: { message: 'TIMEOUT', kind: 'timeout', code: 'TIMEOUT' } })
			}, timeoutMs)
		}
		external?.addEventListener('abort', onExternalAbort, { once: true })
		body(controller.signal, armCommitDeadline).then(
			(r) => settle(r),
			// The body itself rejected - an adapter bug (the body contract is to resolve
			// TResult), not a deadline and not a query to retry. Report 'tx' so it is not
			// mistaken for a retryable query error and the underlying cause stays visible.
			(err) => settle({ ok: false, error: toDbError(err, 'tx') }),
		)
	})
}

function abortedError(reason: unknown): TDbError {
	return {
		message: reason instanceof Error ? reason.message : String(reason ?? 'aborted'),
		kind: 'aborted',
		// A synthetic abort carries an identifier even when the reason was not an Error
		// (no stack to fall back on), so isDbError() still recognizes it as a structured
		// error instead of re-labelling it 'user'.
		code: 'ABORTED',
		...(reason instanceof Error ? { stack: reason.stack } : {}),
	}
}

// The internal transaction deadline aborts with this private reason. Identifying the
// deadline by type (not by the string 'TIMEOUT') keeps an external
// controller.abort(new Error('TIMEOUT')) from being mislabelled 'timeout': that is an
// external abort and must report kind 'aborted'.
class TxTimeout extends Error {
	constructor() { super('TIMEOUT') }
}

// The "hard" commit timer (see armCommitDeadline) aborts with this reason. It must be
// distinguishable from TxTimeout so the resulting error reports an ambiguous commit
// outcome (kind 'tx', code 'COMMIT_TIMEOUT') rather than a plain timeout.
class CommitTimeout extends Error {
	constructor() { super('COMMIT outcome unknown') }
}

// Maps an AbortSignal's reason to a TDbError. The internal transaction deadline aborts
// with a TxTimeout (see withTxTimeout), so that case reports kind 'timeout'; every other
// reason is an external abort and reports kind 'aborted'. The adapters call this from
// their abort handlers and early-abort branches, so an external abort is never
// mislabelled 'timeout' and an Error reason is never stringified into "Error: <message>".
export function abortError(reason: unknown): TDbError {
	if (reason instanceof TxTimeout) {
		return { message: 'TIMEOUT', kind: 'timeout', code: 'TIMEOUT' }
	}
	if (reason instanceof CommitTimeout) {
		return { message: 'COMMIT outcome unknown', kind: 'tx', code: 'COMMIT_TIMEOUT' }
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
