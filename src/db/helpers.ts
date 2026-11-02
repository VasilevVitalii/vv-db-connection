import type { TResult } from '../tresult'
import type { TDbError, TDbErrorKind } from './types'

// Builds a TDbError from whatever a driver throws. Each driver reports its own code in a
// different field (pg: code, mssql: number or code, oracle: code) - all of them are
// picked up so the driver's code survives the generic catch instead of being lost.
export function toDbError(err: unknown, kind: TDbErrorKind): TDbError {
	const e = err as { message?: unknown; code?: unknown; number?: unknown; sqlState?: unknown } | null | undefined
	const code = e?.code ?? e?.number ?? e?.sqlState
	return {
		message: typeof e?.message === 'string' ? e.message : String(err),
		kind,
		...(code != null ? { code: String(code) } : {}),
	}
}

// Runs a transaction body against a deadline. The body receives an AbortSignal that
// fires when the deadline passes; it is the body's job to tear its connection down hard
// on abort (cancel the backend / destroy the socket), so a query stuck on the server does
// not outlive the timeout. connect() lives inside the body, so it is covered by the same
// deadline. A body result that arrives after the deadline is ignored - the race has
// already been settled with a TIMEOUT error.
export function withTxTimeout<T>(
	timeoutMs: number | undefined,
	body: (signal: AbortSignal) => Promise<TResult<T, TDbError>>,
): Promise<TResult<T, TDbError>> {
	const controller = new AbortController()
	if (!timeoutMs) return body(controller.signal)
	return new Promise<TResult<T, TDbError>>((resolve) => {
		const timer = setTimeout(() => {
			controller.abort(new Error('TIMEOUT'))
			resolve({ ok: false, error: { message: 'TIMEOUT', kind: 'timeout' } })
		}, timeoutMs)
		body(controller.signal).then(
			(r) => { clearTimeout(timer); resolve(r) },
			() => { clearTimeout(timer); resolve({ ok: false, error: { message: 'TIMEOUT', kind: 'timeout' } }) },
		)
	})
}
