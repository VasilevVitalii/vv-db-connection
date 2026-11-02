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
