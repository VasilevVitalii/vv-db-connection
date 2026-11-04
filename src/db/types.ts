// A query/transaction parameter. The driver infers the SQL type from `value` unless
// `type` forces a driver-native type name (mssql: 'DateTime2', 'NVarChar', ...; oracle:
// 'NUMBER', 'VARCHAR2', 'DATE', 'CLOB', ...). Forcing it pins the exact bind type - e.g.
// a Date would otherwise arrive as mssql DateTime (losing sub-second precision) and a
// long string as NVarChar. firebird and postgres infer from the JS value and ignore `type`.
export type TDbParam = {
	name: string
	// The bind value. Omit it for a pure out parameter (direction 'out') - the driver
	// sends nothing on an OUT bind; inout keeps it as the input value.
	value?: unknown
	// Forces a driver-native type name; firebird and postgres infer from the value and
	// ignore it (see the type-level comment above).
	type?: string
	// Bind direction. 'out' / 'inout' are supported only by Oracle (oracledb BIND_OUT /
	// BIND_INOUT) and the returned value lands in TDbRes.outParams; the other adapters
	// throw when they see a non-'in' direction. An out/inout parameter needs `type`, so the
	// driver has a type to allocate the out buffer with.
	direction?: 'in' | 'out' | 'inout'
}

export type TDbCol = {
	name: string
	kind: string
	len?: number
	scale?: number
	precision?: number
}

export type TDbRes = {
	schema: TDbCol[]
	rows: Record<string, unknown>[]
	// DML statements (INSERT/UPDATE/DELETE) carry how many rows the database
	// actually changed; SELECT results leave it unset.
	rowsAffected?: number
	// Oracle out/inout bind values, keyed by parameter name (oracledb outBinds).
	outParams?: Record<string, unknown>
}

// A structured error: `kind` tells what happened, `code` keeps the driver's own code
// (SQLSTATE for pg, ORA-xxxxx / NJS-xxxx for oracle, SQL Server number for mssql,
// gdscode for firebird), and `message` is the human text. Two failures are told apart by
// `code`/`kind`, not by parsing `message`. In a transaction: 'tx' is the begin/commit
// itself failing, 'query' is a statement failing, 'user' is the transaction callback's
// own code throwing - so a retry loop can tell "my bug" from "the database said no".
export type TDbErrorKind = 'connect' | 'query' | 'tx' | 'user' | 'timeout' | 'aborted'
export type TDbError = {
	message: string
	code?: string
	kind: TDbErrorKind
	// The underlying Error's stack, when the failure came from a real Error object (a
	// driver throw). Synthetic errors (a deadline, a missing driver) carry none, so the
	// field stays optional - but a consumer's `catch (e) { e.stack }` now works for a
	// query failure inside a transaction.
	stack?: string
}

// The uniform result envelope for exec/execTxDynamic/version/ping: `ok` discriminates,
// `result` carries the value on success, `error` is a TDbError on failure. The error
// type is generic so a caller can narrow it to a stricter shape. Defined here (not in
// tresult.ts) so types.ts and tresult.ts no longer import each other in a cycle.
export type TResult<T, E = TDbError> = { ok: true; result: T } | { ok: false; error: E }

// Per-call knobs for exec/execStream: an external AbortSignal to cancel the query, and
// how long it may stay silent before the first data and between portions (milliseconds).
export type TExecOptions = {
	signal?: AbortSignal
	firstDataTimeoutMs?: number
	idleTimeoutMs?: number
}

// Per-transaction knobs for execTxDynamic: a deadline and an external AbortSignal. The
// deadline fires the same teardown a timeout does; the signal cancels the transaction
// from outside, exactly as TExecOptions.signal cancels a query.
export type TTxOptions = {
	timeoutMs?: number
	signal?: AbortSignal
}

export type TDbStreamEvent =
	| { kind: 'schema'; schema: TDbCol[] }
	| { kind: 'row'; row: Record<string, unknown> }
	| { kind: 'rowsAffected'; count: number }
	| { kind: 'outParams'; params: Record<string, unknown> }
	| { kind: 'finish'; error?: TDbError }

export type TDbTxResult = {
	rows: Record<string, unknown>[]
	// DML only, like TDbRes.rowsAffected: a SELECT leaves this unset — its row
	// count is not a "rows affected" count.
	rowsAffected?: number
	outParams?: Record<string, unknown>
	// Oracle PL/SQL with DBMS_SQL.RETURN_RESULT returns extra result sets in
	// res.implicitResults; each surfaces as its own schema+rows. The statement's main
	// result stays in `rows`/`rowsAffected` (both empty/undefined for such a block).
	implicitResults?: TDbRes[]
}
// Runs one statement inside the transaction. After the transaction's signal is
// aborted the connection is being torn down hard, so exec() rejects with the abort
// error instead of running on a dead socket - the body can `.catch()` it and bail.
export type TDbTxExec   = (query: string, params: TDbParam[]) => Promise<TDbTxResult>
// The transaction body. Its resolved value is carried out of execTxDynamic as the
// result's `result`, so a value computed inside the transaction (a new id, a computed
// total) does not need a closure to escape. After `signal` is aborted (deadline or
// external) the body MUST stop calling `exec()` - it rejects - and any value it
// resolves is ignored (the race is already settled).
export type TDbTxCb<T = void> = (exec: TDbTxExec, signal: AbortSignal) => Promise<T>

export interface IDbAdapter {
	versionQuery(): string
	// A second query to try when versionQuery() is not reachable (Oracle: v$version is
	// not visible without SELECT_CATALOG_ROLE, but PRODUCT_COMPONENT_VERSION is public).
	versionFallbackQuery?(): string
	pingQuery(): string
	// signal: the caller has given up on the query - tear the connection down without
	// waiting for the server. Events that still come after it are not looked at.
	// Validation errors (unknown placeholders, malformed params) are reported through
	// the callback as a finish with kind 'query', BEFORE the connection is taken.
	// The stream contract is: resolve() only after the callback received exactly one
	// finish; reject() is reserved for adapter/driver crashes the caller did not cause.
	execStream(query: string, params: TDbParam[], callback: (event: TDbStreamEvent) => void, signal?: AbortSignal): Promise<void>
	// Runs cb inside a transaction. The body's exec() rejects once signal is aborted
	// (deadline or external abort), and a body result arriving after abort is ignored.
	execTxDynamic<T = void>(cb: TDbTxCb<T>, timeoutMs?: number, signal?: AbortSignal): Promise<TResult<T, TDbError>>
	close(): Promise<void>
}
