import type { TResult } from '../tresult'
import type { TDbKind } from './connection'

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
	// Forces a driver-native type name (mssql: 'DateTime2', 'NVarChar', ...; oracle:
	// 'NUMBER', 'VARCHAR2', 'DATE', 'CLOB', ...). Forcing it pins the exact bind type - e.g.
	// a Date would otherwise arrive as mssql DateTime (losing sub-second precision) and a
	// long string as NVarChar. firebird and postgres infer from the JS value and ignore `type`.
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
}

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

export type TDbTxResult = { rows: Record<string, unknown>[]; outParams?: Record<string, unknown> }
export type TDbTxExec   = (query: string, params: TDbParam[]) => Promise<TDbTxResult>
export type TDbTxCb     = (exec: TDbTxExec, signal: AbortSignal) => Promise<void>

export interface IDbAdapter {
	kind(): TDbKind
	title(): string
	user(): string
	versionQuery(): string
	pingQuery(): string
	// signal: the caller has given up on the query - tear the connection down without
	// waiting for the server. Events that still come after it are not looked at.
	execStream(query: string, params: TDbParam[], callback: (event: TDbStreamEvent) => void, signal?: AbortSignal): Promise<void>
	execTxDynamic(cb: TDbTxCb, timeoutMs?: number, signal?: AbortSignal): Promise<TResult<void, TDbError>>
	close(): Promise<void>
}
