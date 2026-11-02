import type { TResult } from '../tresult'
import type { TDbKind } from './connection'

export type TDbParam = { name: string; value: unknown }

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
}

// A structured error: `kind` tells what happened, `code` keeps the driver's own code
// (SQLSTATE for pg, ORA-xxxxx / NJS-xxxx for oracle, SQL Server number for mssql,
// gdscode for firebird), and `message` is the human text. Two failures are told apart by
// `code`/`kind`, not by parsing `message`.
export type TDbErrorKind = 'connect' | 'query' | 'timeout' | 'aborted'
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

export type TDbStreamEvent =
	| { kind: 'schema'; schema: TDbCol[] }
	| { kind: 'row'; row: Record<string, unknown> }
	| { kind: 'finish'; error?: TDbError }

export type TDbTxResult = { rows: Record<string, unknown>[] }
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
	execTxDynamic(cb: TDbTxCb, timeoutMs?: number): Promise<TResult<void, TDbError>>
	close(): Promise<void>
}
