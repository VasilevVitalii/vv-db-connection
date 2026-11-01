import type { TResult } from '../tresult'

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
	rows: object[]
}

export type TDbStreamEvent =
	| { kind: 'schema'; schema: TDbCol[] }
	| { kind: 'row'; row: object }
	| { kind: 'finish'; error?: string }

export type TDbTxResult = { rows: Record<string, unknown>[] }
export type TDbTxExec   = (query: string, params: TDbParam[]) => Promise<TDbTxResult>
export type TDbTxCb     = (exec: TDbTxExec) => Promise<void>

export interface IDbAdapter {
	kind(): 'pg' | 'mssql' | 'ora' | 'fb'
	title(): string
	user(): string
	versionQuery(): string
	pingQuery(): string
	// signal: the caller has given up on the query - tear the connection down without
	// waiting for the server. Events that still come after it are not looked at.
	execStream(query: string, params: TDbParam[], callback: (event: TDbStreamEvent) => void, signal?: AbortSignal): Promise<void>
	execTxDynamic(cb: TDbTxCb, timeoutMs?: number): Promise<TResult<void>>
}
