import type { TResult } from '../tresult'
import type { TConnectionParam } from './connection'
import type { IDbAdapter, TDbParam, TDbRes, TDbStreamEvent, TDbTxCb } from './types'
import { MssqlAdapter } from './adapters/mssql'
import { PgAdapter } from './adapters/pg'
import { OraAdapter } from './adapters/ora'
import { FbAdapter } from './adapters/fb'

// How long a streaming query may stay silent: until the first data after it was sent,
// and between one portion of data and the next.
const FIRST_DATA_TIMEOUT_MS = 12 * 60 * 60 * 1000
const NEXT_ROW_TIMEOUT_MS   = 12 * 60 * 60 * 1000

function hours(ms: number): string {
	return `${ms / 3_600_000} h`
}

export class DbConnection {
	private _adapter!: IDbAdapter

	constructor(connection: TConnectionParam) {
		switch (connection.kind) {
			case 'mssql': this._adapter = new MssqlAdapter(connection); break
			case 'pg':    this._adapter = new PgAdapter(connection);    break
			case 'ora':   this._adapter = new OraAdapter(connection);   break
			case 'fb':    this._adapter = new FbAdapter(connection);    break
		}
	}

	public async exec(query: string, params: TDbParam[]): Promise<TResult<TDbRes[]>> {
		const results: TDbRes[] = []
		let current: TDbRes | undefined
		let execError: string | undefined
		try {
			await this._adapter.execStream(query, params, (event) => {
				if (event.kind === 'schema') {
					current = { schema: event.schema, rows: [] }
					results.push(current)
				} else if (event.kind === 'row') {
					current?.rows.push(event.row)
				} else if (event.kind === 'finish') {
					execError = event.error
				}
			})
		} catch (err) {
			return { ok: false, error: String(err) }
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
	public async execStream(query: string, params: TDbParam[], callback: (event: TDbStreamEvent) => void): Promise<void> {
		const abort = new AbortController()
		let settled = false
		let timer: ReturnType<typeof setTimeout> | undefined
		let giveUp!: () => void
		const gaveUp = new Promise<void>((resolve) => { giveUp = resolve })

		const arm = (ms: number, error: string): void => {
			clearTimeout(timer)
			timer = setTimeout(() => {
				if (settled) return
				settled = true
				abort.abort(new Error(error))
				callback({ kind: 'finish', error })
				giveUp()
			}, ms)
		}
		arm(FIRST_DATA_TIMEOUT_MS, `timeout: no data from the database within ${hours(FIRST_DATA_TIMEOUT_MS)} after the query was sent`)

		const run = this._adapter.execStream(query, params, (event) => {
			if (settled) return
			if (event.kind === 'finish') {
				settled = true
				clearTimeout(timer)
			} else {
				arm(NEXT_ROW_TIMEOUT_MS, `timeout: no new data from the database within ${hours(NEXT_ROW_TIMEOUT_MS)}` )
			}
			callback(event)
		}, abort.signal)

		try {
			await Promise.race([run, gaveUp])
		} finally {
			clearTimeout(timer)
		}
	}

	public async version(): Promise<TResult<string>> {
		const res = await this.exec(this._adapter.versionQuery(), [])
		if (!res.ok) return { ok: false, error: res.error }
		const firstRow = res.result[0]?.rows[0]
		if (!firstRow) return { ok: false, error: 'no version returned' }
		const val = Object.values(firstRow)[0]
		return { ok: true, result: String(val) }
	}

	public kind(): 'pg' | 'mssql' | 'ora' | 'fb' {
		return this._adapter.kind()
	}

	public title(): string {
		return this._adapter.title()
	}

	public user(): string {
		return this._adapter.user()
	}

	public async execTxDynamic(cb: TDbTxCb, timeoutMs?: number): Promise<TResult<void>> {
		return this._adapter.execTxDynamic(cb, timeoutMs)
	}

	public async ping(): Promise<TResult<null>> {
		const res = await this.exec(this._adapter.pingQuery(), [])
		if (!res.ok) return { ok: false, error: res.error }
		return { ok: true, result: null }
	}

}

export function createDbConnection(connection: TConnectionParam): DbConnection {
	return new DbConnection(connection)
}
