import oracledb from 'oracledb'
import type { IDbAdapter, TDbCol, TDbError, TDbParam, TDbStreamEvent, TDbTxCb, TDbTxResult } from '../types'
import type { TConnOra } from '../connection'
import type { TResult } from '../../tresult'
import { mapPlaceholders } from '../placeholders'
import { toDbError, withTxTimeout } from '../helpers'

type OraMetadata = { name: string; dbTypeName: string; byteSize?: number; scale?: number; precision?: number }
type OraBindValue = oracledb.BindParameter | string | number | bigint | Date | Buffer | null | undefined

function toSchema(meta: OraMetadata[]): TDbCol[] {
	return meta.map((m) => ({
		name:      m.name,
		kind:      m.dbTypeName,
		len:       m.byteSize,
		scale:     m.scale,
		precision: m.precision,
	}))
}

// :name stays :name for the driver, but only the bindings the SQL actually mentions are
// passed: the other adapters ignore extra parameters, and so must Oracle, or the driver
// answers NJS-098 on a bind that has no placeholder.
function buildQuery(query: string, params: TDbParam[]): { sql: string; binds: Record<string, OraBindValue> } {
	const used = new Set<string>()
	const sql = mapPlaceholders(query, (name) => { used.add(name); return `:${name}` })
	const binds: Record<string, OraBindValue> = {}
	for (const p of params) {
		if (used.has(p.name)) binds[p.name] = p.value as OraBindValue
	}
	return { sql, binds }
}

export class OraAdapter implements IDbAdapter {
	constructor(private readonly p: TConnOra) {}

	kind(): 'ora' { return 'ora' }
	title(): string { return `${this.p.host}:${this.p.port || 'DEFAULT'} [${this.p.service}]` }
	user(): string { return this.p.user }
	versionQuery(): string { return 'SELECT banner AS ver FROM v$version WHERE rownum = 1' }
	pingQuery(): string { return 'SELECT 1 AS p FROM dual' }

	private _connParams() {
		const privilegeMap: Record<string, number> = {
			SYSDBA:    oracledb.SYSDBA,
			SYSOPER:   oracledb.SYSOPER,
			SYSASM:    oracledb.SYSASM,
			SYSBACKUP: oracledb.SYSBACKUP,
			SYSDG:     oracledb.SYSDG,
			SYSKM:     oracledb.SYSKM,
			SYSRAC:    oracledb.SYSRAC,
		}
		return {
			connectString: `${this.p.host}:${this.p.port ?? 1521}/${this.p.service}`,
			user:      this.p.user,
			password:  this.p.password,
			...(this.p.privilege && { privilege: privilegeMap[this.p.privilege] }),
			...(this.p.connectTimeout ? { connectTimeout: Math.ceil(this.p.connectTimeout / 1000) } : {}),
		}
	}

	private _pool?: oracledb.Pool
	private async pool(): Promise<oracledb.Pool> {
		if (!this._pool) {
			this._pool = await oracledb.createPool({ ...this._connParams(), poolMin: 0, poolMax: 4, poolIncrement: 1 })
		}
		return this._pool
	}

	async close(): Promise<void> {
		if (this._pool) {
			try { await this._pool.close(0) } catch {}
			this._pool = undefined
		}
	}

	async execStream(query: string, params: TDbParam[], callback: (event: TDbStreamEvent) => void, signal?: AbortSignal): Promise<void> {
		let conn: oracledb.Connection
		try {
			conn = await (await this.pool()).getConnection()
		} catch (err) {
			callback({ kind: 'finish', error: toDbError(err, 'connect') })
			return
		}
		let onAbort: (() => void) | undefined
		try {
			const { sql, binds } = buildQuery(query, params)
			// queryStream only runs a query that returns rows; DML and PL/SQL have to go
			// through execute(), which commits right away - execStream is not a transaction.
			if (!/^\s*(select|with)\b/i.test(sql)) {
				try {
					await conn.execute(sql, binds, { autoCommit: true, outFormat: oracledb.OUT_FORMAT_OBJECT })
					callback({ kind: 'finish' })
				} catch (err) {
					callback({ kind: 'finish', error: toDbError(err, 'query') })
				}
				return
			}
			const stream = conn.queryStream(sql, binds, { outFormat: oracledb.OUT_FORMAT_OBJECT })
			await new Promise<void>((resolve) => {
				let done = false
				const finish = (error?: TDbError) => {
					if (done) return
					done = true
					callback({ kind: 'finish', error })
					resolve()
				}
				stream.on('metadata', (meta: OraMetadata[]) => {
					callback({ kind: 'schema', schema: toSchema(meta) })
				})
				stream.on('data', (row: object) => {
					callback({ kind: 'row', row: row as Record<string, unknown> })
				})
				stream.on('error', (err: Error) => finish(toDbError(err, 'query')))
				stream.on('end', () => finish())
				// break() interrupts the call running on the server; the stream is destroyed
				// in case the server does not answer even that.
				onAbort = () => {
					void conn.break().catch(() => {})
					stream.destroy()
					finish({ message: String(signal?.reason ?? 'aborted'), kind: 'aborted' })
				}
				if (signal?.aborted) onAbort()
				else signal?.addEventListener('abort', onAbort)
			})
		} finally {
			if (onAbort) signal?.removeEventListener('abort', onAbort)
			try { await conn.close() } catch {}
		}
	}

	async execTxDynamic(cb: TDbTxCb, timeoutMs?: number): Promise<TResult<void, TDbError>> {
		return withTxTimeout(timeoutMs, async (signal) => {
			let conn: oracledb.Connection
			try {
				conn = await (await this.pool()).getConnection()
			} catch (err) {
				return { ok: false, error: toDbError(err, 'connect') }
			}
			// break() interrupts the statement running on the server, close() releases the
			// connection; both fire-and-forget so the race is settled at once.
			const teardown = (): void => {
				void conn.break().catch(() => {})
				void conn.close().catch(() => {})
			}
			signal.addEventListener('abort', teardown, { once: true })
			try {
				const exec = (query: string, params: TDbParam[]): Promise<TDbTxResult> => {
					const { sql, binds } = buildQuery(query, params)
					return conn.execute<Record<string, unknown>>(sql, binds, { autoCommit: false, outFormat: oracledb.OUT_FORMAT_OBJECT })
						.then((res) => ({ rows: (res.rows ?? []) as Record<string, unknown>[] }))
				}
				await cb(exec, signal)
				await conn.commit()
				return { ok: true, result: undefined }
			} catch (err) {
				try { await conn.rollback() } catch {}
				return { ok: false, error: toDbError(err, 'query') }
			} finally {
				signal.removeEventListener('abort', teardown)
				if (!signal.aborted) {
					try { await conn.close() } catch {}
				}
			}
		})
	}
}
