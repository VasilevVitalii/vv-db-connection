import oracledb from 'oracledb'
import type { IDbAdapter, TDbCol, TDbParam, TDbStreamEvent, TDbTxCb, TDbTxResult } from '../types'
import type { TConnOra } from '../connection'
import type { TResult } from '../../tresult'

type OraMetadata = { name: string; dbTypeName: string; byteSize?: number; scale?: number; precision?: number }

function toSchema(meta: OraMetadata[]): TDbCol[] {
	return meta.map((m) => ({
		name:      m.name,
		kind:      'unknown',
		len:       m.byteSize,
		scale:     m.scale,
		precision: m.precision,
	}))
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
		}
	}

	async execStream(query: string, params: TDbParam[], callback: (event: TDbStreamEvent) => void, signal?: AbortSignal): Promise<void> {
		const conn = await oracledb.getConnection(this._connParams())
		let onAbort: (() => void) | undefined
		try {
			type OraBindValue = oracledb.BindParameter | string | number | bigint | Date | Buffer | null | undefined
			const binds: Record<string, OraBindValue> = {}
			for (const p of params) {
				binds[p.name] = p.value as OraBindValue
			}
			const stream = conn.queryStream(query, binds, { outFormat: oracledb.OUT_FORMAT_OBJECT })
			await new Promise<void>((resolve) => {
				let done = false
				const finish = (error?: string) => {
					if (done) return
					done = true
					callback({ kind: 'finish', error })
					resolve()
				}
				stream.on('metadata', (meta: OraMetadata[]) => {
					callback({ kind: 'schema', schema: toSchema(meta) })
				})
				stream.on('data', (row: object) => {
					callback({ kind: 'row', row })
				})
				stream.on('error', (err: Error) => finish(String(err)))
				stream.on('end', () => finish())
				// break() interrupts the call running on the server; the stream is destroyed
				// in case the server does not answer even that.
				onAbort = () => {
					void conn.break().catch(() => {})
					stream.destroy()
					finish(String(signal?.reason ?? 'aborted'))
				}
				if (signal?.aborted) onAbort()
				else signal?.addEventListener('abort', onAbort)
			})
		} finally {
			if (onAbort) signal?.removeEventListener('abort', onAbort)
			try { await conn.close() } catch {}
		}
	}

	async execTxDynamic(cb: TDbTxCb, timeoutMs?: number): Promise<TResult<void>> {
		const conn = await oracledb.getConnection(this._connParams())
		const txWork = async (): Promise<TResult<void>> => {
			try {
				const exec = (query: string, params: TDbParam[]): Promise<TDbTxResult> => {
					type OraBindValue = oracledb.BindParameter | string | number | bigint | Date | Buffer | null | undefined
					const binds: Record<string, OraBindValue> = {}
					for (const p of params) binds[p.name] = p.value as OraBindValue
					return new Promise<TDbTxResult>((resolve, reject) => {
						const onUncaught = (err: Error) => reject(err)
						process.once('uncaughtException', onUncaught)
						conn.execute<Record<string, unknown>>(query, binds, { autoCommit: false, outFormat: oracledb.OUT_FORMAT_OBJECT })
							.then((res) => { process.removeListener('uncaughtException', onUncaught); resolve({ rows: (res.rows ?? []) as Record<string, unknown>[] }) })
							.catch((err) => { process.removeListener('uncaughtException', onUncaught); reject(err) })
					})
				}
				await cb(exec)
				await conn.commit()
				return { ok: true, result: undefined }
			} catch (err) {
				try { await conn.rollback() } catch {}
				return { ok: false, error: String(err) }
			} finally {
				try { await conn.close() } catch {}
			}
		}
		if (!timeoutMs) return txWork()
		let timer: ReturnType<typeof setTimeout> | undefined
		const timeoutPromise = new Promise<TResult<void>>((resolve) => {
			timer = setTimeout(() => {
				void conn.close().catch(() => {})
				resolve({ ok: false, error: 'TIMEOUT' })
			}, timeoutMs)
		})
		try {
			return await Promise.race([txWork(), timeoutPromise])
		} finally {
			clearTimeout(timer)
		}
	}
}
