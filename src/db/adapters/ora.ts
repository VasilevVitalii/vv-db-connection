import oracledb from 'oracledb'
import type { IDbAdapter, TDbCol, TDbError, TDbParam, TDbStreamEvent, TDbTxCb, TDbTxResult } from '../types'
import type { TConnOra } from '../connection'
import type { TResult } from '../../tresult'
import { mapPlaceholders } from '../placeholders'
import { toDbError, isDbError, withTxTimeout } from '../helpers'

type OraMetadata = { name: string; dbTypeName: string; byteSize?: number; scale?: number; precision?: number }
type OraBindValue = oracledb.BindParameter | string | number | bigint | Date | Buffer | null | undefined

// A forced parameter type (TDbParam.type) maps a driver-native type name ('NUMBER',
// 'VARCHAR2', 'DATE', 'CLOB', ...) to the oracledb DbType. Without it the driver infers
// the type from the JS value; forcing it pins the exact bind type. Unknown names throw
// before the query is sent rather than failing server-side.
const ORA_IN_TYPES: Record<string, any> = {
	VARCHAR2:  oracledb.DB_TYPE_VARCHAR,
	NVARCHAR2: oracledb.DB_TYPE_NVARCHAR,
	CHAR:      oracledb.DB_TYPE_CHAR,
	NUMBER:    oracledb.DB_TYPE_NUMBER,
	DATE:      oracledb.DB_TYPE_DATE,
	TIMESTAMP: oracledb.DB_TYPE_TIMESTAMP,
	CLOB:      oracledb.DB_TYPE_CLOB,
	BLOB:      oracledb.DB_TYPE_BLOB,
	RAW:       oracledb.DB_TYPE_RAW,
	BOOLEAN:   oracledb.DB_TYPE_BOOLEAN,
	JSON:      oracledb.DB_TYPE_JSON,
}

export function oraBind(p: TDbParam): OraBindValue {
	const dir = p.direction === 'out' ? oracledb.BIND_OUT : p.direction === 'inout' ? oracledb.BIND_INOUT : oracledb.BIND_IN
	if (p.type === undefined) {
		// OUT/INOUT have no server-side type to allocate the out buffer from - the caller
		// must force one, or the driver answers NJS-0058 for the missing type at execute time.
		if (dir !== oracledb.BIND_IN) throw new Error(`ora: ${p.direction} parameter ${p.name} needs a type`)
		return p.value as OraBindValue
	}
	const t = ORA_IN_TYPES[p.type]
	if (t === undefined) throw new Error(`ora: unknown param type: ${p.type}`)
	// An OUT bind carries no input value; INOUT keeps the caller's value as its in value.
	return { dir, type: t, val: dir === oracledb.BIND_OUT ? undefined : p.value } as OraBindValue
}

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
// answers NJS-098 on a bind that has no placeholder. A placeholder with no matching
// parameter is rejected here - otherwise Oracle would answer ORA-01008 from the server
// instead of a clear client-side "param not found" like pg and fb give.
function buildQuery(query: string, params: TDbParam[]): { sql: string; binds: Record<string, OraBindValue> } {
	const used = new Set<string>()
	const sql = mapPlaceholders(query, (name) => { used.add(name); return `:${name}` })
	for (const name of used) {
		if (!params.some((p) => p.name === name)) throw new Error(`ora: param not found: ${name}`)
	}
	const binds: Record<string, OraBindValue> = {}
	for (const p of params) {
		if (used.has(p.name)) binds[p.name] = oraBind(p)
	}
	return { sql, binds }
}

// Whether the statement returns rows (SELECT / WITH), ignoring any leading whitespace and
// comments (/* hint */, -- comment). queryStream() only runs a query that returns rows;
// DML and PL/SQL go through execute(), which discards rows - a SELECT hidden behind a
// leading comment used to be misrouted there and return an empty result silently.
function isRowQuery(sql: string): boolean {
	let i = 0
	const n = sql.length
	for (;;) {
		while (i < n && /\s/.test(sql.charAt(i))) i++
		if (sql.startsWith('--', i)) {
			const nl = sql.indexOf('\n', i)
			if (nl < 0) return false
			i = nl + 1
			continue
		}
		if (sql.startsWith('/*', i)) {
			const end = sql.indexOf('*/', i + 2)
			if (end < 0) return false
			i = end + 2
			continue
		}
		break
	}
	return /^(select|with)\b/i.test(sql.slice(i))
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

	private _pool?: Promise<oracledb.Pool>
	private pool(): Promise<oracledb.Pool> {
		if (!this._pool) {
			this._pool = oracledb.createPool({ ...this._connParams(), poolMin: 0, poolMax: 4, poolIncrement: 1 })
		}
		return this._pool
	}

	async close(): Promise<void> {
		// The pool is created lazily behind a promise; close() must wait for an in-flight
		// createPool rather than leaving the eventual pool to leak, and reset the field so
		// a later call creates a fresh one.
		const inFlight = this._pool
		this._pool = undefined
		if (inFlight) {
			try { await (await inFlight).close(0) } catch {}
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
		try {
			const { sql, binds } = buildQuery(query, params)
			const rowQuery = isRowQuery(sql)
			await new Promise<void>((resolve) => {
				let done = false
				let stream: oracledb.QueryStream<any> | undefined
				const finish = (error?: TDbError): void => {
					if (done) return
					done = true
					signal?.removeEventListener('abort', onAbort)
					callback({ kind: 'finish', error })
					resolve()
				}
				// break() interrupts the statement running on the server, whether it is a
				// query being streamed or a long DML/PL-SQL call; the stream (if any) is
				// destroyed in case the server does not answer even that. The DML branch used
				// to skip this, so a slow UPDATE/PL-SQL block kept its connection past the
				// abort until it finished on its own.
				const onAbort = (): void => {
					void conn.break().catch(() => {})
					stream?.destroy()
					finish({ message: String(signal?.reason ?? 'aborted'), kind: 'aborted' })
				}
				if (signal?.aborted) { onAbort(); return }
				signal?.addEventListener('abort', onAbort)

				// queryStream only runs a query that returns rows; DML and PL/SQL have to go
				// through execute(), which commits right away - execStream is not a transaction.
				if (!rowQuery) {
					conn.execute(sql, binds, { autoCommit: true, outFormat: oracledb.OUT_FORMAT_OBJECT }).then(
						(res) => {
							// DML returns no rows: give it an empty schema and the affected
							// row count, so `exec` reports one result per statement.
							callback({ kind: 'schema', schema: [] })
							if (typeof res.rowsAffected === 'number') callback({ kind: 'rowsAffected', count: res.rowsAffected })
							if (res.outBinds) callback({ kind: 'outParams', params: res.outBinds as Record<string, unknown> })
							finish()
						},
						(err) => finish(toDbError(err, 'query')),
					)
					return
				}
				try {
					stream = conn.queryStream(sql, binds, { outFormat: oracledb.OUT_FORMAT_OBJECT })
				} catch (err) {
					finish(toDbError(err, 'query'))
					return
				}
				stream.on('metadata', (meta: OraMetadata[]) => {
					callback({ kind: 'schema', schema: toSchema(meta) })
				})
				stream.on('data', (row: object) => {
					callback({ kind: 'row', row: row as Record<string, unknown> })
				})
				stream.on('error', (err: Error) => finish(toDbError(err, 'query')))
				stream.on('end', () => finish())
			})
		} finally {
			try { await conn.close() } catch {}
		}
	}

	async execTxDynamic(cb: TDbTxCb, timeoutMs?: number, external?: AbortSignal): Promise<TResult<void, TDbError>> {
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
			// getConnection() may have resolved after the deadline already fired: the
			// listener above was added too late to ever run, and the finally below skips
			// close() on an aborted signal. Release the connection here instead of leaking
			// it from the shared pool (poolMax is small enough to exhaust).
			if (signal.aborted) {
				teardown()
				return { ok: false, error: { message: 'TIMEOUT', kind: 'timeout' } }
			}
			try {
				const exec = (query: string, params: TDbParam[]): Promise<TDbTxResult> => {
					const { sql, binds } = buildQuery(query, params)
					return conn.execute<Record<string, unknown>>(sql, binds, { autoCommit: false, outFormat: oracledb.OUT_FORMAT_OBJECT })
						.then(
							(res) => ({
								rows:      (res.rows ?? []) as Record<string, unknown>[],
								outParams: res.outBinds as Record<string, unknown> | undefined,
							}),
							// Tag the failure as a DB query error so it stays 'query' even
							// when it bubbles through the user's callback.
							(err) => { throw toDbError(err, 'query') },
						)
				}
				try {
					await cb(exec, signal)
				} catch (err) {
					try { await conn.rollback() } catch {}
					return { ok: false, error: isDbError(err) ? err : toDbError(err, 'user') }
				}
				try {
					await conn.commit()
				} catch (err) {
					try { await conn.rollback() } catch {}
					return { ok: false, error: toDbError(err, 'tx') }
				}
				return { ok: true, result: undefined }
			} finally {
				signal.removeEventListener('abort', teardown)
				if (!signal.aborted) {
					try { await conn.close() } catch {}
				}
			}
		}, external)
	}
}
