export { createDbConnection, DbConnection } from './db'

export type { TResult } from './tresult'
export type {
	TConnectionParam,
	TDbKind,
	TConnMssql,
	TConnPg,
	TConnOra,
	TConnFb,
	TMssqlAuth,
	TOraPrivilege,
} from './db/connection'
export type {
	TDbParam,
	TDbCol,
	TDbRes,
	TDbStreamEvent,
	TDbError,
	TDbErrorKind,
	TExecOptions,
	TDbTxResult,
	TDbTxExec,
	TDbTxCb,
} from './db/types'
