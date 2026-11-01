import { loadConnections, reachable, runDbSuite, connHost, connPort } from './helpers'

const conns = await loadConnections()
const conn = conns?.mssql ?? null
const skip = conn
	? ((await reachable(connHost(conn), connPort(conn))) ? false : 'mssql: host unreachable')
	: 'mssql: .connect.json missing'

runDbSuite('mssql', conn, 'SELECT @v AS v', 'SELECT :v AS v', skip)
