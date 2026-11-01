import { loadConnections, reachable, runDbSuite, connHost, connPort } from './helpers'

const conns = await loadConnections()
const conn = conns?.pg ?? null
const skip = conn
	? ((await reachable(connHost(conn), connPort(conn))) ? false : 'pg: host unreachable')
	: 'pg: .connect.json missing'

runDbSuite('pg', conn, 'SELECT :v AS v', 'SELECT :v AS v', skip)
