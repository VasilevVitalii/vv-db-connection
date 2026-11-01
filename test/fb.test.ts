import { loadConnections, reachable, runDbSuite, connHost, connPort } from './helpers'

const conns = await loadConnections()
const conn = conns?.fb ?? null
const skip = conn
	? ((await reachable(connHost(conn), connPort(conn))) ? false : 'fb: host unreachable')
	: 'fb: .connect.json missing'

runDbSuite('fb', conn, 'SELECT :v AS v FROM rdb$database', skip)
