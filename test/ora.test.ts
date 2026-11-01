import { loadConnections, reachable, runDbSuite, connHost, connPort } from './helpers'

const conns = await loadConnections()
const conn = conns?.ora ?? null
const skip = conn
	? ((await reachable(connHost(conn), connPort(conn))) ? false : 'ora: host unreachable')
	: 'ora: .connect.json missing'

runDbSuite('ora', conn, 'SELECT :v AS v FROM dual', skip)
