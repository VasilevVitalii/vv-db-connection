// Connection settings for the supported databases. The package owns these types —
// it does not depend on vv-config-conn, so a consumer passes the connection as a
// plain object and its schema lives here.

export type TMssqlAuth =
	| { kind: 'mssql'; user: string; password: string }
	| { kind: 'ntlm'; domain: string; user: string; password: string }

export type TConnMssql = {
	kind: 'mssql'
	server: string
	port?: number
	database?: string
	auth: TMssqlAuth
	trustServerCertificate?: boolean
	encrypt?: boolean
	ca?: string // PEM CA certificate(s); passed to tedious as cryptoCredentialsDetails.ca
	connectTimeout?: number // ms; drives tedious connectionTimeout + tarn acquire/create
}

export type TConnPg = {
	kind: 'pg'
	host: string
	port?: number
	database: string
	user: string
	password: string
	ssl?: boolean | { ca?: string; trustServerCertificate?: boolean }
	connectTimeout?: number // ms; pg connectionTimeoutMillis
}

export type TOraPrivilege = 'SYSDBA' | 'SYSOPER' | 'SYSASM' | 'SYSBACKUP' | 'SYSDG' | 'SYSKM' | 'SYSRAC'

export type TConnOra = {
	kind: 'ora'
	host: string
	port?: number
	service: string
	user: string
	password: string
	privilege?: TOraPrivilege
	connectTimeout?: number // ms; converted to seconds for oracledb connectTimeout
	// oracledb pool sizing; pooled connections default to a minimum of 0 and a maximum of 4.
	poolMin?: number
	poolMax?: number
	poolIncrement?: number
	// ms; how long a getConnection() call waits for a free slot before failing
	// (oracledb queueTimeout, 0 = never).
	queueTimeout?: number
}

export type TConnFb = {
	kind: 'fb'
	host: string
	port?: number
	database: string
	user: string
	password: string
	charset?: 'UTF8' | 'WIN1251'
	// How BIGINT/INT128 and fixed-point NUMERIC/DECIMAL results are exposed:
	// 'lossy' (default) decodes through JS Number and may lose precision beyond
	// 2^53; 'safe' returns a number when exact and a string otherwise; 'string'
	// always returns an exact scaled string. JS bigint inputs are sent as INT128.
	numericMode?: 'lossy' | 'safe' | 'string'
	// ms; rips the socket of a TCP connect / handshake / attach that is still stuck.
	// node-firebird has no attach timeout of its own, so a firewalled host (SYN dropped
	// rather than refused) would otherwise hang the attach for the OS's TCP timeout.
	connectTimeout?: number
}

// The discriminated union over all supported connections; its "kind" is the single
// source for the list of databases this package talks to.
export type TConnectionParam = TConnMssql | TConnOra | TConnPg | TConnFb
export type TDbKind = TConnectionParam['kind']

// Static connection metadata, computed straight from the connection settings. DbConnection
// serves these synchronously without loading a driver module, so they must not depend on
// the adapter.
export function dbTitle(c: TConnectionParam): string {
	switch (c.kind) {
		case 'mssql': return `${c.server}:${c.port ?? 1433} [${c.database ?? 'default'}]`
		case 'pg':    return `${c.host}:${c.port ?? 5432} [${c.database}]`
		case 'ora':   return `${c.host}:${c.port ?? 1521} [${c.service}]`
		case 'fb':    return `${c.host}:${c.port ?? 3050} [${c.database}]`
	}
}

export function dbUser(c: TConnectionParam): string {
	switch (c.kind) {
		case 'mssql': return c.auth.kind === 'ntlm' ? `${c.auth.domain}\\${c.auth.user}` : c.auth.user
		case 'pg':    return c.user
		case 'ora':   return c.user
		case 'fb':    return c.user
	}
}
