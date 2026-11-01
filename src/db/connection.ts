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
}

export type TConnPg = {
	kind: 'pg'
	host: string
	port?: number
	database: string
	user: string
	password: string
	ssl?: boolean
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
}

export type TConnFb = {
	kind: 'fb'
	host: string
	port?: number
	database: string
	user: string
	password: string
	charset?: 'UTF8' | 'WIN1251'
}

// The discriminated union over all supported connections; its "kind" is the single
// source for the list of databases this package talks to.
export type TConnectionParam = TConnMssql | TConnOra | TConnPg | TConnFb
export type TDbKind = TConnectionParam['kind']
