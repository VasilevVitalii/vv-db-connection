# vv-db-connection

A single connection layer over **MSSQL**, **Oracle**, **PostgreSQL** and **Firebird**. One `createDbConnection` factory, one unified query interface — the rest of the application never touches driver-specific code.

```bash
npm i vv-db-connection
```

## Supported databases

| `kind`    | Driver          | Default port |
|-----------|-----------------|--------------|
| `mssql`   | `mssql`         | 1433         |
| `pg`      | `pg`            | 5432         |
| `ora`     | `oracledb`      | 1521         |
| `fb`      | `node-firebird` | 3050         |

## Initialization

```ts
import { createDbConnection } from 'vv-db-connection'
import type { TConnectionParam } from 'vv-db-connection'

const connection: TConnectionParam = {
  kind: 'pg',
  host: 'localhost',
  port: 5432,
  database: 'app',
  user: 'admin',
  password: 'secret',
}

const db = createDbConnection(connection)
```

`createDbConnection(connection)` picks the adapter from `connection.kind` and returns a `DbConnection`. The connection is passed as a plain object — its schema and types are owned by this package. Create as many connections as you need, in parallel, each with its own driver:

```ts
const src = createDbConnection(mssqlConnection) // source
const dst = createDbConnection(pgConnection)     // target
```

The connection is opened lazily — `createDbConnection` only holds the config; the first query opens the driver's connection pool, which is then reused across calls. Call `await db.close()` to release it when you are done (Firebird holds nothing between calls).

### Connection fields

| `kind`    | Type         | Fields                                                                        |
|-----------|--------------|-------------------------------------------------------------------------------|
| `mssql` | `TConnMssql` | `server`, `port?`, `database?`, `auth`, `trustServerCertificate?`, `encrypt?`, `ca?`, `connectTimeout?` |
| `pg`    | `TConnPg`    | `host`, `port?`, `database`, `user`, `password`, `ssl?: boolean \| { ca?, trustServerCertificate? }`, `connectTimeout?` |
| `ora`   | `TConnOra`   | `host`, `port?`, `service`, `user`, `password`, `privilege?`, `connectTimeout?` |
| `fb`    | `TConnFb`    | `host`, `port?`, `database`, `user`, `password`, `charset?`                   |

MSSQL `auth` is a union: `{ kind: 'mssql', user, password }` or `{ kind: 'ntlm', domain, user, password }`.

Oracle `privilege` is one of `SYSDBA | SYSOPER | SYSASM | SYSBACKUP | SYSDG | SYSKM | SYSRAC`; the connection string is assembled internally as `host:port/service`.

Firebird `charset` is `'UTF8'` (default) or `'WIN1251'`.

`connectTimeout` (mssql/pg/ora, milliseconds) bounds how long establishing a connection may take; without it a driver can hang for minutes against an unreachable host. Firebird has no such option in the driver.

TLS defaults are deliberately permissive for backward compatibility: MSSQL connects without encryption (`encrypt` defaults to `false`), and PostgreSQL `ssl: true` skips certificate verification. To verify it, pass `ssl: { trustServerCertificate: false, ca: '...' }` for PostgreSQL, or `encrypt: true` plus `ca` for MSSQL. `trustServerCertificate: true` means «accept the server certificate without verifying it» — the same flag both adapters use.

## API

### `exec` — buffered query

```ts
const result = await db.exec('SELECT * FROM orders WHERE status = :status', [
  { name: 'status', value: 'open' },
])

if (!result.ok) {
  // result.error is a TDbError: { message, code?, kind }
  console.error(result.error.message, result.error.code)
} else {
  for (const resultSet of result.result) {
    console.log(resultSet.schema) // TDbCol[]
    console.log(resultSet.rows)   // Record<string, unknown>[]
  }
}
```

Collects all rows into memory and returns `TResult<TDbRes[], TDbError>`. Each element of the array is one result set (relevant for multi-statement queries on MSSQL).

### `execStream` — streaming query

```ts
await db.execStream(query, params, (event) => {
  if (event.kind === 'schema') { /* event.schema: TDbCol[] */ }
  if (event.kind === 'row')    { /* event.row: Record<string, unknown> */ }
  if (event.kind === 'finish') { /* event.error?: TDbError */ }
}, { firstDataTimeoutMs: 30_000, idleTimeoutMs: 30_000 })
```

Fires the callback incrementally as data arrives — for large result sets where buffering everything is undesirable. Events are always emitted in this order: `schema` → `row` × N → `finish`.

A query that goes silent is cut off on every database: no data within `firstDataTimeoutMs` after the query was sent, or no new data within `idleTimeoutMs` after the previous portion, ends it with `finish` carrying a `{ kind: 'timeout', ... }` error, and the call returns at once. The adapter is told to drop its connection (an `AbortSignal`) and is not waited for, so a driver stuck in its own teardown cannot hold the caller. `exec` goes through the same path and has the same limits. Both timeouts default to 12 hours.

`exec` and `execStream` take an optional fourth argument:

```ts
type TExecOptions = {
  signal?: AbortSignal         // cancel the query from outside
  firstDataTimeoutMs?: number  // ms, default 12h
  idleTimeoutMs?: number       // ms, default 12h
}
```

An external `signal` aborts the running query — the call ends with `{ kind: 'aborted', ... }`; a silence timeout ends it with `{ kind: 'timeout', ... }`.

### `execTxDynamic` — transaction

```ts
const result = await db.execTxDynamic(async (exec, signal) => {
  if (signal.aborted) return
  await exec('INSERT INTO t (id) VALUES (:id)', [{ name: 'id', value: 1 }])
  const res = await exec('SELECT MAX(id) AS id FROM t', [])
  // res.rows: Record<string, unknown>[]
}, timeoutMs)

// result: TResult<void, TDbError>
```

Opens a transaction, hands the callback an `exec` bound to it and an `AbortSignal`, and commits once the callback resolves. The signal aborts when the timeout expires — check it between long-running steps to stop your own work early. Anything thrown inside rolls back and comes out as `{ ok: false, error }`, where `error` is a `TDbError`. Statements use the same `:name` parameters, and every call returns `{ rows }`, so a value generated inside the transaction can be read back before commit.

`timeoutMs` is optional (`0` or omitted — no limit). On expiry the signal aborts, the connection is torn down, and the call resolves as `{ ok: false, error: { message: 'TIMEOUT', kind: 'timeout' } }`.

A connection is taken from the pool for the transaction and released back afterwards, on the commit and rollback paths alike. Firebird opens its own attach (it has no pool). Firebird runs with `READ COMMITTED` isolation; the other adapters use the driver default.

### `ping`

```ts
const result = await db.ping() // TResult<null, TDbError>
```

A minimal read-only query that requires no privileges; returns `{ ok: true }` on success. Useful for waiting for the database to come up.

### `version`

```ts
const result = await db.version() // TResult<string, TDbError>
```

Queries the server version string.

### `close`

```ts
await db.close()
```

Releases the driver's connection pool (mssql/pg/ora). Firebird has no pool — each attach is closed by its own query, so `close()` is a no-op there.

### `kind` / `title` / `user`

```ts
db.kind()   // 'pg' | 'mssql' | 'ora' | 'fb'
db.title()  // e.g. "localhost:5432 [mydb]"
db.user()   // e.g. "admin"
```

`kind` is the discriminator of the active connection (`TDbKind`); `title` and `user` are a human-readable description for log output.

## Query parameters

All methods accept named parameters in `:name` syntax, regardless of the underlying driver. Each adapter translates them to the driver's native format:

| Driver     | Native format                        | Notes |
|------------|--------------------------------------|-------|
| MSSQL      | `@name` (mssql `request.input`)      | `:name` converted to `@name` |
| PostgreSQL | `$1, $2, …`                          | same `:name` used multiple times maps to the same `$N` |
| Oracle     | `:name` (native bind)                | passed as a named bind object |
| Firebird   | `?` (positional)                     | replaced in left-to-right order of appearance |

In Firebird a `:name` inside a string literal, a quoted identifier or a comment is left as it is. In an `EXECUTE BLOCK` only the header — everything up to the `AS` that closes it — is treated as placeholders; past that begins PSQL, where `:name` refers to a local variable or a block parameter and is passed through untouched.

## Data types

```ts
type TDbErrorKind = 'connect' | 'query' | 'timeout' | 'aborted'
type TDbError = {
  message: string
  code?: string   // driver code: SQLSTATE (pg), ORA-xxxxx / NJS-xxxx (ora), SQL Server number (mssql), gdscode (fb)
  kind: TDbErrorKind
}

type TDbCol = {
  name: string
  kind: string      // driver-specific type name
  len?: number
  scale?: number
  precision?: number
}

type TDbRes = {
  schema: TDbCol[]
  rows: Record<string, unknown>[]
}

type TDbStreamEvent =
  | { kind: 'schema'; schema: TDbCol[] }
  | { kind: 'row'; row: Record<string, unknown> }
  | { kind: 'finish'; error?: TDbError }

type TExecOptions = {
  signal?: AbortSignal
  firstDataTimeoutMs?: number
  idleTimeoutMs?: number
}

type TDbTxResult = { rows: Record<string, unknown>[] }
type TDbTxExec   = (query: string, params: TDbParam[]) => Promise<TDbTxResult>
type TDbTxCb     = (exec: TDbTxExec, signal: AbortSignal) => Promise<void>
```

Every failure is a `TDbError`: `kind` says what happened (`connect` — could not open the connection, `query` — the query failed, `timeout` — no data in time, `aborted` — an external `signal` fired), `code` carries the driver's own error code, and `message` the human text. Failures are told apart by `code`/`kind`, not by parsing `message`.

Unlike `exec`, a statement inside a transaction returns a single set of rows — no schema and no multi-result-set handling.

## Adapter notes

**MSSQL** — `request.stream = true`, rows delivered via events without driver-side buffering. One `ConnectionPool` per `DbConnection`, created lazily and reused across queries.

**PostgreSQL** — a `pg.Pool` per `DbConnection`. Parameterized queries stream rows via `pg.Query` (`'row'` event) without buffering; a multi-statement string has no parameters and goes through the simple query protocol, which buffers. A backend does not notice its client is gone until it has something to send, so an aborted query is cancelled from a connection of its own (`pg_cancel_backend`).

**Oracle** — `conn.queryStream()` with `OUT_FORMAT_OBJECT` for `SELECT`, rows arrive as plain objects; DML and PL/SQL go through `conn.execute()` (auto-committed). Connections come from an `oracledb` pool (`createPool`) and are released back in a `finally` block.

**Firebird** — `node-firebird` has no streaming API; `db.sequentially()` calls the row callback once per row without loading the full result into memory. The column schema comes from the statement's describe (`newStatement`), so it carries the real driver type names and is emitted even for an empty result — never inferred from the first row. Text blobs are fetched as text (`blobAsText`); binary blobs (`SUB_TYPE 0`) are not supported. A lost connection ends the query with `fb: connection lost`: on a closed socket the driver would quietly attach anew and leave the read waiting for an answer that never comes, so that reconnect is cancelled and the driver's `error` / `reconnect` events are taken as the same loss.

The Firebird adapter patches one `node-firebird` internal (the wire-protocol `attach`) to stop a failed attach from leaking the TCP socket — see the note block at the top of `src/db/adapters/fb.ts` for the full rationale.

## License

MIT
