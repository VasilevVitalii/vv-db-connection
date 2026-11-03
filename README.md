# vv-db-connection

A single connection layer over **MSSQL**, **Oracle**, **PostgreSQL** and **Firebird**. One `DbConnection` class, one unified query interface — the rest of the application never touches driver-specific code.

```bash
npm i vv-db-connection
```

The package is **ESM-only** (`"type": "module"`, no CommonJS `require`) and requires **Node.js ≥ 18** (the query-abort path uses `AbortSignal.reason`).

## Supported databases

| `kind`    | Driver          | Default port |
|-----------|-----------------|--------------|
| `mssql`   | `mssql`         | 1433         |
| `pg`      | `pg`            | 5432         |
| `ora`     | `oracledb`      | 1521         |
| `fb`      | `node-firebird` | 3050         |

## Initialization

```ts
import { DbConnection } from 'vv-db-connection'
import type { TConnectionParam } from 'vv-db-connection'

const connection: TConnectionParam = {
  kind: 'pg',
  host: 'localhost',
  port: 5432,
  database: 'app',
  user: 'admin',
  password: 'secret',
}

const db = new DbConnection(connection)
```

`new DbConnection(connection)` picks the adapter from `connection.kind`. The connection is passed as a plain object — its schema and types are owned by this package. Create as many connections as you need, in parallel, each with its own driver:

```ts
const src = new DbConnection(mssqlConnection) // source
const dst = new DbConnection(pgConnection)     // target
```

The connection is opened lazily — `new DbConnection(...)` only holds the config; the first query opens the driver's connection pool, which is then reused across calls. Call `await db.close()` to release it when you are done (Firebird holds nothing between calls).

### Connection fields

| `kind`    | Type         | Fields                                                                        |
|-----------|--------------|-------------------------------------------------------------------------------|
| `mssql` | `TConnMssql` | `server`, `port?`, `database?`, `auth`, `trustServerCertificate?`, `encrypt?`, `ca?`, `connectTimeout?` |
| `pg`    | `TConnPg`    | `host`, `port?`, `database`, `user`, `password`, `ssl?: boolean \| { ca?, trustServerCertificate? }`, `connectTimeout?` |
| `ora`   | `TConnOra`   | `host`, `port?`, `service`, `user`, `password`, `privilege?`, `connectTimeout?` |
| `fb`    | `TConnFb`    | `host`, `port?`, `database`, `user`, `password`, `charset?`, `numericMode?`, `connectTimeout?`  |

MSSQL `auth` is a union: `{ kind: 'mssql', user, password }` or `{ kind: 'ntlm', domain, user, password }`.

Oracle `privilege` is one of `SYSDBA | SYSOPER | SYSASM | SYSBACKUP | SYSDG | SYSKM | SYSRAC`; the connection string is assembled internally as `host:port/service`. Privileged connections are only supported by the pool in **Thin mode** (the default — no Oracle Client installed). In Thick mode (after `oracledb.initOracleClient()`) a pool cannot be privileged, so the adapter fails loudly instead of handing back a connection that is not actually privileged.

Firebird `charset` is `'UTF8'` (default) or `'WIN1251'`; `numericMode` is `'lossy'` (default) | `'safe'` | `'string'` and controls how `BIGINT`/`INT128` and fixed-point `NUMERIC`/`DECIMAL` results are decoded (see Data types).

`connectTimeout` (mssql/pg/ora/fb, milliseconds) bounds how long establishing a connection may take; without it a driver can hang for minutes against an unreachable host. Firebird's is implemented by the adapter — node-firebird itself has no attach timeout, so the adapter rips the socket of a stuck TCP connect / handshake after the deadline.

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
    console.log(resultSet.schema)       // TDbCol[]
    console.log(resultSet.rows)         // Record<string, unknown>[]
    console.log(resultSet.rowsAffected) // number | undefined — DML only
  }
}
```

Collects all rows into memory and returns `TResult<TDbRes[], TDbError>`. Each element of the array is one result set (relevant for multi-statement queries on MSSQL). A DML statement (INSERT/UPDATE/DELETE) produces one result set with an empty `schema`, no rows, and `rowsAffected` set to how many rows the database actually changed; a `SELECT` leaves `rowsAffected` unset.

### `execStream` — streaming query

```ts
await db.execStream(query, params, (event) => {
  if (event.kind === 'schema')       { /* event.schema: TDbCol[] */ }
  if (event.kind === 'row')          { /* event.row: Record<string, unknown> */ }
  if (event.kind === 'rowsAffected') { /* event.count: number — DML only */ }
  if (event.kind === 'finish')       { /* event.error?: TDbError */ }
}, { firstDataTimeoutMs: 30_000, idleTimeoutMs: 30_000 })
```

Fires the callback incrementally as data arrives — for large result sets where buffering everything is undesirable. A `SELECT` emits `schema` → `row` × N → `finish`; a DML statement (INSERT/UPDATE/DELETE) emits `schema` (empty) → `rowsAffected` → `finish` — the count is how many rows the database actually changed, never a row. DDL (CREATE/DROP/ALTER) emits an empty `schema` and `finish` with no `rowsAffected`, since the database reports no count for it.

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
}, { timeoutMs, signal })  // or a bare timeoutMs number (legacy)

// result: TResult<void, TDbError>
```

Opens a transaction, hands the callback an `exec` bound to it and an `AbortSignal`, and commits once the callback resolves. The signal aborts when the timeout expires or the external `signal` is aborted — check it between long-running steps to stop your own work early. Anything thrown inside rolls back and comes out as `{ ok: false, error }`, where `error` is a `TDbError`. Statements use the same `:name` parameters, and every call returns `{ rows }`, so a value generated inside the transaction can be read back before commit.

A failed transaction reports which side failed via `error.kind`: `connect` (getting the connection), `tx` (`BEGIN`/commit), `query` (a statement — even one whose error bubbles through the callback), or `user` (the callback threw its own error). Telling `user` from `query` matters for retry logic: retry a deadlock, not a bug in your code.

`execTxDynamic` takes either a legacy positional `timeoutMs` number or a `TTxOptions` object `{ timeoutMs?, signal? }` (`0` or omitted — no deadline). On expiry the signal aborts, the connection is torn down, and the call resolves as `{ ok: false, error: { message: 'TIMEOUT', kind: 'timeout' } }`; an external `signal` cancels the transaction from outside and ends it as `{ ok: false, error: { ..., kind: 'aborted' } }`, mirroring `exec`.

A connection is taken from the pool for the transaction and released back afterwards, on the commit and rollback paths alike. Firebird opens its own attach (it has no pool). Firebird runs with `READ COMMITTED` isolation; the other adapters use the driver default.

Statements inside one transaction are not safe to run in parallel: `exec` is bound to a single connection, and the adapters behave differently — MSSQL rejects a second concurrent request («another request in progress»), PostgreSQL queues it, Oracle serializes it. `await` each `exec` before starting the next.

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

A parameter's SQL type is normally inferred from its JS `value`. To pin the exact type, pass `type` with a driver-native type name:

```ts
{ name: 'at', value: new Date(), type: 'DateTime2' } // mssql: DateTime2, not the precision-losing DateTime
{ name: 'n',  value: '0123',      type: 'NUMBER' }    // oracle: number, not a string
```

`type` is honoured by MSSQL (`sql.TYPES` names) and Oracle (`VARCHAR2`, `NVARCHAR2`, `CHAR`, `NUMBER`, `DATE`, `TIMESTAMP`, `CLOB`, `BLOB`, `RAW`, `BOOLEAN`, `JSON`). PostgreSQL and Firebird infer from the JS value and ignore it. An unknown type name throws before the query is sent.

`direction` makes a bind `out`/`inout`. Only Oracle supports it: `oracledb` maps it to `BIND_OUT`/`BIND_INOUT` and the returned value lands in `TDbRes.outParams` (keyed by parameter name). A pure `out` parameter omits `value` (there is nothing to send); `inout` keeps it as the input value. Both need `type` — the driver has no type to allocate the out buffer with — and the other adapters reject the direction rather than silently binding `undefined`.

Firebird uploads a `Buffer` (or string) value to a `BLOB` column automatically and sends a JS `bigint` as `INT128`, so LOBs and 64-bit integers work with no extra setup on the way in. Reading a `BIGINT`/`INT128` beyond 2^53 back exactly needs `numericMode: 'string'` (or `'safe'`) on the connection; the default `'lossy'` decodes through `Number` and silently rounds.

## Data types

```ts
type TDbErrorKind = 'connect' | 'query' | 'tx' | 'user' | 'timeout' | 'aborted'
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
  rowsAffected?: number // how many rows a DML statement changed; unset for SELECT
  outParams?: Record<string, unknown> // Oracle out/inout bind values, keyed by param name
}

type TDbStreamEvent =
  | { kind: 'schema'; schema: TDbCol[] }
  | { kind: 'row'; row: Record<string, unknown> }
  | { kind: 'rowsAffected'; count: number }
  | { kind: 'outParams'; params: Record<string, unknown> }
  | { kind: 'finish'; error?: TDbError }

type TExecOptions = {
  signal?: AbortSignal
  firstDataTimeoutMs?: number
  idleTimeoutMs?: number
}

type TDbTxResult = { rows: Record<string, unknown>[]; outParams?: Record<string, unknown> }
type TDbTxExec   = (query: string, params: TDbParam[]) => Promise<TDbTxResult>
type TDbTxCb     = (exec: TDbTxExec, signal: AbortSignal) => Promise<void>
```

Every failure is a `TDbError`: `kind` says what happened (`connect` — could not open the connection, `query` — the query failed, `timeout` — no data in time, `aborted` — an external `signal` fired), `code` carries the driver's own error code, and `message` the human text. Failures are told apart by `code`/`kind`, not by parsing `message`.

Unlike `exec`, a statement inside a transaction returns a single set of rows — no schema and no multi-result-set handling.

## Adapter notes

**MSSQL** — `request.stream = true`, rows delivered via events without driver-side buffering. One `ConnectionPool` per `DbConnection`, created lazily and reused across queries.

**PostgreSQL** — a `pg.Pool` per `DbConnection`. Parameterized queries stream rows via `pg.Query` (`'row'` event) without buffering; a multi-statement string has no parameters and goes through the simple query protocol, which buffers. A backend does not notice its client is gone until it has something to send, so an aborted query is cancelled from a connection of its own (`pg_cancel_backend`).

**Oracle** — `conn.queryStream()` with `OUT_FORMAT_OBJECT` for `SELECT`, rows arrive as plain objects; DML and PL/SQL go through `conn.execute()` (auto-committed). Connections come from an `oracledb` pool (`createPool`) and are released back in a `finally` block.

**Firebird** — `node-firebird` has no streaming API; the adapter drives the low-level `Transaction`/`Statement` wire API directly, so a single prepared statement serves the column schema, the streamed rows and the affected-row count (the high-level `sequentially()` re-prepares internally and cannot do all three). The column schema comes from the statement's describe (`newStatement`), so it carries the real driver type names and is emitted even for an empty result — never inferred from the first row. DML reports `rowsAffected` from the statement's record counts (`isc_info_sql_records`). Text blobs are fetched as text (`blobAsText`); binary blobs (`SUB_TYPE 0`) are not supported. A lost connection ends the query with `fb: connection lost`: on a closed socket the driver would quietly attach anew and leave the read waiting for an answer that never comes, so that reconnect is cancelled and the driver's `error` / `reconnect` events are taken as the same loss.

The Firebird adapter patches one `node-firebird` internal (the wire-protocol `attach`) to stop a failed attach from leaking the TCP socket — see the note block at the top of `src/db/adapters/fb.ts` for the full rationale.

## License

MIT
