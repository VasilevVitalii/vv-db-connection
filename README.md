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
| `ora`   | `TConnOra`   | `host`, `port?`, `service`, `user`, `password`, `privilege?`, `connectTimeout?`, `poolMin?`, `poolMax?`, `poolIncrement?`, `queueTimeout?` |
| `fb`    | `TConnFb`    | `host`, `port?`, `database`, `user`, `password`, `charset?`, `numericMode?`, `connectTimeout?`  |

MSSQL `auth` is a union: `{ kind: 'mssql', user, password }` or `{ kind: 'ntlm', domain, user, password }`.

Oracle `privilege` is one of `SYSDBA | SYSOPER | SYSASM | SYSBACKUP | SYSDG | SYSKM | SYSRAC`; the connection string is assembled internally as `host:port/service`. Privileged connections are only supported by the pool in **Thin mode** (the default — no Oracle Client installed). In Thick mode (after `oracledb.initOracleClient()`) a pool cannot be privileged, so the adapter fails loudly instead of handing back a connection that is not actually privileged.

Oracle pool sizing defaults to a minimum of 0 and a maximum of 4; raise `poolMax` for more concurrency, and set `queueTimeout` (milliseconds) to bound how long acquiring a connection waits for a free slot instead of the driver default of 60 s. A negative, fractional or `NaN` `poolMin`/`poolMax`/`poolIncrement`/`queueTimeout` (or `poolMin > poolMax`) is a programming error: `new DbConnection(...)` throws a `TypeError` at construction, before the driver is loaded.

Firebird `charset` is `'UTF8'` (default) or `'WIN1251'`; `numericMode` is `'lossy'` (default) | `'safe'` | `'string'` and controls how `BIGINT`/`INT128` and fixed-point `NUMERIC`/`DECIMAL` results are decoded (see Data types).

`connectTimeout` (mssql/pg/ora/fb, milliseconds) bounds how long establishing a connection may take; without it a driver can hang for minutes against an unreachable host. Firebird's is implemented by the adapter — node-firebird itself has no attach timeout, so the adapter rips the socket of a stuck TCP connect / handshake after the deadline. A negative, `NaN` or infinite `connectTimeout` is a programming error: `new DbConnection(...)` throws a `TypeError` at construction, before any driver is loaded.

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

Collects all rows into memory and returns `TResult<TDbRes[], TDbError>`. Each element of the array is one result set. A multi-statement query returns one element per statement on MSSQL and on PostgreSQL *without parameters* (the simple protocol); a parameterized PostgreSQL multi-statement is rejected by the server with an error — the extended protocol used for parameterized queries runs a prepared statement, and PostgreSQL cannot execute more than one command in a prepared statement. A DML statement (INSERT/UPDATE/DELETE) produces one result set with an empty `schema`, no rows, and `rowsAffected` set to how many rows the database actually changed; a `SELECT` leaves `rowsAffected` unset. A DML with a `RETURNING`/`OUTPUT` clause (Firebird `INSERT ... RETURNING`, MSSQL `INSERT ... OUTPUT`) returns its rows instead and may leave `rowsAffected` unset. On Oracle, a PL/SQL block that calls `DBMS_SQL.RETURN_RESULT` returns one result set per implicit result, each with its own `schema` and rows.

### `execStream` — streaming query

```ts
await db.execStream(query, params, (event) => {
  if (event.kind === 'schema')       { /* event.schema: TDbCol[] */ }
  if (event.kind === 'row')          { /* event.row: Record<string, unknown> */ }
  if (event.kind === 'rowsAffected') { /* event.count: number — DML only */ }
  if (event.kind === 'outParams')    { /* event.params: Record<string, unknown> — Oracle only */ }
  if (event.kind === 'finish')       { /* event.error?: TDbError */ }
}, { firstDataTimeoutMs: 30_000, idleTimeoutMs: 30_000 })
```

Fires the callback incrementally as data arrives — for large result sets where buffering everything is undesirable. A `SELECT` emits `schema` → `row` × N → `finish`; a DML statement (INSERT/UPDATE/DELETE) emits `schema` (empty) → `rowsAffected` → `finish` — the count is how many rows the database actually changed, never a row. A DML with a `RETURNING`/`OUTPUT` clause (Firebird `INSERT ... RETURNING`, MSSQL `INSERT ... OUTPUT`) emits its rows as `row` events and no `rowsAffected` — the returned rows are data, not an affected-row count. DDL (CREATE/DROP/ALTER) emits an empty `schema` and `finish` with no `rowsAffected`, since the database reports no count for it.

A query that goes silent is cut off on every database: no data within `firstDataTimeoutMs` after the query was sent, or no new data within `idleTimeoutMs` after the previous portion, ends it with `finish` carrying a `{ kind: 'timeout', ... }` error, and the call returns at once. The adapter is told to drop its connection (an `AbortSignal`) and is not waited for, so a driver stuck in its own teardown cannot hold the caller. `exec` goes through the same path and has the same limits. Both timeouts default to 12 hours.

A throw from the consumer's callback is not allowed to reach the driver's event loop: it is caught and delivered as the final `finish`'s error with `kind: 'query'` (a bug in the handler reads as a retryable query failure rather than `user` — a documented choice, see `TDbErrorKind`).

`exec` and `execStream` take an optional fourth argument:

```ts
type TExecOptions = {
  signal?: AbortSignal         // cancel the query from outside
  firstDataTimeoutMs?: number  // ms, default 12h
  idleTimeoutMs?: number       // ms, default 12h
}
```

An external `signal` aborts the running query — the call ends with `{ kind: 'aborted', ... }`; a silence timeout ends it with `{ kind: 'timeout', ... }`. A negative, `NaN` or infinite `firstDataTimeoutMs`/`idleTimeoutMs` is a programming error: the call rejects with a `TypeError` — `execStream` and `exec` alike, mirroring `execTxDynamic`'s `timeoutMs` — rather than timing out instantly or never.

### `execTxDynamic` — transaction

```ts
const result = await db.execTxDynamic<number>(async (exec, signal) => {
  if (signal.aborted) return 0
  await exec('INSERT INTO t (id) VALUES (:id)', [{ name: 'id', value: 1 }])
  return 42
}, { timeoutMs, signal })  // or a bare timeoutMs number (legacy)

// result: TResult<number, TDbError> — result.result === 42
```

Opens a transaction, hands the callback an `exec` bound to it and an `AbortSignal`, and commits once the callback resolves. The signal aborts when the timeout expires or the external `signal` is aborted — check it between long-running steps to stop your own work early. Anything thrown inside rolls back and comes out as `{ ok: false, error }`, where `error` is a `TDbError`. Statements use the same `:name` parameters, and every call returns `{ rows }`, so a value generated inside the transaction can be read back before commit. The callback's own resolved value is returned as the result — `execTxDynamic<T>` resolves to `TResult<T, TDbError>` — so a scalar computed inside the transaction does not need a closure to escape. On Oracle, a PL/SQL block that calls `DBMS_SQL.RETURN_RESULT` additionally returns `implicitResults` — one `{ schema, rows }` per implicit result set, while the statement's own `rows`/`rowsAffected` stay empty/undefined.

A failed transaction reports which side failed via `error.kind`: `connect` (getting the connection), `tx` (`BEGIN`/commit), `query` (a statement — even one whose error bubbles through the callback), or `user` (the callback threw its own error). Telling `user` from `query` matters for retry logic: retry a deadlock, not a bug in your code.

`execTxDynamic` takes either a legacy positional `timeoutMs` number or a `TTxOptions` object `{ timeoutMs?, signal? }` (`0` or omitted — no deadline). A negative, `NaN` or infinite `timeoutMs` is a programming error and throws a `TypeError` (the promise rejects) rather than timing out instantly or running without a deadline. On expiry the signal aborts, the connection is torn down (best-effort where the driver exposes no hard cancel — the transaction's commit is additionally blocked by a guard, so no statement runs after the deadline), and the call resolves as `{ ok: false, error: { message: 'TIMEOUT', kind: 'timeout', code: 'TIMEOUT' } }`; an external `signal` cancels the transaction from outside and ends it as `{ ok: false, error: { ..., kind: 'aborted' } }`, mirroring `exec`.

The timeout deadline covers the body and its statements; the final `COMMIT` is bounded by a separate **hard timer** of the same duration. If the commit does not settle before that timer, the call resolves as `{ ok: false, error: { message: 'COMMIT outcome unknown', kind: 'tx', code: 'COMMIT_TIMEOUT' } }` — an **ambiguous outcome**: the server may already have committed, so a retry after it must be idempotent. An external `signal` aborts the transaction at any point, including mid-commit, and ends it as `{ kind: 'aborted' }`; there too the commit may already have landed. The same applies to `execStream`: a DML runs `autoCommit`, so an abort after the statement was sent does not guarantee a rollback.

PostgreSQL and Oracle take a connection from the pool for the transaction and release it back afterwards, on the commit and rollback paths alike. MSSQL opens its own dedicated `ConnectionPool` per transaction and closes it when the transaction finishes, whichever way it ends. Firebird opens its own attach (it has no pool). Firebird runs with `READ COMMITTED` isolation; the other adapters use the driver default.

Statements inside one transaction are not safe to run in parallel: `exec` is bound to a single connection, and the adapters behave differently — MSSQL rejects a second concurrent request («another request in progress»), PostgreSQL queues it, Oracle serializes it. `await` each `exec` before starting the next.

### `ping`

```ts
const result = await db.ping() // TResult<null, TDbError>
```

A minimal read-only query that requires no privileges; returns `{ ok: true }` on success. Useful for waiting for the database to come up — it accepts the same `options` (`signal`, `firstDataTimeoutMs`, `idleTimeoutMs`) as `exec`, so pass a bounded timeout. Firebird's driver has no attach timeout of its own, so a `ping` against an unreachable Firebird host is cut off by whichever deadline fires first — the connection's `connectTimeout` (see Connection fields) or the call's `firstDataTimeoutMs`; with neither set it hangs until the OS TCP timeout.

### `version`

```ts
const result = await db.version() // TResult<string, TDbError>
```

Queries the server version string, accepting the same `options` as `exec`. On Oracle the `v$version` banner needs `SELECT_CATALOG_ROLE`; without it the adapter falls back to `PRODUCT_COMPONENT_VERSION`, which any user can read.

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

The standalone helpers `dbTitle` and `dbUser` derive the same strings from a `TConnectionParam` without opening a connection (so they never load a driver module):

```ts
import { dbTitle, dbUser } from 'vv-db-connection'
dbTitle(connection) // "localhost:5432 [mydb]"
dbUser(connection)  // "admin"
```

### `IDbAdapter` (low-level adapter contract)

`IDbAdapter` is exported for the rare caller that needs the adapter behind `DbConnection` — typically to mock it in tests. `DbConnection` is the intended surface: it picks the adapter from `connection.kind`, loads the driver lazily, and enforces the stream contract (exactly one `finish` per call, and it contains a throw from any consumer event handler). The adapter itself contains only the `finish` handler's throw — a throw from a `row`/`schema`/`data` handler on a directly-built adapter escapes into the driver's event emitter as an uncaught exception. An adapter implements `versionQuery`, `pingQuery`, `execStream`, `execTxDynamic` and `close` (plus the optional `versionFallbackQuery`); building one directly means you re-validate its config yourself — each adapter does that in its own constructor (connect timeout, Oracle pool sizing), exactly as `DbConnection` does.

## Query parameters

All methods accept named parameters in `:name` syntax, regardless of the underlying driver. Each adapter translates them to the driver's native format:

| Driver     | Native format                        | Notes |
|------------|--------------------------------------|-------|
| MSSQL      | `@name` (mssql `request.input`)      | `:name` converted to `@name` |
| PostgreSQL | `$1, $2, …`                          | same `:name` used multiple times maps to the same `$N` |
| Oracle     | `:name` (native bind)                | passed as a named bind object |
| Firebird   | `?` (positional)                     | replaced in left-to-right order of appearance |

In every dialect a `:name` inside a string literal, a quoted identifier or a comment is left as plain text — placeholder scanning skips those. A backslash is an ordinary character in all four dialects (there is no backslash escaping), so `WHERE path = 'C:\' AND id = :id` keeps the string intact and still binds `:id`; only PostgreSQL `E'...'` strings treat a backslash as an escape and are parsed with their own rules, and Oracle `q'[...]'` alternative quoting is recognised too. PostgreSQL dollar-quoted strings (`$$...$$` or `$tag$...$tag$`) are recognised only by the PostgreSQL adapter — in MSSQL, Oracle and Firebird `$` is an ordinary character, so a `$` inside an identifier there stays literal. In a Firebird `EXECUTE BLOCK` only the header — everything up to the `AS` that closes it — is treated as placeholders; past that begins PSQL, where `:name` refers to a local variable or a block parameter and is passed through untouched. MSSQL's bracket-quoted identifiers (`[a:b]`) are also left untouched, but only by the MSSQL adapter — PostgreSQL, Oracle and Firebird treat brackets as ordinary SQL, so `ARRAY[:a, :b]` and array subscripts like `arr[:i]` bind their placeholders normally.

A parameter's SQL type is normally inferred from its JS `value`. To pin the exact type, pass `type` with a driver-native type name:

```ts
{ name: 'at', value: new Date(), type: 'DateTime2' } // mssql: DateTime2, not the precision-losing DateTime
{ name: 'n',  value: '0123',      type: 'NUMBER' }    // oracle: number, not a string
```

`type` is honoured by MSSQL (`sql.TYPES` names) and Oracle (`VARCHAR2`, `NVARCHAR2`, `CHAR`, `NUMBER`, `DATE`, `TIMESTAMP`, `CLOB`, `BLOB`, `RAW`, `BOOLEAN`, `JSON`). PostgreSQL and Firebird infer from the JS value and ignore it. An unknown type name throws before the query is sent in MSSQL and Oracle (the adapters that map `type`); PostgreSQL and Firebird never read it, so nothing is thrown there.

`direction` makes a bind `out`/`inout`. Only Oracle supports it: `oracledb` maps it to `BIND_OUT`/`BIND_INOUT` and the returned value lands in `TDbRes.outParams` (keyed by parameter name). A pure `out` parameter omits `value` (there is nothing to send); `inout` keeps it as the input value. Both need `type` — the driver has no type to allocate the out buffer with — and the other adapters reject the direction rather than silently binding `undefined`.

A placeholder with no matching parameter is an error in every adapter: the query is rejected client-side with `param not found` before it is sent, instead of letting the server answer an opaque bind error. Extra parameters that the SQL does not mention are ignored.

Firebird uploads a `Buffer` (or string) value to a `BLOB` column automatically and sends a JS `bigint` as `INT128`, so LOBs and 64-bit integers work with no extra setup on the way in. Reading a `BIGINT`/`INT128` beyond 2^53 back exactly needs `numericMode: 'string'` (or `'safe'`) on the connection; the default `'lossy'` decodes through `Number` and silently rounds.

## Data types

```ts
type TDbErrorKind = 'connect' | 'query' | 'tx' | 'user' | 'timeout' | 'aborted'
type TDbError = {
  message: string
  code?: string   // driver code: SQLSTATE (pg), ORA-xxxxx / NJS-xxxx (ora), SQL Server number (mssql), gdscode (fb)
                  // — or a synthetic code the library emits: 'TIMEOUT', 'ABORTED', 'COMMIT_TIMEOUT', 'FB_CONN_LOST', 'NO_VERSION'
  kind: TDbErrorKind
  stack?: string  // the underlying Error's stack, when the failure came from a real Error (a driver throw)
}

type TDbKind = 'mssql' | 'pg' | 'ora' | 'fb' // the connection discriminator (db.kind())

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

type TDbParam = {
  name: string
  value?: unknown
  type?: string      // forces a driver-native type name; firebird and postgres infer from the value
  direction?: 'in' | 'out' | 'inout' // 'out'/'inout' are Oracle-only
}

type TTxOptions = {
  timeoutMs?: number // transaction deadline; 0 or omitted — no deadline
  signal?: AbortSignal
}

type TDbTxResult = { rows: Record<string, unknown>[]; rowsAffected?: number; outParams?: Record<string, unknown>; implicitResults?: TDbRes[] }
type TDbTxExec   = (query: string, params: TDbParam[]) => Promise<TDbTxResult>
type TDbTxCb<T = void> = (exec: TDbTxExec, signal: AbortSignal) => Promise<T>
```

Every failure is a `TDbError`: `kind` says what happened (`connect` — could not open the connection, `query` — the query failed, `tx` — `BEGIN`/commit failed, `user` — the callback threw its own error, `timeout` — no data in time, `aborted` — an external `signal` fired), `code` carries the driver's own error code — or a synthetic one the library emits (`TIMEOUT`, `ABORTED`, `COMMIT_TIMEOUT`, `FB_CONN_LOST`, `NO_VERSION`) — and `message` the human text. Failures are told apart by `code`/`kind`, not by parsing `message`.

Unlike `exec`, a statement inside a transaction returns a single set of rows — no schema and no multi-result-set handling, apart from Oracle `implicitResults`, which arrive as separate `{ schema, rows }` sets (see `execTxDynamic`). That single set is the first result set: on a multi-result-set statement only the first recordset is kept and the rest are dropped. A DML statement reports `rowsAffected` even when 0 rows matched (`0`, not unset); a mixed batch (a DML followed by a `SELECT`) reports only the DML's count, since the `SELECT`'s returned rows are not a "rows affected" count. On MSSQL, `SET NOCOUNT ON` suppresses the per-statement count. Both `execStream` and a transaction statement (`execTxDynamic`) stream the result and tell `SELECT` from DML per statement (by the `recordset`/`row`/`rowsaffected` event order), so a NOCOUNT-suppressed `SELECT` no longer hides a following DML's `rowsAffected`. The one case that stays indistinguishable is a NOCOUNT-suppressed `SELECT` followed by a DML that changes exactly as many rows as the `SELECT` returned: it resolves to the `SELECT` — a documented edge case.

## Adapter notes

**MSSQL** — `request.stream = true`, rows delivered via events without driver-side buffering. One `ConnectionPool` per `DbConnection`, created lazily and reused across queries (a transaction opens its own pool — see `execTxDynamic`). `connectTimeout` drives tedious `connectionTimeout` and the tarn pool's `acquireTimeoutMillis`/`createTimeoutMillis`; without it, a request for a slot from an exhausted pool can wait for the 12-hour watchdog. With `SET NOCOUNT ON` the per-statement affected-row count is suppressed, so `execStream` and the transaction path both tell a `SELECT` from a DML by the per-statement event order (`recordset`/`row`/`rowsaffected`) rather than by the driver's buffered `rowsAffected`/`recordsets` bookkeeping — a NOCOUNT-suppressed `SELECT` followed by a DML that changes exactly as many rows as the `SELECT` returned is indistinguishable and resolves to the `SELECT` (an edge case).

**PostgreSQL** — a `pg.Pool` per `DbConnection`. Parameterized queries stream rows via `pg.Query` (`'row'` event) without buffering. Any query with no parameters — an unparameterized `SELECT * FROM big` or a multi-statement string — goes through the simple query protocol, which buffers the whole result in memory; bind one `:name` to a parameter to force the streaming path. A backend does not notice its client is gone until it has something to send, so an aborted query is cancelled from a connection of its own (`pg_cancel_backend`), which needs the same role and a spare connection slot.

**Oracle** — `conn.queryStream()` with `OUT_FORMAT_OBJECT` for `SELECT`/`WITH` (leading whitespace and comments are ignored when routing), rows arrive as plain objects; DML and PL/SQL go through `conn.execute()` (auto-committed). Connections come from an `oracledb` pool (`createPool`, sized via `poolMin`/`poolMax`/`poolIncrement`/`queueTimeout`) and are released back in a `finally` block — on the normal and plain-error paths. On an abort (deadline or external signal) the in-flight statement is interrupted (`break()`) and the connection is closed back to the pool; a close that still fails is dropped (`close({ drop: true })`), discarding the session rather than leaking the pool slot.

**Firebird** — `node-firebird` has no streaming API; the adapter drives the low-level `Transaction`/`Statement` wire API directly, so a single prepared statement serves the column schema, the streamed rows and the affected-row count (the high-level `sequentially()` re-prepares internally and cannot do all three). The column schema comes from the statement's describe (`newStatement`), so it carries the real driver type names and is emitted even for an empty result — never inferred from the first row. DML reports `rowsAffected` from the statement's record counts (`isc_info_sql_records`). Text blobs are fetched as text (`blobAsText`); binary blobs (`SUB_TYPE 0`) are not supported. A lost connection ends the query with `fb: connection lost`: on a closed socket the driver would quietly attach anew and leave the read waiting for an answer that never comes, so that reconnect is cancelled and the driver's `error` / `reconnect` events are taken as the same loss.

The Firebird adapter patches one `node-firebird` internal (the wire-protocol `attach`) to stop a failed attach from leaking the TCP socket — see the note block at the top of `src/db/adapters/fb.ts` for the full rationale.

## License

MIT
