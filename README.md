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

`createDbConnection(connection)` picks the adapter from `connection.kind` and returns a `DbConnection`. The connection is passed as a plain object — its schema and types are owned by this package. Create as many connections as you need, in parallel, each with its own driver and pool:

```ts
const src = createDbConnection(mssqlConnection) // source
const dst = createDbConnection(pgConnection)     // target
```

The connection is opened lazily — `createDbConnection` only holds the config; the first `exec`/`ping`/`execStream` call opens it.

### Connection fields

| `kind`    | Type         | Fields                                                                        |
|-----------|--------------|-------------------------------------------------------------------------------|
| `mssql` | `TConnMssql` | `server`, `port?`, `database?`, `auth`, `trustServerCertificate?`, `encrypt?` |
| `pg`    | `TConnPg`    | `host`, `port?`, `database`, `user`, `password`, `ssl?`                       |
| `ora`   | `TConnOra`   | `host`, `port?`, `service`, `user`, `password`, `privilege?`                  |
| `fb`    | `TConnFb`    | `host`, `port?`, `database`, `user`, `password`, `charset?`                   |

MSSQL `auth` is a union: `{ kind: 'mssql', user, password }` or `{ kind: 'ntlm', domain, user, password }`.

Oracle `privilege` is one of `SYSDBA | SYSOPER | SYSASM | SYSBACKUP | SYSDG | SYSKM | SYSRAC`; the connection string is assembled internally as `host:port/service`.

Firebird `charset` is `'UTF8'` (default) or `'WIN1251'`.

## API

### `exec` — buffered query

```ts
const result = await db.exec('SELECT * FROM orders WHERE status = :status', [
  { name: 'status', value: 'open' },
])

if (!result.ok) {
  console.error(result.error)
} else {
  for (const resultSet of result.result) {
    console.log(resultSet.schema) // TDbCol[]
    console.log(resultSet.rows)   // object[]
  }
}
```

Collects all rows into memory and returns `TResult<TDbRes[]>`. Each element of the array is one result set (relevant for multi-statement queries on MSSQL).

### `execStream` — streaming query

```ts
await db.execStream(query, params, (event) => {
  if (event.kind === 'schema') { /* event.schema: TDbCol[] */ }
  if (event.kind === 'row')    { /* event.row: object      */ }
  if (event.kind === 'finish') { /* event.error?: string   */ }
})
```

Fires the callback incrementally as data arrives — for large result sets where buffering everything is undesirable. Events are always emitted in this order: `schema` → `row` × N → `finish`.

A query that goes silent is cut off on every database: no data within 12 hours after the query was sent, or no new data within 12 hours after the previous portion, ends it with `finish` carrying a `timeout: ...` error, and the call returns at once. The adapter is told to drop its connection (an `AbortSignal`) and is not waited for, so a driver stuck in its own teardown cannot hold the caller. `exec` goes through the same path and has the same limits.

### `execTxDynamic` — transaction

```ts
const result = await db.execTxDynamic(async (exec) => {
  await exec('INSERT INTO t (id) VALUES (:id)', [{ name: 'id', value: 1 }])
  const res = await exec('SELECT MAX(id) AS id FROM t', [])
  // res.rows: Record<string, unknown>[]
}, timeoutMs)

// result: TResult<void>
```

Opens a transaction, hands the callback an `exec` bound to it, and commits once the callback resolves. Anything thrown inside rolls back and comes out as `{ ok: false, error }`. Statements use the same `:name` parameters, and every call returns `{ rows }`, so a value generated inside the transaction can be read back before commit.

`timeoutMs` is optional (`0` or omitted — no limit). On expiry the connection is torn down and the call resolves as `{ ok: false, error: 'TIMEOUT' }`.

A dedicated connection is opened for the transaction and closed afterwards, on the commit and rollback paths alike. Firebird runs with `READ COMMITTED` isolation; the other adapters use the driver default.

### `ping`

```ts
const result = await db.ping() // TResult<null>
```

A minimal read-only query that requires no privileges; returns `{ ok: true }` on success. Useful for waiting for the database to come up.

### `version`

```ts
const result = await db.version() // TResult<string>
```

Queries the server version string.

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
type TDbCol = {
  name: string
  kind: string      // driver-specific type name
  len?: number
  scale?: number
  precision?: number
}

type TDbRes = {
  schema: TDbCol[]
  rows: object[]
}

type TDbStreamEvent =
  | { kind: 'schema'; schema: TDbCol[] }
  | { kind: 'row'; row: object }
  | { kind: 'finish'; error?: string }

type TDbTxResult = { rows: Record<string, unknown>[] }
type TDbTxExec   = (query: string, params: TDbParam[]) => Promise<TDbTxResult>
type TDbTxCb     = (exec: TDbTxExec) => Promise<void>
```

Unlike `exec`, a statement inside a transaction returns a single set of rows — no schema and no multi-result-set handling.

## Adapter notes

**MSSQL** — `request.stream = true`, rows delivered via events without driver-side buffering. A new `ConnectionPool` is created per query and closed in the `done` handler.

**PostgreSQL** — non-streaming `client.query()` (buffered inside `pg`). A fresh `Client` per query, disconnected in a `finally` block. A backend does not notice its client is gone until it has something to send, so an aborted query is cancelled from a connection of its own (`pg_cancel_backend`).

**Oracle** — `conn.queryStream()` with `OUT_FORMAT_OBJECT`, rows arrive as plain objects. A connection is obtained from the default pool and closed in a `finally` block.

**Firebird** — `node-firebird` has no streaming API; `db.sequentially()` calls the row callback once per row without loading the full result into memory. Column schema is inferred from the keys of the first row (types reported as `"unknown"`). Text blobs are fetched as text (`blobAsText`); binary blobs (`SUB_TYPE 0`) are not supported. With `charset: WIN1251` blob segments are re-encoded on the way in, because the driver decodes them with a hardcoded UTF8. The blobs of one fetch batch are read one after another, not all at once as the driver does, and rows are handed over batch by batch instead of being collected into an array first. A lost connection ends the query with `fb: connection lost`: on a closed socket the driver would quietly attach anew and leave the read waiting for an answer that never comes, so that reconnect is cancelled and the driver's `error` / `reconnect` events are taken as the same loss.

The Firebird adapter patches several `node-firebird` internals (its wire protocol layer) to work around bugs with Firebird 6 — see the note block in `src/db/adapters/fb.ts` for the full rationale.

## License

MIT
