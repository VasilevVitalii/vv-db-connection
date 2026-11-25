# Changelog

All notable changes to this project are documented here.

## 2.0.0 — 2026-11-25

### Breaking change: `firstDataTimeoutMs` default is 10 minutes, not 12 hours

The bound against a query that never produces its first byte (`firstDataTimeoutMs`) dropped from **12 hours to 10 minutes**. A query against a half-open socket — the server, or a NAT/firewall on the way, dropped the idle TCP connection without the client noticing — used to hang until the 12-hour deadline; it is now cut off after 10 minutes. A new **liveness ping** detects the stale socket up front (see Added).

- **Old:** `firstDataTimeoutMs` defaulted to 12 hours.
- **New:** `firstDataTimeoutMs` defaults to 10 minutes. `idleTimeoutMs` stays 12 hours — a pause between portions of an ongoing stream is legitimately long and is not affected.

A query that legitimately needs longer than 10 minutes to produce its first byte must set the bound explicitly:

```ts
await db.exec(longRunningDml, [], { firstDataTimeoutMs: 2 * 60 * 60 * 1000 }) // 2 h
```

**A write timeout does not mean the write did not run.** When `firstDataTimeoutMs` fires, the statement may already have been sent to and executed by the server; the library tears the connection down and reports `{ kind: 'timeout' }`, but it cannot know whether the server committed. Treat a timed-out DML as an ambiguous outcome and make any retry idempotent.

### Added

- **Liveness ping**: after 30+ seconds of idle, the next query is preceded by a `SELECT 1` probe with its own 5-second deadline. On a half-open socket the probe times out, the adapter tears the socket down, and the real query runs on a fresh connection. The ping's failure is swallowed and never fails the caller's query; queries less than 30 seconds apart skip it. See the README (`execStream`) for the full semantics — suspicion tracking, slot ownership, and the "exactly one `finish` per call" guarantee.
