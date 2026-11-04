import { test } from 'node:test'
import assert from 'node:assert/strict'
import { withTxTimeout } from '../src/db/helpers'

// Regression guards for review2 2.2: a rejected transaction body used to be labelled
// TIMEOUT (hiding the real error), and invalid deadlines were accepted silently - a
// negative timeout fired at once, NaN disabled the timer entirely.

test('withTxTimeout returns the body result when there is no deadline', async () => {
	const r = await withTxTimeout(undefined, async () => ({ ok: true as const, result: 42 }))
	assert.deepEqual(r, { ok: true, result: 42 })
})

test('withTxTimeout treats 0 as no deadline (as before)', async () => {
	const r = await withTxTimeout(0, async () => ({ ok: true as const, result: 1 }))
	assert.deepEqual(r, { ok: true, result: 1 })
})

test('withTxTimeout rejects a negative deadline', () => {
	assert.throws(
		() => withTxTimeout(-5, async () => ({ ok: true as const, result: 1 })),
		/non-negative finite number/,
	)
})

test('withTxTimeout rejects NaN and Infinity deadlines', () => {
	assert.throws(
		() => withTxTimeout(NaN, async () => ({ ok: true as const, result: 1 })),
		/non-negative finite number/,
	)
	assert.throws(
		() => withTxTimeout(Infinity, async () => ({ ok: true as const, result: 1 })),
		/non-negative finite number/,
	)
})

test('a rejected body is reported as a tx error, not as TIMEOUT or a retryable query', async () => {
	const r = await withTxTimeout(5000, async () => {
		throw new Error('boom')
	})
	assert.equal(r.ok, false)
	if (!r.ok) {
		assert.equal(r.error.kind, 'tx')
		assert.equal(r.error.message, 'boom')
	}
})

// review2 §3 (group B): execTxDynamic gained an external AbortSignal. The body's signal
// must also fire when the caller aborts it, and the call must end as 'aborted' - not as
// 'timeout' (that kind is reserved for the deadline) and not as the body's own result.
test('an external signal aborts the transaction with kind aborted', async () => {
	const controller = new AbortController()
	const rPromise = withTxTimeout(undefined, async (signal) => {
		await new Promise<void>((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }))
		return { ok: true as const, result: 1 }
	}, controller.signal)
	controller.abort(new Error('cancelled by caller'))
	const r = await rPromise
	assert.equal(r.ok, false)
	if (!r.ok) {
		assert.equal(r.error.kind, 'aborted')
		assert.equal(r.error.message, 'cancelled by caller')
	}
})

// review8 §2.15: without a deadline or an external signal the body must still settle as a
// TResult - a throwing body reports kind 'tx' (an adapter bug, not a query to retry),
// never rejects the caller (symmetric with the deadline path).
test('withTxTimeout without a deadline reports a throwing body as tx', async () => {
	const r = await withTxTimeout(undefined, async () => { throw new Error('boom') })
	assert.equal(r.ok, false)
	if (!r.ok) assert.equal(r.error.kind, 'tx')
})

// review8 §1.1 (variant A): a commit that hangs longer than the deadline is bounded by a
// separate "hard" timer reporting an ambiguous outcome (kind 'tx', code 'COMMIT_TIMEOUT')
// - the server may already have committed, so a retry must be idempotent.
test('withTxTimeout: a hanging commit is bounded by the hard timer', async () => {
	const r = await withTxTimeout<unknown>(30, async (_signal, armCommitDeadline) => {
		armCommitDeadline()
		// A commit that never settles (half-open TCP / stuck server).
		await new Promise<never>(() => {})
		return { ok: true as const, result: undefined }
	})
	assert.equal(r.ok, false)
	if (!r.ok) {
		assert.equal(r.error.kind, 'tx')
		assert.equal(r.error.code, 'COMMIT_TIMEOUT')
	}
})

test('an already-aborted external signal settles at once without running the body', async () => {
	const controller = new AbortController()
	controller.abort(new Error('already'))
	let ran = false
	const r = await withTxTimeout(undefined, async () => {
		ran = true
		return { ok: true as const, result: 1 }
	}, controller.signal)
	assert.equal(r.ok, false)
	if (!r.ok) assert.equal(r.error.kind, 'aborted')
	assert.equal(ran, false)
})

// review9 §2.2: armCommitDeadline() called after the race already settled (the deadline
// fired while the body ignored the signal) must not arm a fresh timer - cleanup() has
// already run, so a new timer would outlive the call and keep the event loop alive.
test('withTxTimeout: armCommitDeadline after the deadline is a no-op', async () => {
	const r = await withTxTimeout<unknown>(20, async (_signal, armCommitDeadline) => {
		// Run past the deadline (20ms) without reacting to the abort, like an adapter
		// whose cb ignores the signal and still reaches the commit phase.
		await new Promise<void>((resolve) => { setTimeout(resolve, 40) })
		// The adapter would call this before commit. It must neither throw nor arm a
		// timer now that the race is settled.
		armCommitDeadline()
		return { ok: true as const, result: undefined }
	})
	assert.equal(r.ok, false)
	if (!r.ok) assert.equal(r.error.kind, 'timeout')
})

// review9 §3: an external abort whose reason is not an Error (a plain string, an object)
// used to build a synthetic TDbError with no code/stack, so isDbError() would re-label it
// 'user'. The synthetic abort now carries code 'ABORTED' so it stays structured.
test('an abort with a non-Error reason still carries an error code', async () => {
	const controller = new AbortController()
	const rPromise = withTxTimeout(undefined, async (signal) => {
		await new Promise<void>((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }))
		return { ok: true as const, result: 1 }
	}, controller.signal)
	controller.abort('plain-string reason')
	const r = await rPromise
	assert.equal(r.ok, false)
	if (!r.ok) {
		assert.equal(r.error.kind, 'aborted')
		assert.equal(r.error.code, 'ABORTED')
	}
})
