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

test('a rejected body is reported as a query error, not as TIMEOUT', async () => {
	const r = await withTxTimeout(5000, async () => {
		throw new Error('boom')
	})
	assert.equal(r.ok, false)
	if (!r.ok) {
		assert.equal(r.error.kind, 'query')
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
