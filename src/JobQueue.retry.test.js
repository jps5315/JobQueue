/**
 * Unit tests for retry-with-exponential-backoff behavior in JobQueue.
 *
 * Requirements covered: 4.2, 4.4, 4.5, 4.6, 4.7
 */
import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { JobQueue } from './index.js'

// Small helper — wait for a promise to settle and return { ok, value } or { ok, error }
function settle(promise) {
  return promise.then(
    value => ({ ok: true, value }),
    error => ({ ok: false, error }),
  )
}

describe('JobQueue retry behavior', () => {
  it('job failing twice then succeeding: jobFn called 3 times, promise resolves', async () => {
    const queue = new JobQueue(1)
    let callCount = 0

    const jobFn = async () => {
      callCount++
      if (callCount < 3) throw new Error(`fail on attempt ${callCount}`)
      return 'done'
    }

    const handle = queue.add(jobFn, { retries: 2, baseDelay: 1 })
    const result = await handle.promise

    assert.equal(callCount, 3, 'jobFn should be called exactly 3 times')
    assert.equal(result, 'done', 'promise should resolve with final result')
    assert.equal(handle.status, 'completed')
  })

  it('job:retry event has correct shape', async () => {
    const queue = new JobQueue(1)
    let callCount = 0
    const retryEvents = []

    queue.on('job:retry', event => retryEvents.push(event))

    const jobFn = async () => {
      callCount++
      if (callCount < 2) throw new Error('fail once')
      return 'ok'
    }

    const handle = queue.add(jobFn, { retries: 1, baseDelay: 1 })
    await handle.promise

    assert.equal(retryEvents.length, 1, 'exactly one job:retry event emitted')

    const ev = retryEvents[0]
    assert.equal(typeof ev.id, 'number', 'id should be a number')
    assert.ok(ev.id > 0, 'id should be a positive integer')
    assert.equal(ev.attempt, 1, 'attempt should be 1 (the attempt that failed and triggered retry)')
    assert.equal(typeof ev.priority, 'number', 'priority should be a number')
    assert.equal(typeof ev.delay, 'number', 'delay should be a number')
    assert.ok(ev.delay >= 0, 'delay should be >= 0')
  })

  it('error.attempts equals retries+1 when all retries exhausted', async () => {
    const queue = new JobQueue(1)

    const jobFn = async () => { throw new Error('always fails') }

    const handle = queue.add(jobFn, { retries: 2, baseDelay: 1 })
    const { ok, error } = await settle(handle.promise)

    assert.equal(ok, false, 'promise should reject')
    assert.equal(error.attempts, 3, 'error.attempts should equal retries + 1 = 3')
    assert.equal(handle.status, 'failed')
  })

  it('handle.status is "pending" during backoff (observable in job:retry listener)', async () => {
    const queue = new JobQueue(1)
    let callCount = 0
    let statusDuringBackoff = null

    const handle = queue.add(
      async () => {
        callCount++
        if (callCount < 2) throw new Error('fail')
        return 'ok'
      },
      { retries: 1, baseDelay: 50 },
    )

    // job:retry is emitted after status is set to 'pending', before sleep
    queue.on('job:retry', () => {
      statusDuringBackoff = handle.status
    })

    await handle.promise

    assert.equal(statusDuringBackoff, 'pending', 'handle.status should be "pending" during backoff')
  })

  it('slot released before sleep — second job starts during backoff window', async () => {
    // concurrency: 1, job A retries once with a real delay, job B should run
    // during A's backoff window (before A's retry attempt fires).
    const queue = new JobQueue(1)
    const successOrder = []

    // Job A: fails on attempt 1, succeeds on attempt 2
    let aCallCount = 0
    const handleA = queue.add(
      async () => {
        aCallCount++
        if (aCallCount < 2) throw new Error('A fails on first attempt')
        return 'A'
      },
      { retries: 1, baseDelay: 50 },
    )

    // Job B: resolves immediately; added right after A
    const handleB = queue.add(
      async () => 'B',
      { retries: 0, baseDelay: 1 },
    )

    queue.on('job:success', ev => successOrder.push(ev.id === handleA.id ? 'A' : 'B'))

    // Wait for both to settle
    await Promise.all([settle(handleA.promise), settle(handleB.promise)])

    // B should have completed during A's backoff, so B's success fires first
    assert.equal(successOrder[0], 'B', 'job B should succeed before job A (B ran during A\'s backoff)')
    assert.equal(successOrder[1], 'A', 'job A should succeed after job B')
    assert.equal(handleA.status, 'completed')
    assert.equal(handleB.status, 'completed')
  })
})
