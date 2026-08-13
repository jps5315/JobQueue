/**
 * Core JobQueue tests.
 *
 * Covers the three mandatory scenarios plus three additional meaningful cases:
 *   1. FIFO within equal priority
 *   2. Backoff does not block the queue (slot released before sleep)
 *   3. Cancellation of a running job
 *   4. Concurrency limit is never exceeded
 *   5. Priority ordering across pending jobs
 *   6. drain() resolves only after all jobs settle
 */
import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { JobQueue, CancelledError } from './index.js'

const sleep = ms => new Promise(r => setTimeout(r, ms))

// Settle a promise without throwing — returns { ok, value } or { ok, error }
function settle(p) {
  return p.then(value => ({ ok: true, value }), error => ({ ok: false, error }))
}

// ─── 1. FIFO within equal priority ───────────────────────────────────────────

describe('FIFO within equal priority', () => {
  it('jobs with the same priority execute in insertion order', async () => {
    const queue = new JobQueue(1)
    const order = []

    // Block the slot so all subsequent jobs queue up before any runs
    const blocker = queue.add(async () => { await sleep(20) })

    // Add 4 jobs at the same priority — they must run in this exact order
    const jobs = [1, 2, 3, 4].map(n =>
      queue.add(async () => { order.push(n) }, { priority: 0 })
    )

    await blocker.promise
    await Promise.all(jobs.map(h => h.promise))

    assert.deepEqual(order, [1, 2, 3, 4], 'jobs should execute in insertion order')
  })

  it('FIFO is preserved even when higher-priority jobs are also present', async () => {
    const queue = new JobQueue(1)
    const order = []

    // Hold the slot open
    const blocker = queue.add(async () => { await sleep(20) })

    // Two equal-priority jobs bookend a higher-priority job
    const a = queue.add(async () => { order.push('A') }, { priority: 0 })
    const b = queue.add(async () => { order.push('B') }, { priority: 0 })
    const hi = queue.add(async () => { order.push('HI') }, { priority: 10 })
    const c = queue.add(async () => { order.push('C') }, { priority: 0 })

    await blocker.promise
    await Promise.all([a, b, hi, c].map(h => h.promise))

    // HI jumps ahead of A/B/C; A, B, C must still be FIFO among themselves
    assert.equal(order[0], 'HI', 'high-priority job runs first')
    assert.deepEqual(
      order.filter(x => x !== 'HI'),
      ['A', 'B', 'C'],
      'equal-priority jobs remain in insertion order'
    )
  })
})

// ─── 2. Backoff does not block the queue ─────────────────────────────────────

describe('backoff does not block the queue', () => {
  it('a waiting job starts during the retrying job backoff window', async () => {
    // concurrency: 1 — only one slot
    const queue = new JobQueue(1)
    const events = []

    queue.on('job:start',   ev => events.push({ type: 'start',   id: ev.id }))
    queue.on('job:success', ev => events.push({ type: 'success', id: ev.id }))

    let aAttempts = 0
    const handleA = queue.add(
      async () => {
        aAttempts++
        if (aAttempts === 1) throw new Error('first attempt fails')
        return 'A done'
      },
      { retries: 1, baseDelay: 60 }   // 60 ms backoff window
    )

    // B is added right after A — it should run during A's backoff
    const handleB = queue.add(async () => 'B done')

    await Promise.all([settle(handleA.promise), settle(handleB.promise)])

    // Extract the order B and A succeeded
    const successOrder = events
      .filter(e => e.type === 'success')
      .map(e => e.id === handleA.id ? 'A' : 'B')

    assert.equal(successOrder[0], 'B', 'B should succeed before A (ran during A\'s backoff)')
    assert.equal(successOrder[1], 'A', 'A should succeed after B')
    assert.equal(handleA.status, 'completed')
    assert.equal(handleB.status, 'completed')
  })

  it('activeCount never exceeds concurrency during retry backoff', async () => {
    const LIMIT = 2
    const queue = new JobQueue(LIMIT)
    let concurrent = 0
    let maxConcurrent = 0

    // Measure true simultaneous execution by bracketing the actual work inside jobFn
    const track = async (fn) => {
      concurrent++
      maxConcurrent = Math.max(maxConcurrent, concurrent)
      try { return await fn() } finally { concurrent-- }
    }

    let aAttempts = 0
    const handles = [
      queue.add(() => track(async () => { aAttempts++; if (aAttempts < 2) throw new Error('x') }), { retries: 1, baseDelay: 30 }),
      queue.add(() => track(async () => { await sleep(10) })),
      queue.add(() => track(async () => { await sleep(10) })),
      queue.add(() => track(async () => { await sleep(10) })),
    ]

    await Promise.all(handles.map(h => settle(h.promise)))
    assert.ok(maxConcurrent <= LIMIT, `${maxConcurrent} jobs ran simultaneously, limit is ${LIMIT}`)
  })
})

// ─── 3. Cancellation of a running job ────────────────────────────────────────

describe('cancellation of a running job', () => {
  it('cancelling a running job rejects with CancelledError and no retry occurs', async () => {
    const queue = new JobQueue(1)
    let callCount = 0

    const handle = queue.add(
      async signal => {
        callCount++
        // Stay alive until aborted
        await new Promise((_, reject) => {
          signal.addEventListener('abort', () => reject(signal.reason))
        })
      },
      { retries: 3, baseDelay: 1 }   // retries should be ignored after cancel
    )

    // Give the job a tick to start running
    await sleep(10)
    assert.equal(handle.status, 'running', 'job should be running before cancel')

    handle.cancel()

    const { ok, error } = await settle(handle.promise)

    assert.equal(ok, false, 'promise should reject')
    assert.ok(error instanceof CancelledError, 'error should be CancelledError')
    assert.equal(handle.status, 'cancelled')
    assert.equal(callCount, 1, 'jobFn should only have been called once — no retry after cancel')
  })

  it('slot is released after cancelling a running job', async () => {
    const queue = new JobQueue(1)

    const handleA = queue.add(
      async signal => {
        await new Promise((_, reject) => {
          signal.addEventListener('abort', () => reject(signal.reason))
        })
      }
    )

    await sleep(10)   // let A start
    handleA.cancel()
    await settle(handleA.promise)

    // The slot should now be free — B should start immediately
    const handleB = queue.add(async () => 'B')
    const result = await handleB.promise

    assert.equal(result, 'B', 'slot was released — B ran after A was cancelled')
  })
})

// ─── 4. Concurrency limit ────────────────────────────────────────────────────

describe('concurrency limit', () => {
  it('never runs more jobs simultaneously than the configured limit', async () => {
    const LIMIT = 3
    const queue = new JobQueue(LIMIT)
    let active = 0
    let maxSeen = 0

    const jobFn = async () => {
      active++
      maxSeen = Math.max(maxSeen, active)
      await sleep(20)
      active--
    }

    const handles = Array.from({ length: 10 }, () => queue.add(jobFn))
    await Promise.all(handles.map(h => h.promise))

    assert.ok(maxSeen <= LIMIT, `ran ${maxSeen} jobs simultaneously, limit is ${LIMIT}`)
    assert.equal(maxSeen, LIMIT, 'should have saturated all slots')
  })
})

// ─── 5. Priority ordering ────────────────────────────────────────────────────

describe('priority ordering', () => {
  it('higher-priority jobs run before lower-priority jobs when queued together', async () => {
    const queue = new JobQueue(1)
    const order = []

    // Hold the slot so everything queues up
    await new Promise(resolve => {
      queue.add(async () => { await sleep(20) }).promise.then(resolve)

      queue.add(async () => { order.push('low')  }, { priority: -10 })
      queue.add(async () => { order.push('high') }, { priority:  50 })
      queue.add(async () => { order.push('mid')  }, { priority:  20 })
      queue.add(async () => { order.push('zero') }, { priority:   0 })
    })

    // Wait for all queued jobs
    await sleep(10)
    assert.deepEqual(order, ['high', 'mid', 'zero', 'low'])
  })
})

// ─── 6. drain() ──────────────────────────────────────────────────────────────

describe('drain()', () => {
  it('resolves only after all pending and running jobs have settled', async () => {
    const queue = new JobQueue(2)
    const settled = []

    queue.on('job:settled', ev => settled.push(ev.id))

    const handles = [
      queue.add(async () => { await sleep(40) }),
      queue.add(async () => { await sleep(20) }),
      queue.add(async () => { await sleep(10) }),
    ]

    await queue.drain()

    // All three jobs must have settled before drain resolved
    assert.equal(settled.length, 3, 'all 3 jobs should have settled before drain resolves')
    handles.forEach(h => assert.equal(h.status, 'completed'))
  })

  it('add() after drain() returns a failed handle without throwing', async () => {
    const queue = new JobQueue(1)
    queue.drain()

    const handle = queue.add(async () => 'should not run')
    assert.equal(handle.status, 'failed', 'handle.status should be "failed" immediately')

    const { ok } = await settle(handle.promise)
    assert.equal(ok, false, 'handle.promise should reject')
  })
})
