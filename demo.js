/**
 * demo.js — JobQueue feature showcase
 *
 * Demonstrates:
 *   1. Concurrency limiting  — only 2 jobs run at a time
 *   2. Priority ordering     — high-priority jobs jump the queue
 *   3. Retry with backoff    — a flaky job retries automatically
 *   4. Per-attempt timeout   — a slow job is aborted after N ms
 *   5. Cancellation          — a pending job is cancelled before it ever runs
 *
 * Run:  node demo.js
 */
import { JobQueue, CancelledError, TimeoutError } from './index.js'

// ─── helpers ────────────────────────────────────────────────────────────────

const sleep = ms => new Promise(r => setTimeout(r, ms))

function log(label, msg) {
  const t = (performance.now() / 1000).toFixed(2)
  console.log(`[${t}s] ${label.padEnd(22)} ${msg}`)
}

function section(title) {
  console.log(`\n${'─'.repeat(56)}`)
  console.log(`  ${title}`)
  console.log('─'.repeat(56))
}

// ─── 1. Concurrency limiting ─────────────────────────────────────────────────

section('1 · Concurrency limiting  (max 2 simultaneous)')

{
  const queue = new JobQueue(2)
  let running = 0
  let maxSeen = 0

  queue.on('job:start',   () => { running++; maxSeen = Math.max(maxSeen, running) })
  queue.on('job:settled', () => running--)

  // Add 5 jobs that each take 80 ms
  const handles = Array.from({ length: 5 }, (_, i) =>
    queue.add(async () => { await sleep(80); return `job-${i + 1}` }, { priority: 0 })
  )

  await Promise.all(handles.map(h => h.promise))
  log('concurrency', `max simultaneous = ${maxSeen}  (limit was 2) ✓`)
}

// ─── 2. Priority ordering ────────────────────────────────────────────────────

section('2 · Priority ordering  (higher number runs first)')

{
  const queue = new JobQueue(1)   // single slot so ordering is deterministic
  const order = []

  // Block the slot with one long job so the rest queue up
  queue.add(async () => { await sleep(60) })

  // Then add jobs with different priorities out of insertion order
  const specs = [
    { name: 'low   (-10)', priority: -10 },
    { name: 'high  (+50)', priority:  50 },
    { name: 'zero  (  0)', priority:   0 },
    { name: 'mid   (+20)', priority:  20 },
  ]

  const handles = specs.map(s =>
    queue.add(async () => { order.push(s.name); return s.name }, { priority: s.priority })
  )

  await Promise.all(handles.map(h => h.promise))
  log('priority', 'execution order:')
  order.forEach((name, i) => log('', `  ${i + 1}. ${name}`))
}

// ─── 3. Retry with exponential backoff ───────────────────────────────────────

section('3 · Retry with exponential backoff  (2 retries)')

{
  const queue = new JobQueue(1)
  let attempts = 0

  queue.on('job:retry', ({ attempt, delay }) =>
    log('retry', `attempt ${attempt} failed — retrying in ${delay.toFixed(0)} ms`)
  )

  const handle = queue.add(
    async () => {
      attempts++
      if (attempts < 3) throw new Error(`transient error (attempt ${attempts})`)
      return `succeeded on attempt ${attempts}`
    },
    { retries: 2, baseDelay: 40 }
  )

  const result = await handle.promise
  log('retry', `✓ ${result}`)
}

// ─── 4. Per-attempt timeout ───────────────────────────────────────────────────

section('4 · Per-attempt timeout  (150 ms limit, job takes 400 ms)')

{
  const queue = new JobQueue(1)

  const handle = queue.add(
    async signal => {
      // Respect the abort signal so the job exits cleanly on timeout
      await new Promise((resolve, reject) => {
        const id = setTimeout(resolve, 400)
        signal.addEventListener('abort', () => {
          clearTimeout(id)
          reject(signal.reason)
        })
      })
    },
    { timeout: 150 }
  )

  try {
    await handle.promise
    log('timeout', '✗ expected a TimeoutError but job resolved')
  } catch (err) {
    if (err instanceof TimeoutError) {
      log('timeout', `✓ caught TimeoutError — "${err.message}"`)
    } else {
      log('timeout', `✗ unexpected error: ${err.message}`)
    }
  }
}

// ─── 5. Cancellation ─────────────────────────────────────────────────────────

section('5 · Cancellation  (pending job cancelled before it runs)')

{
  const queue = new JobQueue(1)
  let jobFnCalled = false

  // Block the only slot
  queue.add(async () => { await sleep(100) })

  // This job is pending — it will never get the slot
  const handle = queue.add(async () => {
    jobFnCalled = true
    return 'should not reach here'
  })

  log('cancel', `status before cancel: "${handle.status}"`)
  handle.cancel()
  log('cancel', `status after  cancel: "${handle.status}"`)

  try {
    await handle.promise
    log('cancel', '✗ expected a CancelledError but job resolved')
  } catch (err) {
    if (err instanceof CancelledError) {
      log('cancel', `✓ caught CancelledError — jobFn called: ${jobFnCalled}`)
    } else {
      log('cancel', `✗ unexpected error: ${err.message}`)
    }
  }
}

console.log('\n✓ All demos complete.\n')
