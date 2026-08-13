/**
 * JobQueue — in-memory asynchronous job queue.
 *
 * Extends Node.js EventEmitter and manages concurrent execution of async job
 * functions with support for priority ordering, retries with exponential
 * backoff, per-attempt timeouts, cancellation, and graceful drain.
 */
import { EventEmitter } from 'node:events'
import { MinHeap } from './MinHeap.js'
import { CancelledError, TimeoutError } from './errors.js'

/**
 * Resolves after `ms` milliseconds. Used for retry backoff delays.
 * @param {number} ms
 * @returns {Promise<void>}
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

export class JobQueue extends EventEmitter {
  /**
   * Create a new JobQueue.
   *
   * @param {number} concurrency - Maximum number of simultaneously running jobs.
   *   Must be a positive integer.
   * @throws {RangeError} If concurrency is not a positive integer.
   */
  constructor(concurrency) {
    super()

    if (
      typeof concurrency !== 'number' ||
      !Number.isInteger(concurrency) ||
      concurrency < 1
    ) {
      throw new RangeError(
        `concurrency must be a positive integer, got ${concurrency}`
      )
    }

    this.concurrency = concurrency

    // Queue state
    this.activeCount = 0
    this.heap = new MinHeap()
    this.draining = false
    this.drainResolve = null
    this.drainPromise = null   // stored so repeated drain() calls return the same promise
    this.nextId = 1
    this.nextSeq = 1
  }

  /**
   * Add a job to the queue.
   *
   * Validates all arguments synchronously and throws before returning.
   * Returns a Handle whose .promise resolves/rejects with the job outcome.
   *
   * @param {Function} jobFn - Async function with signature (signal) => Promise.
   * @param {object} [options={}] - Job options.
   * @param {number} [options.priority=0] - Integer in [-100, 100].
   * @param {number} [options.retries=0] - Integer in [0, 10].
   * @param {number} [options.baseDelay=100] - Number (ms) in [1, 30000].
   * @param {number} [options.timeout] - Positive integer ≥ 1 (ms), if provided.
   * @returns {Handle}
   * @throws {TypeError} If jobFn is not a function, or timeout is not a positive integer.
   * @throws {RangeError} If priority, retries, or baseDelay are out of range.
   */
  add(jobFn, options = {}) {
    // Validate jobFn
    if (typeof jobFn !== 'function') {
      throw new TypeError(
        `jobFn must be a function, got ${typeof jobFn}`
      )
    }

    const {
      priority = 0,
      retries = 0,
      baseDelay = 100,
      timeout,
    } = options

    // Validate priority: integer in [-100, 100]
    if (
      typeof priority !== 'number' ||
      !Number.isInteger(priority) ||
      priority < -100 ||
      priority > 100
    ) {
      throw new RangeError(
        `priority must be an integer in [-100, 100], got ${priority}`
      )
    }

    // Validate retries: integer in [0, 10]
    if (
      typeof retries !== 'number' ||
      !Number.isInteger(retries) ||
      retries < 0 ||
      retries > 10
    ) {
      throw new RangeError(
        `retries must be an integer in [0, 10], got ${retries}`
      )
    }

    // Validate baseDelay: number in [1, 30000]
    if (
      typeof baseDelay !== 'number' ||
      isNaN(baseDelay) ||
      baseDelay < 1 ||
      baseDelay > 30000
    ) {
      throw new RangeError(
        `baseDelay must be a number in [1, 30000], got ${baseDelay}`
      )
    }

    // Validate timeout: if provided, must be a positive integer >= 1
    if (timeout !== undefined) {
      if (
        typeof timeout !== 'number' ||
        !Number.isInteger(timeout) ||
        timeout < 1
      ) {
        throw new TypeError(
          `timeout must be a positive integer >= 1, got ${timeout}`
        )
      }
    }

    // 1. Draining check — return a valid Handle with a rejected promise (Req 8.2, 8.3)
    if (this.draining) {
      const drainError = new Error('JobQueue is draining and no longer accepting jobs')
      const failedId = this.nextId++
      let _status = 'failed'
      const rejectedPromise = Promise.reject(drainError)
      // Prevent unhandled rejection — the caller is expected to handle handle.promise
      rejectedPromise.catch(() => {})
      const handle = {
        get status() { return _status },
        get id() { return failedId },
        promise: rejectedPromise,
        cancel() {},
      }
      return handle
    }

    // 2. JobRecord creation
    const jobRecord = {
      id: this.nextId++,
      jobFn,
      priority,
      retries,
      baseDelay,
      timeout,
      attempt: 0,
      status: 'pending',
      cancelled: false,
      resolve: null,
      reject: null,
      insertionSeq: this.nextSeq++,
    }

    // 3. Deferred Promise — attach resolve/reject to jobRecord
    const promise = new Promise((resolve, reject) => {
      jobRecord.resolve = resolve
      jobRecord.reject = reject
    })

    // Capture `this` so the cancel closure can reference queue methods/state
    const self = this

    // 4. Handle — lightweight view over jobRecord
    const handle = {
      get status() { return jobRecord.status },
      get id() { return jobRecord.id },
      promise,
      cancel() {
        // No-op for terminal states
        if (
          jobRecord.status === 'completed' ||
          jobRecord.status === 'failed' ||
          jobRecord.status === 'cancelled'
        ) {
          return
        }

        jobRecord.cancelled = true

        if (jobRecord.status === 'pending') {
          // Remove from the heap so _dispatch won't start it
          self.heap.remove(jobRecord.id)
          jobRecord.status = 'cancelled'
          self.emit('job:cancelled', {
            id: jobRecord.id,
            attempt: 0,
            priority: jobRecord.priority,
          })
          self.emit('job:settled', {
            id: jobRecord.id,
            attempt: 0,
            priority: jobRecord.priority,
          })
          jobRecord.reject(new CancelledError())
        } else if (jobRecord.status === 'running') {
          // Abort the in-flight attempt — _start()'s catch block handles the rest
          if (jobRecord.controller) {
            jobRecord.controller.abort(new CancelledError())
          }
        }
      },
    }

    // 5. Push to heap
    this.heap.push({
      negPriority: -priority,
      insertionSeq: jobRecord.insertionSeq,
      jobRecord,
    })

    // 6. Call _dispatch() synchronously
    this._dispatch()

    // 7. Return the Handle
    return handle
  }

  /**
   * Stop accepting new jobs and return a Promise that resolves when all
   * pending and running jobs have settled.
   *
   * - If already draining, returns the same promise from the first call.
   * - If the queue is already empty, resolves on the next microtask tick.
   * - All subsequent add() calls will return a Handle with status "failed"
   *   and an immediately-rejected promise.
   *
   * @returns {Promise<void>}
   */
  drain() {
    // If already draining, return the existing promise (Req 8.6)
    if (this.draining) {
      return this.drainPromise
    }

    this.draining = true

    // If nothing is running or pending, resolve immediately (Req 8.5)
    if (this.activeCount === 0 && this.heap.size === 0) {
      this.drainPromise = Promise.resolve()
      return this.drainPromise
    }

    // Otherwise create a promise whose resolver is stored for checkDrain()
    this.drainPromise = new Promise(resolve => {
      this.drainResolve = resolve
    })
    return this.drainPromise
  }

  /**
   * Synchronous orchestrator. Fills all available concurrency slots from the
   * MinHeap. Called after add() and after any attempt settles.
   */
  _dispatch() {
    while (this.activeCount < this.concurrency && this.heap.size > 0) {
      const item = this.heap.pop()
      // Skip cancelled items — do NOT call _start
      if (item.jobRecord.cancelled) {
        continue
      }
      this.activeCount++
      item.jobRecord.status = 'running'
      this._start(item.jobRecord) // fire-and-forget async — no await
    }
  }

  /**
   * Retry-loop owner. Drives a job through all its attempts, manages success
   * and failure paths, releases the concurrency slot, and resolves/rejects the
   * handle's promise.
   *
   * @param {object} jobRecord
   */
  async _start(jobRecord) {
    // Track whether we already released the slot (retry-backoff path will do
    // this mid-loop; for now it never happens, but the flag keeps the post-loop
    // accounting correct when task 6.1 adds that path).
    let slotReleased = false

    // eslint-disable-next-line no-constant-condition
    while (true) {
      jobRecord.attempt++

      // --- Cancellation check at top of each iteration ---
      if (jobRecord.cancelled) {
        jobRecord.status = 'cancelled'
        this.emit('job:cancelled', {
          id: jobRecord.id,
          attempt: jobRecord.attempt,
          priority: jobRecord.priority,
        })
        this.emit('job:settled', {
          id: jobRecord.id,
          attempt: jobRecord.attempt,
          priority: jobRecord.priority,
        })
        jobRecord.reject(new CancelledError())
        break
      }

      try {
        const result = await this._runAttempt(jobRecord, jobRecord.attempt)

        // --- Success path ---
        jobRecord.status = 'completed'
        this.emit('job:success', {
          id: jobRecord.id,
          attempt: jobRecord.attempt,
          priority: jobRecord.priority,
          result,
        })
        this.emit('job:settled', {
          id: jobRecord.id,
          attempt: jobRecord.attempt,
          priority: jobRecord.priority,
        })
        jobRecord.resolve(result)
        break
      } catch (error) {
        // --- Cancellation during run ---
        if (jobRecord.cancelled) {
          jobRecord.status = 'cancelled'
          this.emit('job:cancelled', {
            id: jobRecord.id,
            attempt: jobRecord.attempt,
            priority: jobRecord.priority,
          })
          this.emit('job:settled', {
            id: jobRecord.id,
            attempt: jobRecord.attempt,
            priority: jobRecord.priority,
          })
          jobRecord.reject(new CancelledError())
          break
        }

        // --- Retry with exponential backoff ---
        if (jobRecord.retries > 0) {
          jobRecord.retries--
          const delay = Math.random() * jobRecord.baseDelay * (2 ** (jobRecord.attempt - 1))

          this.emit('job:failure', {
            id: jobRecord.id,
            attempt: jobRecord.attempt,
            priority: jobRecord.priority,
            error,
          })

          // Set status to 'pending' before emitting job:retry so that listeners
          // observing handle.status during the event see 'pending'.
          jobRecord.status = 'pending'

          this.emit('job:retry', {
            id: jobRecord.id,
            attempt: jobRecord.attempt,
            priority: jobRecord.priority,
            delay,
          })

          // Release slot BEFORE backoff sleep so other jobs can fill it (Req 4.4, 10.5)
          this.activeCount--
          slotReleased = true
          this._dispatch()
          this._checkIdle()

          await sleep(delay)

          // After sleep, re-push back onto the heap and let _dispatch() claim a
          // slot normally. This prevents activeCount from exceeding concurrency
          // when other jobs filled the slot during the backoff window.
          if (jobRecord.cancelled) {
            // Cancelled during backoff — settle without re-queuing
            jobRecord.status = 'cancelled'
            this.emit('job:cancelled', {
              id: jobRecord.id,
              attempt: jobRecord.attempt,
              priority: jobRecord.priority,
            })
            this.emit('job:settled', {
              id: jobRecord.id,
              attempt: jobRecord.attempt,
              priority: jobRecord.priority,
            })
            jobRecord.reject(new CancelledError())
            // Slot already released above; skip post-loop decrement
            this._dispatch()
            this._checkIdle()
            this._checkDrain()
            return
          }

          // Re-enqueue at original priority so _dispatch() starts it when a slot opens
          this.heap.push({
            negPriority: -jobRecord.priority,
            insertionSeq: jobRecord.insertionSeq,
            jobRecord,
          })
          this._dispatch()
          this._checkDrain()
          // _dispatch() will call _start() for this job again when a slot is free.
          // This _start() invocation's work is done — return without touching activeCount.
          return
        }

        // --- No retries left — terminal failure ---
        error.attempts = jobRecord.attempt
        jobRecord.status = 'failed'
        this.emit('job:failure', {
          id: jobRecord.id,
          attempt: jobRecord.attempt,
          priority: jobRecord.priority,
          error,
        })
        this.emit('job:settled', {
          id: jobRecord.id,
          attempt: jobRecord.attempt,
          priority: jobRecord.priority,
        })
        jobRecord.reject(error)
        break
      }
    }

    // --- Post-loop slot management ---
    if (!slotReleased) {
      this.activeCount--
    }
    this._dispatch()
    this._checkIdle()
    this._checkDrain()
  }

  /**
   * Execute a single attempt of the job function.
   *
   * Creates a fresh AbortController, optionally arms a timeout, emits
   * `job:start`, awaits `jobFn(signal)`, and returns the result.
   * On failure it normalises TimeoutError so callers always see a TimeoutError
   * instance when the timeout fired (not an AbortError or whatever the signal
   * propagates).
   *
   * @param {object} jobRecord
   * @param {number} attempt - 1-indexed attempt number.
   * @returns {Promise<*>} Resolves with jobFn's return value; rejects with the
   *   caught error (or the TimeoutError if a timeout fired).
   */
  async _runAttempt(jobRecord, attempt) {
    const controller = new AbortController()
    const { signal } = controller
    let timerId

    // Expose controller so cancel() can abort the in-flight attempt
    jobRecord.controller = controller

    if (jobRecord.timeout !== undefined) {
      timerId = setTimeout(() => {
        controller.abort(new TimeoutError())
      }, jobRecord.timeout)
    }

    this.emit('job:start', {
      id: jobRecord.id,
      attempt,
      priority: jobRecord.priority,
    })

    try {
      const result = await jobRecord.jobFn(signal)
      clearTimeout(timerId)
      // Even if jobFn resolved, if the timeout fired before we got the result
      // we must honour the timeout and reject the attempt.
      if (signal.aborted && signal.reason instanceof TimeoutError) {
        throw signal.reason
      }
      return result
    } catch (error) {
      clearTimeout(timerId)
      // Normalise: if the signal was aborted due to a TimeoutError, surface
      // the TimeoutError rather than whatever rejection the jobFn produced.
      if (signal.aborted && signal.reason instanceof TimeoutError) {
        throw signal.reason
      }
      throw error
    } finally {
      // Clear stale controller reference so cancel() can't re-abort it
      jobRecord.controller = null
    }
  }

  /**
   * Emit `idle` if there are no active or pending jobs.
   * Called after every job settles.
   */
  _checkIdle() {
    if (this.activeCount === 0 && this.heap.size === 0) {
      this.emit('idle')
    }
  }

  /**
   * Resolve the drain promise if the queue is draining and now empty.
   * Called after every job settles.
   */
  _checkDrain() {
    if (this.draining && this.activeCount === 0 && this.heap.size === 0) {
      if (this.drainResolve) {
        this.drainResolve()
        this.drainResolve = null
      }
    }
  }
}
