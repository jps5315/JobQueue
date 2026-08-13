/**
 * Error classes for async-job-queue.
 */

/**
 * Thrown when a job is cancelled via handle.cancel().
 */
export class CancelledError extends Error {
  constructor(message = 'Job was cancelled') {
    super(message)
    this.name = 'CancelledError'
  }
}

/**
 * Thrown when a job attempt exceeds its configured timeout duration.
 */
export class TimeoutError extends Error {
  constructor(message = 'Job attempt timed out') {
    super(message)
    this.name = 'TimeoutError'
  }
}
