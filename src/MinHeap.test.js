/**
 * Unit tests for MinHeap.
 *
 * Uses Node.js built-in test runner (node:test + node:assert).
 * Run: node --test src/MinHeap.test.js
 */
import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { MinHeap } from './MinHeap.js'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Build a heap item from simple primitives. */
function item(priority, seq, id = seq) {
  return { negPriority: -priority, insertionSeq: seq, jobRecord: { id } }
}

/** Pop all items from the heap and return an array of jobRecord ids. */
function popAll(heap) {
  const result = []
  while (heap.size > 0) result.push(heap.pop().jobRecord.id)
  return result
}

// ---------------------------------------------------------------------------
// size / peek / empty behaviour
// ---------------------------------------------------------------------------

describe('MinHeap — basic state', () => {
  it('starts empty', () => {
    const h = new MinHeap()
    assert.equal(h.size, 0)
    assert.equal(h.peek(), undefined)
    assert.equal(h.pop(), undefined)
  })

  it('size reflects the number of pushed items', () => {
    const h = new MinHeap()
    h.push(item(1, 1))
    assert.equal(h.size, 1)
    h.push(item(2, 2))
    assert.equal(h.size, 2)
    h.pop()
    assert.equal(h.size, 1)
    h.pop()
    assert.equal(h.size, 0)
  })

  it('peek returns the root without removing it', () => {
    const h = new MinHeap()
    h.push(item(5, 1, 1))
    h.push(item(10, 2, 2))
    const peeked = h.peek()
    assert.equal(peeked.jobRecord.id, 2) // priority 10 → negPriority -10 → smallest
    assert.equal(h.size, 2)              // still 2 items
  })
})

// ---------------------------------------------------------------------------
// Priority ordering (higher priority value surfaces first)
// ---------------------------------------------------------------------------

describe('MinHeap — priority ordering', () => {
  it('pops items in descending priority order', () => {
    const h = new MinHeap()
    h.push(item(1, 1, 1))   // lowest priority
    h.push(item(10, 2, 2))  // highest priority
    h.push(item(5, 3, 3))   // middle priority
    const order = popAll(h)
    assert.deepEqual(order, [2, 3, 1]) // 10 → 5 → 1
  })

  it('handles negative priority values', () => {
    const h = new MinHeap()
    h.push(item(-50, 1, 1))
    h.push(item(0, 2, 2))
    h.push(item(50, 3, 3))
    const order = popAll(h)
    assert.deepEqual(order, [3, 2, 1]) // 50 → 0 → -50
  })

  it('pops a single item correctly', () => {
    const h = new MinHeap()
    h.push(item(7, 1, 42))
    const popped = h.pop()
    assert.equal(popped.jobRecord.id, 42)
    assert.equal(h.size, 0)
  })
})

// ---------------------------------------------------------------------------
// FIFO tie-breaking
// ---------------------------------------------------------------------------

describe('MinHeap — FIFO tie-breaking', () => {
  it('FIFO order when all priorities are equal', () => {
    const h = new MinHeap()
    for (let seq = 1; seq <= 5; seq++) h.push(item(0, seq, seq))
    const order = popAll(h)
    assert.deepEqual(order, [1, 2, 3, 4, 5])
  })

  it('FIFO within the same priority group when mixed with other priorities', () => {
    const h = new MinHeap()
    h.push(item(5, 1, 1))  // group A, first
    h.push(item(5, 2, 2))  // group A, second
    h.push(item(3, 3, 3))  // group B (lower priority)
    h.push(item(5, 4, 4))  // group A, third
    const order = popAll(h)
    assert.deepEqual(order, [1, 2, 4, 3]) // all priority-5 in insertion order, then priority-3
  })
})

// ---------------------------------------------------------------------------
// remove()
// ---------------------------------------------------------------------------

describe('MinHeap — remove()', () => {
  it('remove the only element', () => {
    const h = new MinHeap()
    h.push(item(5, 1, 99))
    h.remove(99)
    assert.equal(h.size, 0)
    assert.equal(h.pop(), undefined)
  })

  it('remove the root element', () => {
    const h = new MinHeap()
    h.push(item(10, 1, 1)) // highest priority — will be root
    h.push(item(5, 2, 2))
    h.push(item(3, 3, 3))
    h.remove(1)
    const order = popAll(h)
    assert.deepEqual(order, [2, 3])
  })

  it('remove a middle element preserves heap invariant', () => {
    const h = new MinHeap()
    h.push(item(10, 1, 1))
    h.push(item(5, 2, 2))
    h.push(item(8, 3, 3))
    h.push(item(3, 4, 4))
    h.remove(2) // remove the item with id=2 (priority 5)
    const order = popAll(h)
    assert.deepEqual(order, [1, 3, 4]) // 10 → 8 → 3
  })

  it('remove the last element', () => {
    const h = new MinHeap()
    h.push(item(10, 1, 1))
    h.push(item(5, 2, 2))
    h.remove(2)
    assert.equal(h.size, 1)
    assert.equal(h.pop().jobRecord.id, 1)
  })

  it('remove a non-existent id is a no-op', () => {
    const h = new MinHeap()
    h.push(item(10, 1, 1))
    h.push(item(5, 2, 2))
    h.remove(999) // does not exist
    assert.equal(h.size, 2)
    const order = popAll(h)
    assert.deepEqual(order, [1, 2])
  })

  it('heap invariant is correct after removing then pushing more items', () => {
    const h = new MinHeap()
    for (let i = 1; i <= 6; i++) h.push(item(i, i, i))
    h.remove(3) // remove mid-level item
    h.push(item(4, 7, 7)) // push a new item with duplicate priority
    const order = popAll(h)
    // Expected order by priority desc, then FIFO: 6,5,4(id4),4(id7),2,1
    assert.deepEqual(order, [6, 5, 4, 7, 2, 1])
  })
})

// ---------------------------------------------------------------------------
// Large sequence — stress test for heap invariant
// ---------------------------------------------------------------------------

describe('MinHeap — stress / ordering invariant', () => {
  it('always pops in non-increasing priority (with FIFO tie-break) for 200 random items', () => {
    const h = new MinHeap()
    const priorities = Array.from({ length: 200 }, (_, i) => (i % 10) - 5) // -5 to 4 repeated
    priorities.forEach((p, seq) => h.push(item(p, seq, seq)))

    let prev = null
    while (h.size > 0) {
      const curr = h.pop()
      if (prev !== null) {
        // Priority must be non-increasing
        assert.ok(
          curr.negPriority >= prev.negPriority,
          `Priority regressed: prev negPriority=${prev.negPriority}, curr negPriority=${curr.negPriority}`
        )
        // If same negPriority, insertionSeq must be non-decreasing (FIFO)
        if (curr.negPriority === prev.negPriority) {
          assert.ok(
            curr.insertionSeq > prev.insertionSeq,
            `FIFO violated: prev seq=${prev.insertionSeq}, curr seq=${curr.insertionSeq}`
          )
        }
      }
      prev = curr
    }
  })
})
