/**
 * MinHeap — binary min-heap for async-job-queue priority scheduling.
 *
 * Each item stored in the heap has the shape:
 *   { negPriority: number, insertionSeq: number, jobRecord: object }
 *
 * Comparison key: (negPriority ASC, insertionSeq ASC)
 *   - Lower negPriority  → higher original priority → surfaces first
 *   - Tie-break by lower insertionSeq → FIFO ordering
 *
 * Array-index arithmetic (zero-indexed):
 *   parent(i)     = floor((i - 1) / 2)
 *   leftChild(i)  = 2i + 1
 *   rightChild(i) = 2i + 2
 */
export class MinHeap {
  constructor() {
    /** @type {Array<{negPriority: number, insertionSeq: number, jobRecord: object}>} */
    this._data = []
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /** Number of items currently in the heap. O(1). */
  get size() {
    return this._data.length
  }

  /**
   * Return the minimum item without removing it. O(1).
   * Returns `undefined` when the heap is empty.
   */
  peek() {
    return this._data[0]
  }

  /**
   * Insert an item into the heap. O(log n).
   * @param {{ negPriority: number, insertionSeq: number, jobRecord: object }} item
   */
  push(item) {
    this._data.push(item)
    this._siftUp(this._data.length - 1)
  }

  /**
   * Remove and return the minimum item. O(log n).
   * Returns `undefined` when the heap is empty.
   */
  pop() {
    if (this._data.length === 0) return undefined
    if (this._data.length === 1) return this._data.pop()

    const min = this._data[0]
    // Move the last element to the root and restore the invariant.
    this._data[0] = this._data.pop()
    this._siftDown(0)
    return min
  }

  /**
   * Remove the item whose `jobRecord.id` matches `id`. O(n) scan + O(log n) re-heap.
   * Does nothing if no matching item is found.
   * @param {number} id
   */
  remove(id) {
    const idx = this._data.findIndex(item => item.jobRecord.id === id)
    if (idx === -1) return

    const last = this._data.length - 1
    if (idx === last) {
      // The item to remove is already the last element — just pop it.
      this._data.pop()
      return
    }

    // Replace the target with the last element, then restore heap invariant.
    this._data[idx] = this._data.pop()
    // Both siftUp and siftDown are attempted; exactly one will be a no-op.
    this._siftUp(idx)
    this._siftDown(idx)
  }

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  /**
   * Compare two heap items.
   * Returns negative if `a` should be above `b` (a has higher priority),
   * positive if `b` should be above `a`, zero if equal.
   * @param {{ negPriority: number, insertionSeq: number }} a
   * @param {{ negPriority: number, insertionSeq: number }} b
   */
  _compare(a, b) {
    if (a.negPriority !== b.negPriority) return a.negPriority - b.negPriority
    return a.insertionSeq - b.insertionSeq
  }

  /**
   * Restore the heap invariant upward from index `i`.
   * Repeatedly swaps the item at `i` with its parent while it is smaller.
   * @param {number} i
   */
  _siftUp(i) {
    const data = this._data
    while (i > 0) {
      const parent = Math.floor((i - 1) / 2)
      if (this._compare(data[i], data[parent]) < 0) {
        // Swap child and parent.
        const tmp = data[parent]
        data[parent] = data[i]
        data[i] = tmp
        i = parent
      } else {
        break
      }
    }
  }

  /**
   * Restore the heap invariant downward from index `i`.
   * Repeatedly swaps the item at `i` with its smaller child while a smaller
   * child exists.
   * @param {number} i
   */
  _siftDown(i) {
    const data = this._data
    const n = data.length
    while (true) {
      const left = 2 * i + 1
      const right = 2 * i + 2
      let smallest = i

      if (left < n && this._compare(data[left], data[smallest]) < 0) {
        smallest = left
      }
      if (right < n && this._compare(data[right], data[smallest]) < 0) {
        smallest = right
      }

      if (smallest === i) break

      // Swap with the smaller child.
      const tmp = data[smallest]
      data[smallest] = data[i]
      data[i] = tmp
      i = smallest
    }
  }
}
