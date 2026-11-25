const WriteBatch = require('./write.js')
const { KeyPointer, ValuePointer, TreeNode } = require('./tree.js')
const { CompressedArray, DeltaOp, DeltaCohort, OP_INSERT } = require('./compression.js')
const { TYPE_LATEST } = require('./encoding.js')

const { appendBlocks, INLINE_VALUE_SIZE, PREFERRED_BLOCK_SIZE, DELTA_MAX } = WriteBatch

const APPEND_QUEUE = 32

// Builds a hyperbee tree directly from an ordered stream of entries. Leaves and
// internal nodes are filled in order and separator entries are promoted exactly
// once, so a loaded tree is packed instead of the roughly half full tree that
// sorted insertion produces. Blocks are appended as soon as a node can no
// longer take part in the final tail rebalance, which keeps the buffer to the
// right spine, one node per level and the pending value block.
module.exports = class BulkLoader {
  constructor(destination) {
    this.destination = destination
    this.config = destination.config
    this.context = destination.context.getLocalContext()
    this.t = destination.t
    this.type = TYPE_LATEST
    this.maxKeys = 2 * this.t - 1
    this.minKeys = this.t - 1
    this.maxChildren = 2 * this.t
    this.inlineValueSize = INLINE_VALUE_SIZE
    this.preferredBlockSize = PREFERRED_BLOCK_SIZE
    this.deltaMax = DELTA_MAX
    this.firstSeq = this.context.core.length
    this.entries = 0
    this.values = []
    this.valueBytes = 0
    this.queue = []
    this.root = null
    this.leaf = new LeafLevel(this)
    this.locked = false
    this.closed = false
  }

  static async open(destination) {
    const loader = new BulkLoader(destination)
    await loader._open()
    return loader
  }

  async _open() {
    await this.context.core.ready()
    await this.context.update(this.config)
    await this.context.lock.lock()
    this.locked = true
  }

  async append(entry) {
    this.entries++

    const value = entry.value
    let valuePointer = null

    if (value !== null && value.byteLength > this.inlineValueSize) {
      valuePointer = new ValuePointer(this.context, 0, 0, 0, 0)
    }

    const key = new KeyPointer(this.context, 0, 0, 0, true, entry.key, value, valuePointer)

    if (valuePointer !== null) {
      this.values.push(key)
      this.valueBytes += value.byteLength

      if (this.valueBytes >= this.preferredBlockSize) await this._flushValues()
    }

    await this.leaf.push(key)
  }

  async finish() {
    if (this.entries === 0) return null

    let level = this.leaf

    while (!(await level.finish())) {
      if (level.parent === null) throw new Error('Bulk loader lost its root')
      level = level.parent
    }

    return this.root
  }

  async appendNode(node, { root = false } = {}) {
    // A node block points at its value blocks, so queue them first. The queue
    // is written child first; appendBlocks assigns the higher sequence numbers
    // to the parents at the front of the reversed batch.
    await this._flushValues()

    if (root) {
      await this._flushQueue()
      await this._writeEntries([{ node }], this.firstSeq)
      this.root = node.ptr
      return node.ptr
    }

    this.queue.push({ node })
    await this._maybeFlushQueue()

    return node.ptr
  }

  async _flushQueue() {
    if (this.queue.length === 0) return

    const entries = this.queue
    this.queue = []

    await this._writeEntries(entries, null)
  }

  async _maybeFlushQueue() {
    if (this.queue.length >= APPEND_QUEUE) await this._flushQueue()
  }

  async _writeEntries(entries, batchOffset) {
    const nodes = []

    for (const entry of entries) {
      if (entry.node) nodes.push(entry.node)
    }

    // Reserve every pointer first so a parent can reference a child that is
    // written in the same batch. Encoding happens after the whole batch has
    // been prepared, when every pointer has its final sequence number.
    for (const node of nodes) {
      if (node.ptr) continue
      node.ptr = this.context.createTreeNode(0, 0, 0, true, new TreeNode(this.t, [], []))
    }

    const updates = new Array(entries.length)

    for (let i = 0; i < entries.length; i++) {
      const entry = entries[i]

      if (entry.values) {
        updates[i] = { size: 0, nodes: [], keys: [], values: entry.values }
        continue
      }

      const value = entry.node.ptr.value

      value.keys = packedArray(entry.node.keys)
      value.children = packedArray(entry.node.children.map((child) => child.ptr))

      updates[i] = { size: 0, nodes: [entry.node.ptr], keys: entry.node.keys, values: [] }
    }

    await appendBlocks(this.context, updates.reverse(), {
      type: this.type,
      t: this.t,
      deltaMax: this.deltaMax,
      deltaMin: 0,
      config: this.config,
      previous: null,
      // Only the root block spans every block written by the import, so its
      // batch rewinds to the start of the genesis version.
      batchOffset
    })
  }

  async _flushValues() {
    if (this.values.length === 0) return

    const values = this.values
    this.values = []
    this.valueBytes = 0

    this.queue.push({ values })
    await this._maybeFlushQueue()
  }

  close() {
    if (this.closed) return
    this.closed = true

    // Nodes and values are only appended once the queue is complete; anything
    // still buffered was not committed and must be dropped on success and
    // failure alike.
    this.values = []
    this.valueBytes = 0
    this.queue = []

    if (this.locked) {
      this.context.cache.flush()
      this.context.lock.unlock()
      this.locked = false
    }
  }
}

class LeafLevel {
  constructor(loader) {
    this.loader = loader
    this.depth = 0
    this.open = []
    this.pending = null
    this.parent = null
  }

  async push(key) {
    if (this.open.length < this.loader.maxKeys) {
      this.open.push(key)
      return
    }

    const node = { keys: this.open, children: [] }

    this.open = []

    // The incoming entry is the separator after the closed leaf, so no entry is
    // ever copied into two nodes.
    await this._close(node, key)
  }

  async _close(node, sep) {
    const pending = this.pending
    this.pending = { node, sep }

    // A full closed node can no longer be the tail, so the previous one is now
    // safe to write. The latest closed node stays buffered for the final
    // rebalance.
    if (pending !== null) await this._emit(pending)
  }

  async _emit(record) {
    if (this.parent === null) this.parent = new NodeLevel(this.loader, 1)

    await this.loader.appendNode(record.node)
    await this.parent.accept(record.node, record.sep)
  }

  async finish() {
    const node = { keys: this.open, children: [] }

    if (this.pending === null) {
      await this.loader.appendNode(node, { root: true })
      return true
    }

    if (node.keys.length < this.loader.minKeys) {
      rebalance(this.pending, { node, sep: null }, this.loader)
    }

    await this._emit(this.pending)
    await this._emit({ node, sep: null })

    return false
  }
}

class NodeLevel {
  constructor(loader, depth) {
    this.loader = loader
    this.depth = depth
    this.open = { keys: [], children: [] }
    this.pending = null
    this.pendingSep = null
    this.emitted = 0
    this.parent = null
  }

  async accept(child, sep) {
    const open = this.open

    if (open.children.length === 0) {
      open.children.push(child)
      this.pendingSep = sep
      return
    }

    if (open.children.length === this.loader.maxChildren) {
      const node = open
      const promoted = this.pendingSep

      this.open = { keys: [], children: [] }
      this.open.children.push(child)

      this.pendingSep = sep

      await this._close(node, promoted)
      return
    }

    open.keys.push(this.pendingSep)
    open.children.push(child)

    this.pendingSep = sep
  }

  async _close(node, promoted) {
    const pending = this.pending
    this.pending = { node, sep: promoted }

    if (pending !== null) await this._emit(pending)
  }

  async _emit(record) {
    this.emitted++

    if (this.parent === null) this.parent = new NodeLevel(this.loader, this.depth + 1)

    await this.loader.appendNode(record.node)
    await this.parent.accept(record.node, record.sep)
  }

  async finish() {
    const open = this.open
    const node = { keys: open.keys, children: open.children }

    if (this.emitted === 0 && this.pending === null) {
      await this.loader.appendNode(node, { root: true })
      return true
    }

    if (node.keys.length < this.loader.minKeys) {
      rebalance(this.pending, { node, sep: this.pendingSep }, this.loader)
    }

    await this._emit(this.pending)
    await this._emit({ node, sep: this.pendingSep })

    return false
  }
}

// Redistribute the final two siblings around their separator. `previous` was
// closed at full occupancy and `next` is the possibly underfilled tail, so a
// balanced split is always legal for both.
function rebalance(previous, next, loader) {
  const maxKeys = loader.maxKeys
  const keys = previous.node.keys.concat([previous.sep], next.node.keys)

  let left = keys.length >> 1
  if (left > maxKeys) left = maxKeys
  if (keys.length - 1 - left > maxKeys) left = keys.length - 1 - maxKeys

  previous.node.keys = keys.slice(0, left)
  previous.sep = keys[left]
  next.node.keys = keys.slice(left + 1)

  if (previous.node.children.length > 0) {
    const children = previous.node.children.concat(next.node.children)

    previous.node.children = children.slice(0, left + 1)
    next.node.children = children.slice(left + 1)
  }
}

function packedArray(entries) {
  if (entries.length === 0) return new CompressedArray([])

  const deltas = new Array(entries.length)

  for (let i = 0; i < entries.length; i++) {
    deltas[i] = new DeltaOp(true, OP_INSERT, i, entries[i])
  }

  return new CompressedArray([new DeltaCohort(true, null, deltas)])
}
