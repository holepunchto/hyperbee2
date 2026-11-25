const b4a = require('b4a')
const c = require('compact-encoding')
const Hypercore = require('hypercore')
const { OP_COHORT } = require('./compression.js')
const { Block, encodeBlock, TYPE_COMPAT, TYPE_LATEST } = require('./encoding.js')
const { Pointer, KeyPointer, ValuePointer, TreeNode, INSERTED, NEEDS_SPLIT } = require('./tree.js')
const { inflateValue } = require('./inflate.js')
const compat = require('./compat.js')

const PREFERRED_BLOCK_SIZE = 4096
const INLINE_VALUE_SIZE = 1024
const MAX_BLOCK_SIZE = Hypercore.MAX_SUGGESTED_BLOCK_SIZE
// Hypercore rejects an appended block above 15 MiB
// (Hypercore.MAX_SUGGESTED_BLOCK_SIZE). Grouping keeps a wide margin so the
// encoded block header cannot push a grouped value block over the limit. A
// single value above this threshold is still written as its own block.
const MAX_VALUE_BLOCK_SIZE = 8 * 1024 * 1024
const ESTIMATED_POINTER_SIZE = 12 // conversative, seq=8b, offset=2b, core=2b

module.exports = class WriteBatch {
  constructor(tree, opts = {}) {
    const {
      length = -1,
      key = null,
      mappings = null,
      autoUpdate = true,
      compat = false,
      type = compat ? TYPE_COMPAT : TYPE_LATEST,
      deltaMax = supportsCompression(type) ? 16 : 0,
      deltaMin = supportsCompression(type) ? 1 : Infinity,
      inlineValueSize = INLINE_VALUE_SIZE,
      preferredBlockSize = PREFERRED_BLOCK_SIZE
    } = opts

    this.tree = tree
    this.config = tree.config.options(opts)
    this.deltaMax = deltaMax
    this.deltaMin = deltaMin
    this.inlineValueSize = inlineValueSize
    this.preferredBlockSize = preferredBlockSize
    this.packed = false
    this.packedLastKey = null
    this.packedMaxKeys = 2 * tree.t - 1
    this.packedMinKeys = tree.t - 1
    this.packedSplitAt = 3 * tree.t - 1
    this.snapshot = tree.snapshot()
    this.autoUpdate = autoUpdate
    this.length = length
    this.key = key
    this.type = type
    this.hasLock = false
    this.closed = false
    this.applied = 0
    this.root = null
    this.ops = []
    this.mappings = mappings
  }

  tryPut(key, value) {
    this.checkIfClosed()
    this.ops.push({ put: true, applied: false, key, value })
  }

  tryDelete(key) {
    if (this.packed) throw new Error('Packed writes do not support deletes')
    this.checkIfClosed()
    this.ops.push({ put: false, applied: false, key, value: null })
  }

  tryClear() {
    this.checkIfClosed()
    this.ops = []
    this.length = 0
  }

  _getContext(root) {
    if (!this.key && !root) return this.tree.context
    return this.key ? this.tree.context.getContextByKey(this.key) : root.context
  }

  _getLength(root) {
    if (this.length > -1) return this.length
    return root ? root.seq + 1 : 0
  }

  async lock() {
    if (this.hasLock) return
    await this.tree.context.lock.lock()
    this.hasLock = true
  }

  _unlock() {
    if (!this.hasLock) return
    this.tree.context.lock.unlock()
    this.hasLock = false
  }

  async flush() {
    this.checkIfClosed()
    await this.lock()

    let context = null

    try {
      this.checkIfClosed()

      const ops = this.ops

      const root = await this.tree.bootstrap(this.config)

      const length = this._getLength(root)

      context = this._getContext(root)

      const changed = length === 0
      const seq = length === 0 ? 0 : length - 1
      const t = this.type === TYPE_COMPAT ? compat.t : this.tree.t
      const value = changed ? new TreeNode(t, [], []) : null

      this.length = length
      this.root = context.createTreeNode(0, seq, 0, changed, value)

      for (const op of ops) {
        if (op.put) op.applied = await this._put(op.key, op.value)
        else op.applied = await this._delete(op.key)
        if (op.applied) this.applied++
      }

      if (this.packed && this.applied > 0) await this._rebalancePacked()

      await this._flush()
      await this.close()

      if (this.autoUpdate) {
        this.tree._setRoot(this.root, true)
      }
    } finally {
      if (context) context.cache.flush()
      this._unlock()
    }
  }

  close() {
    this.closed = true
    return this.snapshot.close()
  }

  async _put(key, value) {
    const stack = []
    const target = key
    const snap = this.snapshot
    const conf = this.config

    if (this.packed) {
      if (this.packedLastKey !== null && b4a.compare(target, this.packedLastKey) <= 0) {
        throw new Error('Packed writes require strictly ascending keys')
      }
      this.packedLastKey = target
    }

    let ptr = this.root

    while (true) {
      const v = await retainAndInflate(ptr, snap, conf)
      if (!v.children.ulength) break

      stack.push(ptr)

      let s = 0
      let e = v.keys.ulength
      let c = 0

      while (s < e) {
        const mid = (s + e) >> 1
        const m = v.keys.uget(mid)

        c = b4a.compare(target, m.key)

        if (c === 0) {
          const existing = await inflateValue(m, conf)
          if (b4a.equals(existing, value)) return false
          v.setValue(this.tree.context, mid, value)
          for (let i = 0; i < stack.length; i++) stack[i].mark()
          return true
        }

        if (c < 0) e = mid
        else s = mid + 1
      }

      const i = c < 0 ? e : s
      ptr = v.children.uget(i)
    }

    const v = await retainAndInflate(ptr, snap, conf)
    let status = v.insertLeaf(this.tree.context, target, value)

    // A packed batch is fed in ascending key order. Let the rightmost nodes grow
    // past the maximum instead of splitting halfway, so the left sibling can be
    // filled before it is closed. Everything is split back to a legal size
    // before the batch is committed.
    if (status === NEEDS_SPLIT && this.packed && v.keys.ulength < this.packedSplitAt) {
      status = INSERTED
    }

    if (status >= 0) {
      // already exists, upsert if changed
      const m = v.keys.uget(status)
      const existing = await inflateValue(m, conf)
      if (b4a.equals(existing, value)) return false
      v.setValue(this.tree.context, status, value)
    }

    ptr.mark()
    for (let i = 0; i < stack.length; i++) stack[i].mark()

    while (status === NEEDS_SPLIT) {
      const v = await retainAndInflate(ptr, snap, conf)
      const parent = stack.pop()
      const left = this.packed
        ? Math.min(this.packedMaxKeys, v.keys.ulength - 1 - this.packedMinKeys)
        : null
      const { median, right } = v.split(this.tree.context, left)

      if (parent) {
        const p = await retainAndInflate(parent, snap, conf)
        status = p.insertNode(median, right)
        if (this.packed && status === NEEDS_SPLIT && p.keys.ulength < this.packedSplitAt) {
          status = INSERTED
        }
        ptr = parent
      } else {
        const node = new TreeNode(v.t, [], [])
        this.root = this.tree.context.createTreeNode(0, 0, 0, true, node)
        this.root.value.keys.push(median)
        this.root.value.children.push(ptr)
        this.root.value.children.push(right)
        status = INSERTED
      }
    }

    return true
  }

  // Sorted inserts only ever grow the rightmost path. Split any right-spine
  // node that is still above the maximum in a left-heavy way before the
  // standard flush encodes it. Every committed batch is therefore a legal tree.
  async _rebalancePacked() {
    const snap = this.snapshot
    const conf = this.config
    const stack = []

    let ptr = this.root

    while (true) {
      await retainAndInflate(ptr, snap, conf)
      stack.push(ptr)
      const v = ptr.value
      if (!v.children.ulength) break
      ptr = v.children.uget(v.children.ulength - 1)
    }

    for (let i = stack.length - 1; i >= 0; i--) {
      const node = stack[i]
      let v = node.value

      while (v.keys.ulength > this.packedMaxKeys) {
        const left = Math.max(
          this.packedMinKeys,
          Math.min(this.packedMaxKeys, v.keys.ulength - 1 - this.packedMinKeys)
        )
        const { median, right } = v.split(this.tree.context, left)

        if (i > 0) {
          const parent = stack[i - 1]
          const p = await retainAndInflate(parent, snap, conf)
          p.insertNode(median, right)
          parent.mark()
        } else {
          const root = new TreeNode(v.t, [], [])
          this.root = this.tree.context.createTreeNode(0, 0, 0, true, root)
          this.root.value.keys.push(median)
          this.root.value.children.push(node)
          this.root.value.children.push(right)
          v = root
        }
      }
    }
  }

  async _delete(key) {
    let ptr = this.root

    const stack = []

    while (true) {
      const v = await retainAndInflate(ptr, this.snapshot, this.config)
      stack.push(ptr)

      let s = 0
      let e = v.keys.ulength
      let c = 0

      while (s < e) {
        const mid = (s + e) >> 1
        c = b4a.compare(key, v.keys.uget(mid).key)

        if (c === 0) {
          if (v.children.ulength) await this._setKeyToNearestLeaf(v, mid, stack)
          else v.removeKey(mid)
          // we mark these as changed late, so we don't rewrite them if it is a 404
          for (let i = 0; i < stack.length; i++) stack[i].mark()
          this.root = await this._rebalance(stack)
          return true
        }

        if (c < 0) e = mid
        else s = mid + 1
      }

      if (!v.children.ulength) return false

      const i = c < 0 ? e : s
      ptr = v.children.uget(i)
    }

    return false
  }

  async _setKeyToNearestLeaf(v, index, stack) {
    const snap = this.snapshot
    const conf = this.config

    let left = v.children.uget(index)
    let right = v.children.uget(index + 1)

    const [ls, rs] = await Promise.all([this._leafSize(left, false), this._leafSize(right, true)])

    if (ls < rs) {
      // if fewer leaves on the left
      stack.push(right)
      let r = await retainAndInflate(right, snap, conf)
      while (r.children.ulength) {
        right = r.children.uget(0)
        stack.push(right)
        r = await retainAndInflate(right, snap, conf)
      }
      v.keys.set(index, r.keys.shift())
    } else {
      // if fewer leaves on the right
      stack.push(left)
      let l = await retainAndInflate(left, snap, conf)
      while (l.children.ulength) {
        left = l.children.uget(l.children.ulength - 1)
        stack.push(left)
        l = await retainAndInflate(left, snap, conf)
      }
      v.keys.set(index, l.keys.pop())
    }
  }

  async _leafSize(ptr, goLeft) {
    const snap = this.snapshot
    const conf = this.config

    let v = await retainAndInflate(ptr, snap, conf)
    while (v.children.ulength) {
      ptr = v.children.uget(goLeft ? 0 : v.children.ulength - 1)
      v = await retainAndInflate(ptr, snap, conf)
    }
    return v.keys.ulength
  }

  async _rebalance(stack) {
    const root = stack[0]
    const snap = this.snapshot
    const conf = this.config

    while (stack.length > 1) {
      const ptr = stack.pop()
      const parent = stack[stack.length - 1]

      const v = await retainAndInflate(ptr, snap, conf)
      const minKeys = v.t - 1

      if (v.keys.ulength >= minKeys) return root

      const p = await retainAndInflate(parent, snap, conf)

      let { left, index, right } = v.siblings(p)

      let l = left && (await retainAndInflate(left, snap, conf))

      // maybe borrow from left sibling?
      if (l && l.keys.ulength > minKeys) {
        left.mark()
        v.keys.unshift(p.keys.uget(index - 1))
        if (l.children.ulength) v.children.unshift(l.children.pop())
        p.keys.set(index - 1, l.keys.pop())
        return root
      }

      let r = right && (await retainAndInflate(right, snap, conf))

      // maybe borrow from right sibling?
      if (r && r.keys.ulength > minKeys) {
        right.mark()
        v.keys.push(p.keys.uget(index))
        if (r.children.ulength) v.children.push(r.children.shift())
        p.keys.set(index, r.keys.shift())
        return root
      }

      // merge node with another sibling
      if (l) {
        index--
        r = v
        right = ptr
      } else {
        l = v
        left = ptr
      }

      left.mark()
      l.merge(r, p.keys.uget(index))

      parent.mark()
      p.removeKey(index)
    }

    const r = await retainAndInflate(root, snap, conf)
    // check if the tree shrunk
    if (!r.keys.ulength && r.children.ulength) return r.children.uget(0)
    return root
  }

  _shouldInlineValue(k) {
    if (!k.value || !supportsCompression(this.type)) return true
    if (k.valuePointer) return true
    return k.value.byteLength <= this.inlineValueSize
  }

  async _flush() {
    const context = this.tree.context.getLocalContext()
    const config = this.tree.config

    await context.update(config)

    if (this.mappings !== null) {
      context.mappings.add(this.mappings)
      if (this.root && !this.root.changed && context.mappings.dirty) this.root.mark()
    }

    if (!this.root || !this.root.changed) {
      return
    }

    if (!this.root.value) await this.snapshot.inflate(this.root, this.config)

    let update = { size: 0, nodes: [], keys: [], values: [] }
    let minValue = -1

    this.root = this.root.commit()

    // t is global for the tree, so the root's t applies to every node
    const t = this.root.value.t

    const batch = [update]
    const stack = [this.root]
    const values = []

    while (stack.length > 0) {
      const node = stack.pop()

      if (this.type !== TYPE_COMPAT && update.size >= this.preferredBlockSize) {
        update = { size: 0, nodes: [], keys: [], values: [] }
        batch.push(update)
      }

      update.nodes.push(node)
      update.size += getEstimatedNodeSize(node.value)

      const keys = node.value.keys
      const children = node.value.children

      for (let i = 0; i < keys.entries.length; i++) {
        const k = keys.entries[i]
        if (!k.changed) continue

        if (!this._shouldInlineValue(k)) {
          values.push(k)
          k.valuePointer = new ValuePointer(context, 0, 0, 0, 0)

          if (minValue === -1 || minValue < k.value.byteLength) {
            minValue = k.value.byteLength
          }
        }

        update.keys.push(k)
        update.size += getEstimatedKeySize(k)
      }

      for (let i = 0; i < children.entries.length; i++) {
        const c = children.entries[i]
        if (!c.value || !c.changed) continue
        const node = c.commit()
        children.touch(i, node)
        stack.push(node)
      }
    }

    if (this.type === TYPE_COMPAT) await toCompatType(context, batch, this.ops)

    const length = context.core.length

    // if noop and not genesis, bail early
    const mapping = this.mappings !== null && context.mappings.dirty
    if (this.applied === 0 && this.length > 0 && !mapping) return

    if (minValue > -1 && minValue + update.size < this.preferredBlockSize) {
      // TODO: repack the value into the block
    }

    if (values.length) {
      update = { size: 0, nodes: [], keys: [], values: [] }

      for (let i = 0; i < values.length; i++) {
        const k = values[i]
        const size = getEstimatedValueSize(k)

        // Do not group a value into a block that would cross the block limit. A
        // value larger than the limit is flushed alone instead of being folded
        // into the previous group.
        if (update.values.length > 0 && update.size + size > MAX_VALUE_BLOCK_SIZE) {
          batch.push(update)
          update = { size: 0, nodes: [], keys: [], values: [] }
        }

        update.size += size
        update.values.push(k)

        if (i === values.length - 1 || update.size >= this.preferredBlockSize) {
          batch.push(update)
          update = { size: 0, nodes: [], keys: [], values: [] }
        }
      }
    }

    let records = new Array(batch.length)

    for (let i = 0; i < batch.length; i++) {
      const update = batch[i]
      const seq = length + batch.length - i - 1

      const block = {
        type: this.type,
        checkpoint: 0,
        batch: { start: batch.length - 1 - i, end: i },
        previous: null,
        metadata: null,
        tree: null,
        keys: null,
        values: null,
        cohorts: null,
        t,
        invalidation: context.invalidation
      }

      for (const k of update.values) {
        if (block.values === null) block.values = []
        const ptr = k.valuePointer

        ptr.core = 0
        ptr.context = context
        ptr.seq = seq
        ptr.offset = block.values.length
        ptr.split = 0

        block.values.push(k.value)
        k.value = null // unlinked
      }

      for (const k of update.keys) {
        if (block.keys === null) block.keys = []

        k.core = 0
        k.context = context
        k.seq = seq
        k.offset = block.keys.length

        if (k.valuePointer) updateValuePointerContext(k.valuePointer, context)

        k.unmark()
        block.keys.push(k)
      }

      for (const n of update.nodes) {
        if (block.tree === null) block.tree = []

        n.core = 0
        n.context = context
        n.seq = seq
        n.offset = block.tree.length

        const treeDelta = {
          keys: n.value.keys.flush(this.deltaMax, this.deltaMin),
          children: n.value.children.flush(this.deltaMax, this.deltaMin)
        }

        prepareCohorts(context, block, seq, treeDelta.keys, supportsCompression(this.type))
        prepareCohorts(context, block, seq, treeDelta.children, false)

        n.unmark()
        block.tree.push(treeDelta)
      }

      records[seq - length] = { update, block }
    }

    if (records.length > 0 && this.length > 0) {
      const core = this.key ? await context.getCoreOffsetByKey(this.key, config) : 0
      records[records.length - 1].block.previous = { core, seq: this.length - 1 }
    }

    let flushedMetadata = false

    // TODO: make this transaction safe
    if (context.needsFlush()) {
      context.checkpoint = context.core.length + records.length
      records[records.length - 1].block.metadata = context.flush()
      flushedMetadata = true
    }

    for (const { block } of records) block.checkpoint = context.checkpoint

    let split = false

    // A single large key can share a block with unrelated nodes in older
    // writers. Modern blocks keep the key in the same batch, but in its own
    // key-only block, so the tree node never exceeds Hypercore's block limit.
    if (this.type === TYPE_LATEST) {
      const oversized = records.filter(
        ({ block }) => block.tree !== null && preencodeBlock(block) > MAX_BLOCK_SIZE
      )

      if (oversized.length > 0) {
        split = true
        const keyRecords = []

        for (const record of oversized) {
          const block = record.block

          if (block.keys === null || block.keys.length === 0) {
            throw oversizedBlockError('tree', preencodeBlock(block))
          }

          while (block.keys.length > 0 && preencodeBlock(block) > MAX_BLOCK_SIZE) {
            const index = largestKeyIndex(block.keys)
            const key = block.keys.splice(index, 1)[0]

            keyRecords.push({
              update: { size: 0, nodes: [], keys: [key], values: [] },
              block: createKeyBlock(this.type, t, context, key)
            })
          }

          if (block.keys.length === 0) block.keys = null
          if (preencodeBlock(block) > MAX_BLOCK_SIZE) {
            throw oversizedBlockError('tree', preencodeBlock(block))
          }
        }

        const values = records.filter(({ block }) => block.values !== null)
        const trees = records.filter(({ block }) => block.values === null)

        // Keep value blocks at their existing sequences and insert the key-only
        // blocks before the tree blocks. The root stays last in the batch.
        records = values.concat(keyRecords, trees)

        if (flushedMetadata) context.checkpoint = context.core.length + records.length
      }
    }

    const nodeSeq = new Map()

    for (let i = 0; i < records.length; i++) {
      const { update, block } = records[i]

      block.seq = length + i
      block.batch = { start: i, end: records.length - 1 - i }
      block.invalidation = context.invalidation

      for (let j = 0; j < update.values.length; j++) {
        const ptr = update.values[j].valuePointer

        ptr.core = 0
        ptr.context = context
        ptr.seq = block.seq
        ptr.offset = j
        ptr.split = 0
      }

      if (block.keys !== null) {
        for (let j = 0; j < block.keys.length; j++) {
          const k = block.keys[j]

          k.core = 0
          k.context = context
          k.seq = block.seq
          k.offset = j

          if (k.valuePointer) updateValuePointerContext(k.valuePointer, context)

          // Moved keys were already unmarked before their cohorts were
          // prepared, so the cohorts still reference these pointers and encode
          // their final block offsets.
          k.unmark()
        }
      }

      if (block.tree !== null) {
        for (let j = 0; j < update.nodes.length; j++) {
          const n = update.nodes[j]

          n.core = 0
          n.context = context
          n.seq = block.seq
          n.offset = j

          nodeSeq.set(n, block.seq)
        }
      }
    }

    for (const { block } of records) {
      if (block.tree === null) continue

      for (const delta of block.tree) {
        remapTreeDelta(delta, block, nodeSeq)
      }
    }

    const buffers = new Array(records.length)

    for (let i = 0; i < records.length; i++) {
      const block = records[i].block

      block.checkpoint = context.checkpoint
      block.invalidation = context.invalidation
      buffers[i] = encodeBlock(block)

      if (split && buffers[i].byteLength > MAX_BLOCK_SIZE) {
        const kind = block.tree !== null ? 'tree' : block.keys !== null ? 'key' : 'value'
        throw oversizedBlockError(kind, buffers[i].byteLength)
      }
    }

    this.checkIfClosed()

    await context.core.append(buffers)

    for (let i = 0; i < batch.length; i++) {
      const update = batch[i]

      for (let j = 0; j < update.nodes.length; j++) {
        const node = update.nodes[j]
        this.snapshot.bump(node)
      }
    }
  }

  checkIfClosed() {
    if (this.closed) {
      throw new Error('Write batch is closed')
    }
  }
}

function createKeyBlock(type, t, context, key) {
  return {
    type,
    checkpoint: context.checkpoint,
    batch: { start: 0, end: 0 },
    previous: null,
    metadata: null,
    tree: null,
    keys: [key],
    values: null,
    cohorts: null,
    t,
    invalidation: context.invalidation
  }
}

function largestKeyIndex(keys) {
  let index = 0
  let size = keyEntrySize(keys[0])

  for (let i = 1; i < keys.length; i++) {
    const next = keyEntrySize(keys[i])
    if (next <= size) continue
    index = i
    size = next
  }

  return index
}

function keyEntrySize(key) {
  return key.key.byteLength + (key.value ? key.value.byteLength : 0)
}

function preencodeBlock(block) {
  const state = c.state()
  Block.preencode(state, block)
  return state.end
}

function remapTreeDelta(delta, block, nodeSeq) {
  const pointer = delta.pointer

  if (pointer !== null && pointer !== undefined) {
    if (delta.type === OP_COHORT) {
      const offset = pointer.offset

      // A changed cohort owns a fresh pointer into this block. Unchanged
      // cohorts keep pointing at the block where they were first written.
      if (
        block.cohorts !== null &&
        offset < block.cohorts.length &&
        delta.deltas === block.cohorts[offset]
      ) {
        pointer.core = 0
        pointer.seq = block.seq
      }
    } else if (nodeSeq.has(pointer)) {
      pointer.seq = nodeSeq.get(pointer)
    }
  }

  if (delta.deltas === null || delta.deltas === undefined) return

  for (const d of delta.deltas) {
    if (d.pointer && nodeSeq.has(d.pointer)) d.pointer.seq = nodeSeq.get(d.pointer)
  }
}

function oversizedBlockError(kind, size) {
  const err = new Error(
    'Encoded ' +
      kind +
      ' block exceeds Hypercore.MAX_SUGGESTED_BLOCK_SIZE (' +
      size +
      ' > ' +
      MAX_BLOCK_SIZE +
      ')'
  )

  err.code = 'BLOCK_TOO_LARGE'
  return err
}

async function toCompatType(context, batch, ops) {
  const map = new Map()
  let index = 0

  for (const k of batch[0].keys) {
    map.set(b4a.toString(k.key, 'hex'), k)
  }

  for (let i = ops.length - 1; i >= 0; i--) {
    const op = ops[i]
    if (!op.put && !op.applied) continue

    const k = map.get(b4a.toString(op.key, 'hex'))

    const j = index++
    if (j === batch.length) batch.push({ size: 0, nodes: [], keys: [], values: [] })
    batch[j].keys = [k || new KeyPointer(context, 0, 0, 0, false, op.key, op.value, null)]
  }

  // compat doesnt support block 0
  if (context.core.length > 0) return

  const header = b4a.from('0a086879706572626565', 'hex')
  await context.core.append(header)
}

function updateValuePointerContext(valuePointer, context) {
  valuePointer.core = context.getCoreOffsetLocal(valuePointer.context, valuePointer.core)
  valuePointer.context = context
}

// TODO: this isnt right anymore as we compress the delta post flush, but prob ok...
function getEstimatedNodeSize(n) {
  return (
    n.keys.delta.length * ESTIMATED_POINTER_SIZE + n.children.delta.length * ESTIMATED_POINTER_SIZE
  )
}

function getEstimatedKeySize(k) {
  return k.key.byteLength + (k.valuePointer ? ESTIMATED_POINTER_SIZE : 0)
}

function getEstimatedValueSize(k) {
  return k.value.byteLength
}

function prepareCohorts(context, block, seq, deltas, keys) {
  for (const d of deltas) {
    // same below but the delta might be a noop (ie add and the delete) - handle that
    if (keys && d.changed && d.pointer && d.pointer.changed) d.pointer = null

    if (d.pointer && d.pointer.context !== context) {
      const p = d.pointer
      p.core = context.getCoreOffsetLocal(p.context, p.core)
      p.context = context
    }

    if (d.type !== OP_COHORT || !d.changed) continue
    if (block.cohorts === null) block.cohorts = []

    d.unmark()
    d.pointer = new Pointer(context, 0, seq, block.cohorts.length)

    for (const dd of d.deltas) {
      if (keys && dd.changed && dd.pointer && dd.pointer.changed) dd.pointer = null
      dd.unmark()
      const p = dd.pointer
      if (!p) continue
      p.core = context.getCoreOffsetLocal(p.context, p.core)
      p.context = context
    }

    block.cohorts.push(d.deltas)
  }
}

function supportsCompression(type) {
  return type !== TYPE_COMPAT && type !== 0
}

async function retainAndInflate(ptr, snap, conf) {
  if (ptr.value) {
    ptr.retain()
    return ptr.value
  }
  ptr.retain()
  return await snap.inflate(ptr, conf)
}
