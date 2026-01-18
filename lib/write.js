const b4a = require('b4a')
const c = require('compact-encoding')
const { OP_COHORT } = require('./compression.js')
const { encodeBlock, TYPE_COMPAT, TYPE_LATEST } = require('./encoding.js')
const { Pointer, KeyPointer, ValuePointer, TreeNode, INSERTED, NEEDS_SPLIT } = require('./tree.js')

const PREFERRED_BLOCK_SIZE = 4096
const INLINE_VALUE_SIZE = 4096
const ESTIMATED_POINTER_SIZE = 12 // conversative, seq=8b, offset=2b, core=2b

module.exports = class WriteBatch {
  constructor(tree, opts = {}) {
    const {
      length = -1,
      key = null,
      autoUpdate = true,
      compat = false,
      type = compat ? TYPE_COMPAT : TYPE_LATEST,
      deltaMax = supportsCompression(type) ? 16 : 0,
      deltaMin = supportsCompression(type) ? 1 : Infinity,
      inlineValueSize = INLINE_VALUE_SIZE,
      preferredBlockSize = PREFERRED_BLOCK_SIZE
    } = opts

    this.tree = tree
    this.deltaMax = deltaMax
    this.deltaMin = deltaMin
    this.inlineValueSize = inlineValueSize
    this.preferredBlockSize = preferredBlockSize
    this.snapshot = tree.snapshot()
    this.autoUpdate = autoUpdate
    this.length = length
    this.key = key
    this.type = type
    this.closed = false
    this.applied = 0
    this.root = null
    this.ops = []
  }

  tryPut(key, value) {
    this.ops.push({ put: true, applied: false, key, value })
  }

  tryDelete(key) {
    this.ops.push({ put: false, applied: false, key, value: null })
  }

  tryClear() {
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

  async flush() {
    const lock = this.tree.context.lock
    await lock.lock()

    try {
      const ops = this.ops

      const root = await this.tree.bootstrap()

      const length = this._getLength(root)
      const context = this._getContext(root)

      const changed = length === 0
      const seq = length === 0 ? 0 : length - 1
      const value = changed ? new TreeNode([], []) : null

      this.length = length
      this.root = context.createTreeNode(0, seq, 0, changed, value)

      for (const op of ops) {
        if (op.put) op.applied = await this._put(op.key, op.value)
        else op.applied = await this._delete(op.key)
        if (op.applied) this.applied++
      }

      await this._flush()
      await this.snapshot.close()

      if (this.autoUpdate) {
        this.tree.update(this.root)
      }
    } finally {
      lock.unlock()
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

    let ptr = this.root

    while (true) {
      const v = await retainAndInflate(ptr, snap)
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
          const existing = await snap.inflateValue(m)
          if (b4a.equals(existing, value)) return false
          v.setValue(this.tree.context, mid, value)
          for (let i = 0; i < stack.length; i++) stack[i].changed = true
          return true
        }

        if (c < 0) e = mid
        else s = mid + 1
      }

      const i = c < 0 ? e : s
      ptr = v.children.uget(i)
    }

    const v = await retainAndInflate(ptr, snap)
    let status = v.insertLeaf(this.tree.context, target, value)

    if (status >= 0) {
      // already exists, upsert if changed
      const m = v.keys.uget(status)
      const existing = await snap.inflateValue(m)
      if (b4a.equals(existing, value)) return false
      v.setValue(this.tree.context, status, value)
    }

    ptr.changed = true
    for (let i = 0; i < stack.length; i++) stack[i].changed = true

    while (status === NEEDS_SPLIT) {
      const v = await retainAndInflate(ptr, snap)
      const parent = stack.pop()
      const { median, right } = v.split(this.tree.context)

      if (parent) {
        const p = await retainAndInflate(parent, snap)
        status = p.insertNode(this.tree.context, median, right)
        ptr = parent
      } else {
        this.root = this.tree.context.createTreeNode(0, 0, 0, true, new TreeNode([], []))
        this.root.value.keys.push(median)
        this.root.value.children.push(ptr)
        this.root.value.children.push(right)
        status = INSERTED
      }
    }

    return true
  }

  async _delete(key) {
    let ptr = this.root

    const stack = []

    while (true) {
      const v = await retainAndInflate(ptr, this.snapshot)
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
          for (let i = 0; i < stack.length; i++) stack[i].changed = true
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

    let left = v.children.uget(index)
    let right = v.children.uget(index + 1)

    const [ls, rs] = await Promise.all([this._leafSize(left, false), this._leafSize(right, true)])

    if (ls < rs) {
      // if fewer leaves on the left
      stack.push(right)
      let r = await retainAndInflate(right, snap)
      while (r.children.ulength) {
        right = r.children.uget(0)
        stack.push(right)
        r = await retainAndInflate(right, snap)
      }
      v.keys.set(index, r.keys.shift())
    } else {
      // if fewer leaves on the right
      stack.push(left)
      let l = await retainAndInflate(left, snap)
      while (l.children.ulength) {
        left = l.children.uget(l.children.ulength - 1)
        stack.push(left)
        l = await retainAndInflate(left, snap)
      }
      v.keys.set(index, l.keys.pop())
    }
  }

  async _leafSize(ptr, goLeft) {
    const snap = this.snapshot

    let v = await retainAndInflate(ptr, snap)
    while (v.children.ulength) {
      ptr = v.children.uget(goLeft ? 0 : v.children.ulength - 1)
      v = await retainAndInflate(ptr, snap)
    }
    return v.keys.ulength
  }

  async _rebalance(stack) {
    const root = stack[0]
    const minKeys = this.tree.context.minKeys
    const snap = this.snapshot

    while (stack.length > 1) {
      const ptr = stack.pop()
      const parent = stack[stack.length - 1]

      const v = await retainAndInflate(ptr, snap)

      if (v.keys.ulength >= minKeys) return root

      const p = await retainAndInflate(parent, snap)

      let { left, index, right } = v.siblings(p)

      let l = left && (await retainAndInflate(left, snap))

      // maybe borrow from left sibling?
      if (l && l.keys.ulength > minKeys) {
        left.changed = true
        v.keys.unshift(p.keys.uget(index - 1))
        if (l.children.ulength) v.children.unshift(l.children.pop())
        p.keys.set(index - 1, l.keys.pop())
        return root
      }

      let r = right && (await retainAndInflate(right, snap))

      // maybe borrow from right sibling?
      if (r && r.keys.ulength > minKeys) {
        right.changed = true
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

      left.changed = true
      l.merge(r, p.keys.uget(index))

      parent.changed = true
      p.removeKey(index)
    }

    const r = await retainAndInflate(root, snap)
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
    if (!this.root || !this.root.changed) {
      return
    }

    if (!this.root.value) await this.snapshot.inflate(this.root)

    let update = { size: 0, nodes: [], keys: [], values: [] }
    let minValue = -1

    this.root = this.root.commit()

    const batch = [update]
    const stack = [this.root]
    const values = []

    const context = this.tree.context.getLocalContext()
    const activeRequests = this.tree.activeRequests

    await context.update(activeRequests)

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
    if (this.applied === 0 && length > 0) return

    if (minValue > -1 && minValue + update.size < this.preferredBlockSize) {
      // TODO: repack the value into the block
    }

    if (values.length) {
      update = { size: 0, nodes: [], keys: [], values: [] }

      for (let i = 0; i < values.length; i++) {
        const k = values[i]

        update.size += getEstimatedValueSize(k)
        update.values.push(k)

        if (i === values.length - 1 || update.size >= this.preferredBlockSize) {
          batch.push(update)
          update = { size: 0, nodes: [], keys: [], values: [] }
        }
      }
    }

    const blocks = new Array(batch.length)

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
        cohorts: null
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
        k.changed = false

        if (k.valuePointer) updateValuePointerContext(k.valuePointer, context)

        block.keys.push(k)
      }

      for (const n of update.nodes) {
        if (block.tree === null) block.tree = []

        n.core = 0
        n.context = context
        n.seq = seq
        n.offset = block.tree.length
        n.changed = false

        const treeDelta = {
          keys: n.value.keys.flush(this.deltaMax, this.deltaMin),
          children: n.value.children.flush(this.deltaMax, this.deltaMin)
        }

        prepareCohorts(context, block, seq, treeDelta.keys, supportsCompression)
        prepareCohorts(context, block, seq, treeDelta.children, false)

        block.tree.push(treeDelta)
      }

      blocks[seq - length] = block
    }

    const buffers = new Array(blocks.length)

    if (blocks.length > 0 && this.length > 0) {
      const core = this.key ? await context.getCoreOffsetByKey(this.key, activeRequests) : 0
      blocks[blocks.length - 1].previous = { core, seq: this.length - 1 }
    }

    // TODO: make this transaction safe
    if (context.changed) {
      context.checkpoint = context.core.length + blocks.length
      blocks[blocks.length - 1].metadata = context.flush()
    }

    for (let i = 0; i < blocks.length; i++) {
      blocks[i].checkpoint = context.checkpoint
      buffers[i] = encodeBlock(blocks[i])
    }

    if (this.closed) {
      throw new Error('Write batch is closed')
    }

    await context.core.append(buffers)

    for (let i = 0; i < batch.length; i++) {
      const update = batch[i]

      for (let j = 0; j < update.nodes.length; j++) {
        const node = update.nodes[j]
        this.snapshot.bump(node)
      }
    }

    context.cache.retained++
    context.cache.gc()
  }
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

    d.changed = false
    d.pointer = new Pointer(context, 0, seq, block.cohorts.length)

    for (const dd of d.deltas) {
      if (keys && dd.changed && dd.pointer && dd.pointer.changed) dd.pointer = null
      dd.changed = false
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

async function retainAndInflate(ptr, snap) {
  if (ptr.value) {
    ptr.retain()
    return ptr.value
  }
  ptr.retain()
  return await snap.inflate(ptr)
}
