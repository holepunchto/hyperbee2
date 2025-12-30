const b4a = require('b4a')
const c = require('compact-encoding')
const { encodeBlock } = require('./encoding.js')
const {
  KeyPointer,
  ValuePointer,
  TreeNode,
  TreeNodePointer,
  MIN_KEYS,
  INSERTED,
  NEEDS_SPLIT
} = require('./tree.js')

const PREFERRED_BLOCK_SIZE = 4096
const INLINE_VALUE_SIZE = 0

module.exports = class WriteBatch {
  constructor(tree, { length = -1, key = null, autoUpdate = true, blockFormat = 3 } = {}) {
    this.tree = tree
    this.inline = 0
    this.snapshot = tree.snapshot()
    this.autoUpdate = autoUpdate
    this.length = length
    this.key = key
    this.blockFormat = blockFormat
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

      this.length = length
      this.root = new TreeNodePointer(
        context,
        0,
        seq,
        0,
        changed,
        changed ? new TreeNode([], []) : null
      )

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

    let ptr = this.root

    while (true) {
      const v = ptr.value ? this.snapshot.bump(ptr) : await this.snapshot.inflate(ptr)
      if (!v.children.length) break

      stack.push(ptr)

      let s = 0
      let e = v.keys.length
      let c = 0

      while (s < e) {
        const mid = (s + e) >> 1
        const m = v.keys[mid]

        c = b4a.compare(target, m.key)

        if (c === 0) {
          const existing = await this.snapshot.inflateValue(m)
          if (b4a.equals(existing, value)) return false
          v.setValue(this.tree.context, mid, value)
          for (let i = 0; i < stack.length; i++) stack[i].changed = true
          return true
        }

        if (c < 0) e = mid
        else s = mid + 1
      }

      const i = c < 0 ? e : s
      ptr = v.children[i]
    }

    const v = ptr.value ? this.snapshot.bump(ptr) : await this.snapshot.inflate(ptr)
    let status = v.insert(this.tree.context, target, value, null, null)

    if (status >= 0) { // already exists, upsert if changed
      const m = v.keys[status]
      const existing = await this.snapshot.inflateValue(m)
      if (b4a.equals(existing, value)) return false
      v.setValue(this.tree.context, status, value)
    }

    ptr.changed = true

    for (let i = 0; i < stack.length; i++) stack[i].changed = true

    while (status === NEEDS_SPLIT) {
      const v = ptr.value ? this.snapshot.bump(ptr) : await this.snapshot.inflate(ptr)
      const parent = stack.pop()
      const { median, right } = v.split(this.tree.context)

      if (parent) {
        const p = parent.value ? this.snapshot.bump(parent) : await this.snapshot.inflate(parent)
        status = p.insert(this.tree.context, median.key, median.value, median.valuePointer, right)
        ptr = parent
      } else {
        this.root = new TreeNodePointer(this.tree.context, 0, 0, 0, true, new TreeNode([], []))
        this.root.value.keys.push(median)
        this.root.value.children.push(ptr, right)
        this.snapshot.bump(this.root)
        status = INSERTED
      }
    }

    return true
  }

  async _delete(key) {
    let ptr = this.root

    const stack = []

    while (true) {
      const v = ptr.value ? this.snapshot.bump(ptr) : await this.snapshot.inflate(ptr)
      stack.push(ptr)

      let s = 0
      let e = v.keys.length
      let c = 0

      while (s < e) {
        const mid = (s + e) >> 1
        c = b4a.compare(key, v.keys[mid].key)

        if (c === 0) {
          if (v.children.length) await this._setKeyToNearestLeaf(v, mid, stack)
          else v.removeKey(mid)

          // we mark these as changed late, so we don't rewrite them if it is a 404
          for (let i = 0; i < stack.length; i++) stack[i].changed = true
          this.root = await this._rebalance(stack)
          return true
        }

        if (c < 0) e = mid
        else s = mid + 1
      }

      if (!v.children.length) return false

      const i = c < 0 ? e : s
      ptr = v.children[i]
    }

    return false
  }

  async _setKeyToNearestLeaf(v, index, stack) {
    let left = v.children[index]
    let right = v.children[index + 1]

    const [ls, rs] = await Promise.all([this._leafSize(left, false), this._leafSize(right, true)])

    if (ls < rs) {
      // if fewer leaves on the left
      stack.push(right)
      let r = right.value ? this.snapshot.bump(right) : await this.snapshot.inflate(right)
      while (r.children.length) {
        right = r.children[0]
        stack.push(right)
        r = right.value ? this.snapshot.bump(right) : await this.snapshot.inflate(right)
      }
      v.keys[index] = r.keys.shift()
    } else {
      // if fewer leaves on the right
      stack.push(left)
      let l = left.value ? this.snapshot.bump(left) : await this.snapshot.inflate(left)
      while (l.children.length) {
        left = l.children[l.children.length - 1]
        stack.push(left)
        l = left.value ? this.snapshot.bump(left) : await this.snapshot.inflate(left)
      }
      v.keys[index] = l.keys.pop()
    }
  }

  async _leafSize(ptr, goLeft) {
    let v = ptr.value ? this.snapshot.bump(ptr) : await this.snapshot.inflate(ptr)
    while (v.children.length) {
      ptr = v.children[goLeft ? 0 : v.children.length - 1]
      v = ptr.value ? this.snapshot.bump(ptr) : await this.snapshot.inflate(ptr)
    }
    return v.keys.length
  }

  async _rebalance(stack) {
    const root = stack[0]

    while (stack.length > 1) {
      const ptr = stack.pop()
      const parent = stack[stack.length - 1]

      const v = ptr.value ? this.snapshot.bump(ptr) : await this.snapshot.inflate(ptr)

      if (v.keys.length >= MIN_KEYS) return root

      const p = parent.value ? this.snapshot.bump(parent) : await this.snapshot.inflate(parent)

      let { left, index, right } = v.siblings(p)

      let l = left && (left.value ? this.snapshot.bump(left) : await this.snapshot.inflate(left))

      // maybe borrow from left sibling?
      if (l && l.keys.length > MIN_KEYS) {
        left.changed = true
        v.keys.unshift(p.keys[index - 1])
        if (l.children.length) v.children.unshift(l.children.pop())
        p.keys[index - 1] = l.keys.pop()
        return root
      }

      let r =
        right && (right.value ? this.snapshot.bump(right) : await this.snapshot.inflate(right))

      // maybe borrow from right sibling?
      if (r && r.keys.length > MIN_KEYS) {
        right.changed = true
        v.keys.push(p.keys[index])
        if (r.children.length) v.children.push(r.children.shift())
        p.keys[index] = r.keys.shift()
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
      l.merge(r, p.keys[index])

      parent.changed = true
      p.removeKey(index)
    }

    const r = root.value ? this.snapshot.bump(root) : await this.snapshot.inflate(root)
    // check if the tree shrunk
    if (!r.keys.length && r.children.length) return r.children[0]
    return root
  }

  async _flush() {
    if (!this.root || !this.root.changed) {
      // console.log('bail?')
      // return
    }

    if (!this.root.value) await this.snapshot.inflate(this.root)

    let update = { size: 0, nodes: [], keys: [], values: [] }
    let minValue = -1

    const batch = [update]
    const stack = [this.root]
    const values = []

    const context = this.tree.context.getLocalContext()
    const activeRequests = this.tree.activeRequests

    await context.update(activeRequests)

    while (stack.length > 0) {
      const node = stack.pop()

      if (this.blockFormat > 2 && update.size >= PREFERRED_BLOCK_SIZE) {
        update = { size: 0, nodes: [], keys: [], values: [] }
        batch.push(update)
      }

      node.changed = false

      update.nodes.push(node)
      update.size += 16

      // note that in practice that the below getCoreOffsets are sync
      // in practice since we did the update above on the ctx

      for (let i = 0; i < node.value.keys.length; i++) {
        const k = node.value.keys[i]
        const ptr = k.valuePointer

        if (!k.changed) continue

        k.changed = false
        update.keys.push(k)
        update.size += k.key.byteLength + 16

        if (k.value && k.value.byteLength > INLINE_VALUE_SIZE && !ptr) {
          values.push(k)
          if (minValue === -1 || minValue < k.value.byteLength) {
            minValue = k.value.byteLength
          }
        }
      }

      for (let i = 0; i < node.value.children.length; i++) {
        const n = node.value.children[i]
        if (!n.changed) continue

        stack.push(n)
      }
    }

    const length = context.core.length

    // if noop and not genesis, bail early
    if (this.applied === 0 && length > 0) {
      return
    }

    if (minValue > -1 && minValue + update.size < PREFERRED_BLOCK_SIZE) {
      // console.log('AUTO BATCH', minValue, values)
    }

    if (this.blockFormat === 2) toBlockFormat2(context, batch, this.ops)

    if (values.length) {
      update = { size: 0, nodes: [], keys: [], values: [] }

      for (let i = 0; i < values.length; i++) {
        const k = values[i]

        update.size += k.value.byteLength + 16
        update.values.push(k)
        k.valuePointer = new ValuePointer(context, 0, 0, 0, 0)

        if (i === values.length - 1 || update.size >= PREFERRED_BLOCK_SIZE) {
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
        type: 0,
        checkpoint: 0,
        batch: { start: batch.length - 1 - i, end: i },
        previous: null,
        tree: null,
        keys: null,
        values: null,
        cores: null
      }

      for (const k of update.values) {
        if (block.values === null) block.values = []
        const ptr = k.valuePointer
console.log('?')
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

        const ptr = k.valuePointer
        if (ptr) {
          ptr.core = await context.getCoreOffset(ptr.context, ptr.core, activeRequests)
          ptr.context = context
          console.log('writing ptr', ptr.core, context.core.id)
        }

        k.core = 0
        k.context = context
        k.seq = seq
        k.offset = block.keys.length
        block.keys.push(k)
      }

      for (const n of update.nodes) {
        if (block.tree === null) block.tree = []

        n.core = 0
        n.context = context
        n.seq = seq
        n.offset = block.tree.length
        block.tree.push(n.value)
      }

      blocks[seq - length] = block
    }

    const buffers = new Array(blocks.length)

    if (blocks.length > 0 && this.length > 0) {
      const core = this.key
        ? await context.getCoreOffsetByKey(this.key, activeRequests)
        : 0
      blocks[blocks.length - 1].previous = { core, seq: this.length - 1 }
    }

    // TODO: make this transaction safe
    if (context.changed) {
      context.changed = false
      context.checkpoint = context.core.length + blocks.length
      blocks[blocks.length - 1].cores = context.cores
    }

    for (let i = 0; i < blocks.length; i++) {
      blocks[i].checkpoint = context.checkpoint
      buffers[i] = encodeBlock(blocks[i], this.blockFormat)
    }

    if (this.closed) {
      throw new Error('Write batch is closed')
    }

    await context.core.append(buffers)

    for (let i = 0; i < batch.length; i++) {
      const update = batch[i]

      for (let j = 0; j < update.nodes.length; j++) {
        this.snapshot.bump(update.nodes[j])
      }
    }
  }
}

function toBlockFormat2(context, batch, ops) {
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
    if (j === batch.length) batch.push({ node: [], keys: [] })
    batch[j].keys = [k || new KeyPointer(context, 0, 0, 0, false, op.key, op.value, null)]
  }

  return batch
}
