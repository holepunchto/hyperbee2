const b4a = require('b4a')
const c = require('compact-encoding')
const { encodeBlock } = require('./encoding.js')
const {
  DataPointer,
  TreeNode,
  TreeNodePointer,
  MIN_KEYS,
  UNCHANGED,
  CHANGED,
  NEEDS_SPLIT
} = require('./tree.js')

module.exports = class WriteBatch {
  constructor(tree, { length = -1, key = null, autoUpdate = true, blockFormat = 3 } = {}) {
    this.tree = tree
    this.snapshot = tree.snapshot()
    this.autoUpdate = autoUpdate
    this.length = length
    this.key = key
    this.blockFormat = blockFormat
    this.closed = false
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
          if (b4a.equals(m.value, value)) return false
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
    let status = v.put(this.tree.context, target, value, null)

    if (status === UNCHANGED) return false

    ptr.changed = true

    for (let i = 0; i < stack.length; i++) stack[i].changed = true

    while (status === NEEDS_SPLIT) {
      const v = ptr.value ? this.snapshot.bump(ptr) : await this.snapshot.inflate(ptr)
      const parent = stack.pop()
      const { median, right } = v.split(this.tree.context)

      if (parent) {
        const p = parent.value ? this.snapshot.bump(parent) : await this.snapshot.inflate(parent)
        status = p.put(this.tree.context, median.key, median.value, right)
        ptr = parent
      } else {
        this.root = new TreeNodePointer(this.tree.context, 0, 0, 0, true, new TreeNode([], []))
        this.root.value.keys.push(median)
        this.root.value.children.push(ptr, right)
        this.snapshot.bump(this.root)
        status = UNCHANGED
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
    if (!this.root || !this.root.changed) return

    const update = { node: [], keys: [] }
    const batch = [update]
    const stack = [{ update, node: this.root }]
    const context = this.tree.context.getLocalContext()

    await context.update(this.tree.activeRequests)

    while (stack.length > 0) {
      const { update, node } = stack.pop()

      node.changed = false
      update.node.push(node)

      for (let i = 0; i < node.value.keys.length; i++) {
        const k = node.value.keys[i]

        if (!k.changed) {
          k.core = await context.getCoreOffset(k.context, k.core, this.tree.activeRequests)
          k.context = context
          continue
        }

        k.changed = false
        update.keys.push(k)
      }

      let first = true

      for (let i = 0; i < node.value.children.length; i++) {
        const n = node.value.children[i]

        if (!n.changed) {
          n.core = await context.getCoreOffset(n.context, n.core, this.tree.activeRequests)
          n.context = context
          continue
        }

        if (first || this.blockFormat === 2) {
          stack.push({ update, node: n })
          first = false
        } else {
          const update = { node: [], keys: [] }
          batch.push(update)
          stack.push({ update, node: n })
        }
      }
    }

    // if only the root was marked dirty and is === current bootstrap the batch is a noop - skip
    if (update.node.length === 1 && update.keys.length === 0) {
      const b = await this.tree.bootstrap()
      const n = update.node[0]
      if (b && b.context === n.context && b.core === n.core && b.seq === n.seq) return
      if (!b && n.value.isEmpty()) return
    }

    if (this.blockFormat === 2) toBlockFormat2(context, batch, this.ops)

    const length = context.core.length
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
        data: null,
        cores: null
      }

      for (const k of update.keys) {
        if (block.data === null) block.data = []

        k.core = 0
        k.context = context
        k.seq = seq
        k.offset = block.data.length
        block.data.push(k)
      }

      for (const n of update.node) {
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
        ? await context.getCoreOffsetByKey(this.key, this.tree.activeRequests)
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

      for (let j = 0; j < update.node.length; j++) {
        this.snapshot.bump(update.node)
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
    batch[j].keys = [k || new DataPointer(context, 0, 0, 0, false, op.key, op.value)]
  }

  return batch
}
