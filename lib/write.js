const b4a = require('b4a')
const c = require('compact-encoding')
const { TreeNode, TreeNodePointer, MIN_KEYS } = require('./tree.js')
const { Block } = require('./encoding.js')

module.exports = class WriteBatch {
  constructor (tree, { truncate = -1 } = {}) {
    this.tree = tree
    this.truncate = truncate
    this.flushing = false
    this.root = null
    this.ops = []
  }

  tryPut (key, value) {
    this.ops.push({ put: true, key, value })
  }

  tryDelete (key) {
    this.ops.push({ put: false, key, value: null })
  }

  tryClear () {
    this.ops = []
    this.truncate = 0
  }

  async flush () {
    if (this.flushing) throw new Error('Already flushed')
    this.flushing = true

    const ops = this.ops
    this.ops = []

    await this.tree.ready()

    const length = this.truncate === -1
      ? this.tree.core.length
      : this.truncate

    const changed = length === 0
    const seq = length === 0 ? 0 : length - 1

    this.root = new TreeNodePointer(seq, 0, changed, changed ? new TreeNode([], []) : null)

    for (const op of ops) {
      if (op.put) await this._put(op.key, op.value)
      else await this._delete(op.key)
    }

    await this._flush()
  }

  async _put (key, value) {
    const stack = []
    const target = key

    let ptr = this.root

    while (true) {
      const v = ptr.value ? this.tree.bump(ptr) : await this.tree.inflate(ptr)
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
          if (b4a.compare(m.value, value)) return
          v.setValue(mid, value)
          for (let i = 0; i < stack.length; i++) stack[i].changed = true
          return
        }

        if (c < 0) e = mid
        else s = mid + 1
      }

      const i = c < 0 ? e : s
      ptr = v.children[i]
    }

    const v = ptr.value ? this.tree.bump(ptr) : await this.tree.inflate(ptr)
    let needsSplit = !v.put(target, value, null)

    for (let i = 0; i < stack.length; i++) stack[i].changed = true

    while (needsSplit) {
      const v = ptr.value ? this.tree.bump(ptr) : await this.tree.inflate(ptr)
      const parent = stack.pop()
      const { median, right } = v.split()

      if (parent) {
        const p = parent.value ? this.tree.bump(parent) : await this.tree.inflate(parent)
        needsSplit = !p.put(median.key, median.value, right)
        ptr = parent
      } else {
        this.root = new TreeNodePointer(0, 0, true, new TreeNode([], []))
        this.root.value.keys.push(median)
        this.root.value.children.push(ptr, right)
        this.tree.bump(this.root)
        needsSplit = false
      }
    }
  }

  async _delete (key) {
    let ptr = this.root

    const stack = []

    while (true) {
      const v = ptr.value ? this.tree.bump(ptr) : await this.tree.inflate(ptr)
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
          return
        }

        if (c < 0) e = mid
        else s = mid + 1
      }

      if (!v.children.length) return

      const i = c < 0 ? e : s
      ptr = v.children[i]
    }
  }

  async _setKeyToNearestLeaf (v, index, stack) {
    let left = v.children[index]
    let right = v.children[index + 1]

    const [ls, rs] = await Promise.all([
      this._leafSize(left, false),
      this._leafSize(right, true)
    ])

    if (ls < rs) { // if fewer leaves on the left
      stack.push(right)
      let r = right.value ? this.tree.bump(right) : await this.tree.inflate(right)
      while (r.children.length) {
        right = r.children[0]
        stack.push(right)
        r = right.value ? this.tree.bump(right) : await this.tree.inflate(right)
      }
      v.keys[index] = r.keys.shift()
    } else { // if fewer leaves on the right
      stack.push(left)
      let l = left.value ? this.tree.bump(left) : await this.tree.inflate(left)
      while (l.children.length) {
        left = l.children[l.children.length - 1]
        stack.push(left)
        l = left.value ? this.tree.bump(left) : await this.tree.inflate(left)
      }
      v.keys[index] = l.keys.pop()
    }
  }

  async _leafSize (ptr, goLeft) {
    let v = ptr.value ? this.tree.bump(ptr) : await this.tree.inflate(ptr)
    while (v.children.length) {
      ptr = v.children[goLeft ? 0 : v.children.length - 1]
      v = ptr.value ? this.tree.bump(ptr) : await this.tree.inflate(ptr)
    }
    return v.keys.length
  }

  async _rebalance (stack) {
    const root = stack[0]

    while (stack.length > 1) {
      const ptr = stack.pop()
      const parent = stack[stack.length - 1]

      const v = ptr.value ? this.tree.bump(ptr) : await this.tree.inflate(ptr)

      if (v.keys.length >= MIN_KEYS) return root

      const p = parent.value ? this.tree.bump(parent) : await this.tree.inflate(parent)

      let { left, index, right } = v.siblings(p)

      let l = left && (left.value ? this.tree.bump(left) : await this.tree.inflate(left))

      // maybe borrow from left sibling?
      if (l && l.keys.length > MIN_KEYS) {
        left.changed = true
        v.keys.unshift(p.keys[index - 1])
        if (l.children.length) v.children.unshift(l.children.pop())
        p.keys[index - 1] = l.keys.pop()
        return root
      }

      let r = right && (right.value ? this.tree.bump(right) : await this.tree.inflate(right))

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

    const r = root.value ? this.tree.bump(root) : await this.tree.inflate(root)
    // check if the tree shrunk
    if (!r.keys.length && r.children.length) return r.children[0]
    return root
  }

  async _flush () {
    if (!this.root || !this.root.changed) return

    const update = { node: [], keys: [] }
    const batch = [update]
    const stack = [{ update, node: this.root }]

    while (stack.length > 0) {
      const { update, node } = stack.pop()

      node.changed = false
      update.node.push(node)

      for (const k of node.value.keys) {
        if (!k.changed) continue
        k.changed = false
        update.keys.push(k)
      }

      let first = true

      for (let i = 0; i < node.value.children.length; i++) {
        const next = node.value.children[i]
        if (!next.changed) continue

        if (first) {
          stack.push({ update, node: next })
          first = false
        } else {
          const update = { node: [], keys: [] }
          batch.push(update)
          stack.push({ update, node: next })
        }
      }
    }

    const length = this.tree.core.length
    const blocks = new Array(batch.length)

    for (let i = 0; i < batch.length; i++) {
      const update = batch[i]
      const seq = length + batch.length - i - 1

      const block = {
        type: 0,
        batch: i,
        tree: [],
        data: []
      }

      for (const k of update.keys) {
        k.seq = seq
        k.offset = block.data.length
        block.data.push(k)
      }

      for (const n of update.node) {
        n.seq = seq
        n.offset = block.tree.length
        block.tree.push(n.value)
      }

      blocks[seq - length] = block
    }

    const buffers = new Array(blocks.length)

    for (let i = 0; i < blocks.length; i++) {
      buffers[i] = c.encode(Block, blocks[i])
    }

    await this.tree.core.append(buffers)

    for (let i = 0; i < batch.length; i++) {
      const update = batch[i]

      for (let j = 0; j < update.node.length; j++) {
        this.tree.bump(update.node)
      }
    }

    // auto update
    this.tree.update(this.root)
  }
}
