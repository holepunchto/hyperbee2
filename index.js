const b4a = require('b4a')
const KeyValueStream = require('./lib/read-stream.js')
const NodeCache = require('./lib/cache.js')
const c = require('compact-encoding')
const { getEncoding } = require('./spec')

const T = 5
const MIN_KEYS = T - 1
const MAX_CHILDREN = MIN_KEYS * 2 + 1

const Block = getEncoding('@bee/block')

class DataPointer {
  constructor (seq, offset, changed, key, value) {
    this.seq = seq
    this.offset = offset
    this.changed = changed
    this.key = key
    this.value = value
  }
}

class TreeNodePointer {
  constructor (seq, offset, changed, value) {
    this.seq = seq
    this.offset = offset
    this.changed = changed
    this.value = value

    this.next = null
    this.prev = null
  }
}

class TreeNode {
  constructor (keys, children) {
    this.keys = keys
    this.children = children
  }

  put (key, value, child) {
    let s = 0
    let e = this.keys.length
    let c

    while (s < e) {
      const mid = (s + e) >> 1
      const k = this.keys[mid]

      c = b4a.compare(key, k.key)

      if (c === 0) {
        this.keys[mid] = new DataPointer(0, 0, true, key, value)
        return true
      }

      if (c < 0) e = mid
      else s = mid + 1
    }

    const i = c < 0 ? e : s
    this.keys.splice(i, 0, new DataPointer(0, 0, true, key, value))
    if (child) this.children.splice(i + 1, 0, child)

    return this.keys.length < MAX_CHILDREN
  }

  setValue (i, value) {
    this.keys[i] = new DataPointer(0, 0, true, this.keys[i].key, value)
  }

  removeKey (i) {
    this.keys.splice(i, 1)
    if (this.children.length) {
      this.children.splice(i + 1, 1)
    }
  }

  siblings (parent) {
    for (let i = 0; i < parent.children.length; i++) {
      if (parent.children[i].value !== this) continue // TODO: move to a seq/offset check instead

      const left = i ? parent.children[i - 1] : null
      const right = i < parent.children.length - 1 ? parent.children[i + 1] : null
      return { left, index: i, right }
    }

    // TODO: assert
    throw new Error('Bad parent')
  }

  merge (node, median) {
    this.keys.push(median)
    for (let i = 0; i < node.keys.length; i++) this.keys.push(node.keys[i])
    for (let i = 0; i < node.children.length; i++) this.children.push(node.children[i])
  }

  split () {
    const len = this.keys.length >> 1
    const right = new TreeNodePointer(0, 0, true, new TreeNode([], []))

    while (right.value.keys.length < len) right.value.keys.push(this.keys.pop())
    right.value.keys.reverse()

    const median = this.keys.pop()

    if (this.children.length) {
      while (right.value.children.length < len + 1) right.value.children.push(this.children.pop())
      right.value.children.reverse()
    }

    return {
      left: this,
      median,
      right
    }
  }
}

class WriteBatch {
  constructor (tree) {
    this.tree = tree
    this.ops = []
  }

  tryPut (key, value) {
    this.ops.push({ put: true, key, value })
  }

  tryDelete (key) {
    this.ops.push({ put: false, key, value: null })
  }

  async flush () {
    const ops = this.ops
    this.ops = []
    for (const op of ops) {
      if (op.put) await this.tree.put(op.key, op.value)
      else await this.tree.del(op.key)
    }
    await this.tree.flush()
  }
}

module.exports = class Hyperbee2 {
  constructor (core, { maxCacheSize = 4096, cache = new NodeCache() } = {}) {
    this.core = core
    this.root = null
    this.cache = cache
    this.maxCacheSize = maxCacheSize
  }

  checkout () {
    return this
  }

  snapshot () {
    return this
  }

  write () {
    return new WriteBatch(this)
  }

  async ready () {
    await this.core.ready()
  }

  async close () {
    await this.core.close()
  }

  async flush () {
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

    const length = this.core.length
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

    await this.core.append(buffers)
  }

  createReadStream (range) {
    return new KeyValueStream(this, range)
  }

  async peek (range = {}) {
    const rs = new KeyValueStream(this, { ...range, limit: 1 })
    let entry = null
    for await (const data of rs) entry = data
    return entry
  }

  async getBlock (seq) {
    const buffer = await this.core.get(seq)
    const block = c.decode(Block, buffer)
    return block
  }

  bump (ptr) {
    this.cache.bump(ptr)
    while (this.cache.size > this.maxCacheSize) {
      const old = this.cache.oldest()
      if (old.changed) break
      this.cache.remove(old)
      old.value = null
    }
    return ptr.value
  }

  async inflate (ptr) {
    if (ptr.value) {
      this.bump(ptr)
      return ptr.value
    }

    const block = await this.getBlock(ptr.seq)
    const tree = block.tree[ptr.offset]

    const keys = new Array(tree.keys.length)
    const children = new Array(tree.children.length)

    for (let i = 0; i < keys.length; i++) {
      const k = tree.keys[i]
      const blk = k.seq === ptr.seq ? block : await this.getBlock(k.seq)
      const d = blk.data[k.offset]
      keys[i] = new DataPointer(k.seq, k.offset, false, d.key, d.value)
    }

    for (let i = 0; i < children.length; i++) {
      const c = tree.children[i]
      children[i] = new TreeNodePointer(c.seq, c.offset, false, null)
    }

    ptr.value = new TreeNode(keys, children)
    this.bump(ptr)

    return ptr.value
  }

  async bootstrap () {
    await this.ready()
    if (this.core.length === 0) return
    const node = new TreeNodePointer(this.core.length - 1, 0, false, null)

    if (node.value) this.bump(node)
    else await this.inflate(node)

    this.root = node
    return node
  }

  async get (key) {
    if (!this.root) await this.bootstrap()

    let ptr = this.root
    if (!ptr) return null

    while (true) {
      const v = ptr.value ? this.bump(ptr) : await this.inflate(ptr)

      let s = 0
      let e = v.keys.length
      let c = 0

      while (s < e) {
        const mid = (s + e) >> 1
        const m = v.keys[mid]

        c = b4a.compare(key, m.key)

        if (c === 0) return m

        if (c < 0) e = mid
        else s = mid + 1
      }

      if (!v.children.length) return null

      const i = c < 0 ? e : s
      ptr = v.children[i]
    }
  }

  async put (key, value) {
    if (!this.root) await this.bootstrap()
    if (!this.root) this.root = new TreeNodePointer(0, 0, true, new TreeNode([], []))

    const stack = []
    const target = key

    let ptr = this.root

    while (true) {
      const v = ptr.value ? this.bump(ptr) : await this.inflate(ptr)
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

    const v = ptr.value ? this.bump(ptr) : await this.inflate(ptr)
    let needsSplit = !v.put(target, value, null)

    for (let i = 0; i < stack.length; i++) stack[i].changed = true

    while (needsSplit) {
      const v = ptr.value ? this.bump(ptr) : await this.inflate(ptr)
      const parent = stack.pop()
      const { median, right } = v.split()

      if (parent) {
        const p = parent.value ? this.bump(parent) : await this.inflate(parent)
        needsSplit = !p.put(median.key, median.value, right)
        ptr = parent
      } else {
        this.root = new TreeNodePointer(0, 0, true, new TreeNode([], []))
        this.root.value.keys.push(median)
        this.root.value.children.push(ptr, right)
        this.bump(this.root)
        needsSplit = false
      }
    }
  }

  async del (key) {
    if (!this.root) await this.bootstrap()
    if (!this.root) return

    let ptr = this.root

    const stack = []

    while (true) {
      const v = ptr.value ? this.bump(ptr) : await this.inflate(ptr)
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
      let r = right.value ? this.bump(right) : await this.inflate(right)
      while (r.children.length) {
        right = r.children[0]
        stack.push(right)
        r = right.value ? this.bump(right) : await this.inflate(right)
      }
      v.keys[index] = r.keys.shift()
    } else { // if fewer leaves on the right
      stack.push(left)
      let l = left.value ? this.bump(left) : await this.inflate(left)
      while (l.children.length) {
        left = l.children[l.children.length - 1]
        stack.push(left)
        l = left.value ? this.bump(left) : await this.inflate(left)
      }
      v.keys[index] = l.keys.pop()
    }
  }

  async _leafSize (ptr, goLeft) {
    let v = ptr.value ? this.bump(ptr) : await this.inflate(ptr)
    while (v.children.length) {
      ptr = v.children[goLeft ? 0 : v.children.length - 1]
      v = ptr.value ? this.bump(ptr) : await this.inflate(ptr)
    }
    return v.keys.length
  }

  async _rebalance (stack) {
    const root = stack[0]

    while (stack.length > 1) {
      const ptr = stack.pop()
      const parent = stack[stack.length - 1]

      const v = ptr.value ? this.bump(ptr) : await this.inflate(ptr)

      if (v.keys.length >= MIN_KEYS) return root

      const p = parent.value ? this.bump(parent) : await this.inflate(parent)

      let { left, index, right } = v.siblings(p)

      let l = left && (left.value ? this.bump(left) : await this.inflate(left))

      // maybe borrow from left sibling?
      if (l && l.keys.length > MIN_KEYS) {
        left.changed = true
        v.keys.unshift(p.keys[index - 1])
        if (l.children.length) v.children.unshift(l.children.pop())
        p.keys[index - 1] = l.keys.pop()
        return root
      }

      let r = right && (right.value ? this.bump(right) : await this.inflate(right))

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

    const r = root.value ? this.bump(root) : await this.inflate(root)
    // check if the tree shrunk
    if (!r.keys.length && r.children.length) return r.children[0]
    return root
  }
}
