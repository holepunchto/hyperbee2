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
    this.changed = true
    this.keys[i] = new DataPointer(0, 0, true, this.keys[i].key, value)
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

  async flush () {
    const ops = this.ops
    this.ops = []
    for (const op of ops) {
      if (op.put) await this.tree.put(op.key, op.value)
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
      this.cache.remove(old)
      old.value = null
    }
    return ptr.value
  }

  async inflate (ptr) {
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

    let node = this.root
    if (!node) return null

    if (node.value) this.bump(node)
    else await this.inflate(node)

    while (true) {
      let s = 0
      let e = node.value.keys.length
      let c = 0

      while (s < e) {
        const mid = (s + e) >> 1
        const m = node.value.keys[mid]

        c = b4a.compare(key, m.key)

        if (c === 0) return m

        if (c < 0) e = mid
        else s = mid + 1
      }

      if (!node.value.children.length) return null

      const i = c < 0 ? e : s
      node = node.value.children[i]

      if (node.value) this.bump(node)
      else await this.inflate(node)
    }
  }

  async put (key, value) {
    if (!this.root) await this.bootstrap()
    if (!this.root) this.root = new TreeNodePointer(0, 0, true, new TreeNode([], []))

    const stack = []

    let node = this.root

    if (node.value) this.bump(node)
    else await this.inflate(node)

    const target = key

    while (node.value.children.length) {
      stack.push(node)

      let s = 0
      let e = node.value.keys.length
      let c = 0

      while (s < e) {
        const mid = (s + e) >> 1
        const m = node.value.keys[mid]

        c = b4a.compare(target, m.key)

        if (c === 0) {
          if (b4a.compare(m.value, value)) return
          node.setValue(mid, value)
          for (let i = 0; i < stack.length; i++) stack[i].changed = true
          return
        }

        if (c < 0) e = mid
        else s = mid + 1
      }

      const i = c < 0 ? e : s

      node = node.value.children[i]

      if (node.value) this.bump(node)
      else await this.inflate(node)
    }

    let needsSplit = !node.value.put(target, value, null)

    for (let i = 0; i < stack.length; i++) stack[i].changed = true

    while (needsSplit) {
      const parent = stack.pop()
      const { median, right } = node.value.split()

      if (parent) {
        needsSplit = !parent.value.put(median.key, median.value, right)
        node = parent
      } else {
        this.root = new TreeNodePointer(0, 0, true, new TreeNode([], []))
        this.root.value.keys.push(median)
        this.root.value.children.push(node, right)
        needsSplit = false
      }
    }
  }
}
