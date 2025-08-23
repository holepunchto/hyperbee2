const b4a = require('b4a')
const { Readable } = require('streamx')
const { getEncoding } = require('./spec')
const c = require('compact-encoding')

const T = 5
const MIN_KEYS = T - 1
const MAX_CHILDREN = MIN_KEYS * 2 + 1

const Block = getEncoding('@bee/block')

class KeyValuePointer {
  constructor (seq, offset, key, value, changed) {
    this.seq = seq
    this.offset = offset
    this.key = key
    this.value = value
    this.changed = changed
  }
}

class TreeNodePointer {
  constructor (seq, offset, node) {
    this.seq = seq
    this.offset = offset
    this.node = node
    this.changed = false
  }
}

class TreeNode {
  constructor () {
    this.keys = []
    this.children = []
    this.changed = false
    this.seq = 0
    this.offset = 0
  }

  static inflate (node) {
    console.log(node)
  }

  async inflate () {

  }

  async load (i) {

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
        this.keys[mid] = new KeyValuePointer(0, 0, key, value, true)
        return true
      }

      if (c < 0) e = mid
      else s = mid + 1
    }

    const i = c < 0 ? e : s
    this.keys.splice(i, 0, new KeyValuePointer(0, 0, key, value, true))
    if (child) this.children.splice(i + 1, 0, child)

    return this.keys.length < MAX_CHILDREN
  }

  flush () {
    this.changed = false
  }

  setValue (i, value) {
    this.changed = true
    this.keys[i] = new KeyValuePointer(0, 0, this.keys[i].key, value, true)
  }

  split () {
    const len = this.keys.length >> 1
    const right = new TreeNodePointer(0, 0, new TreeNode())

    right.changed = true

    while (right.node.keys.length < len) right.node.keys.push(this.keys.pop())
    right.node.keys.reverse()

    const median = this.keys.pop()

    if (this.children.length) {
      while (right.node.children.length < len + 1) right.node.children.push(this.children.pop())
      right.node.children.reverse()
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
  constructor (core) {
    this.core = core
    this.root = null
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

      for (const k of node.node.keys) {
        if (!k.changed) continue
        k.changed = false
        update.keys.push(k)
      }

      let first = true

      for (let i = 0; i < node.node.children.length; i++) {
        const next = node.node.children[i]
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
        block.tree.push(n.node)
      }

      blocks[seq] = block
    }

    const buffers = new Array(blocks.length)

    for (let i = 0; i < blocks.length; i++) {
      buffers[i] = c.encode(Block, blocks[i])
    }

    await this.core.append(buffers)
  }

  createReadStream ({ limit = Infinity, gte = null, gt = null, lte = null, lt = null } = {}) {
    const rs = new Readable()

    const stack = this.root ? [{ node: this.root, offset: 0 }] : []

    const start = gte || gt
    const end = lte || lt

    const startCompare = gte ? 0 : 1
    const endCompare = lte ? 0 : -1

    if (start && stack.length) {
      while (true) {
        const top = stack[stack.length - 1]

        for (let i = 0; i < top.node.keys.length; i++) {
          let c = b4a.compare(start, top.node.keys[i].key)
          if (c < 0) break
          top.offset = 2 * i + 1 + (c === 0 ? startCompare : 1)
        }

        const child = (top.offset & 1) === 0
        const k = top.offset >> 1

        if (!child || k >= top.node.children.length) break

        stack.push({
          offset: 0,
          node: top.node.children[k]
        })
      }
    }

    while (stack.length && limit > 0) {
      const top = stack.pop()

      const offset = top.offset++
      const child = (offset & 1) === 0
      const k = offset >> 1

      if (child) {
        stack.push(top)
        if (k < top.node.children.length) {
          stack.push({ node: top.node.children[k], offset: 0 })
        }
      } else if (k < top.node.keys.length) {
        const result = top.node.keys[k]
        const c = end ? b4a.compare(result.key, end) : -1
        if (c > endCompare) break
        stack.push(top)
        rs.push(result)
        limit--
      }
    }

    rs.push(null)

    return rs
  }

  async tmp () {
    if (this.core.length === 0) return null
    const seq = this.core.length - 1
    await this.inflate(seq, 0)
  }

  async getBlock (seq) {
    const buffer = await this.core.get(seq)
    const block = c.decode(Block, buffer)
    return block
  }

  async inflate (seq, offset) {
    const block = await this.getBlock(seq)
    const tree = block.tree[offset]

    const keys = new Array(tree.keys.length)
    const children = new Array(tree.children.length)

    for (let i = 0; i < keys.length; i++) {
      const k = tree.keys[i]
      const blk = k.seq === seq ? block : await this.getBlock(k.seq)
      const d = blk.data[k.offset]
      keys[i] = new KeyValuePointer(k.seq, k.offset, d.key, d.value, false)
    }

    for (let i = 0; i < children.length; i++) {
      const c = tree.children[i]
      children[i] = new TreeNodePointer(c.seq, c.offset, null)
    }

    const node = new TreeNode()
    node.keys = keys
    node.children = children

    return node
  }

  async get (key) {
    let node = this.root
    if (!node) return null

    while (true) {
      let s = 0
      let e = node.node.keys.length
      let c = 0

      while (s < e) {
        const mid = (s + e) >> 1
        const m = node.node.keys[mid]

        c = b4a.compare(key, m.key)

        if (c === 0) return m.value

        if (c < 0) e = mid
        else s = mid + 1
      }

      if (!node.node.children.length) return null

      const i = c < 0 ? e : s
      node = node.node.children[i]
    }
  }

  async put (key, value) {
    const stack = []

    if (!this.root) this.root = new TreeNodePointer(0, 0, new TreeNode())

    let node = this.root

    const target = key

    while (node.node.children.length) {
      stack.push(node)

      let s = 0
      let e = node.node.keys.length
      let c = 0

      while (s < e) {
        const mid = (s + e) >> 1
        const m = node.node.keys[mid]

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

      node = node.node.children[i]
    }

    let needsSplit = !node.node.put(target, value, null)

    for (let i = 0; i < stack.length; i++) stack[i].changed = true

    while (needsSplit) {
      const parent = stack.pop()
      const { median, right } = node.node.split()

      if (parent) {
        needsSplit = !parent.node.put(median.key, median.value, right)
        node = parent
      } else {
        this.root = new TreeNodePointer(0, 0, new TreeNode())
        this.root.changed = true
        this.root.node.keys.push(median)
        this.root.node.children.push(node, right)
        needsSplit = false
      }
    }
  }
}
