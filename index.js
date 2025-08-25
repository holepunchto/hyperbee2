const b4a = require('b4a')
const c = require('compact-encoding')
const KeyValueStream = require('./lib/key-value-stream.js')
const NodeCache = require('./lib/cache.js')
const WriteBatch = require('./lib/write.js')
const { DataPointer, TreeNode, TreeNodePointer, EMPTY } = require('./lib/tree.js')
const { Block } = require('./lib/encoding.js')

class Hyperbee {
  constructor (core, { maxCacheSize = 4096, cache = new NodeCache(maxCacheSize), root = null } = {}) {
    this.core = core
    this.root = root
    this.cache = cache

    this.ready().catch(noop)
  }

  get opening () {
    return this.core.opening
  }

  get opened () {
    return this.core.opened
  }

  get closing () {
    return this.core.closing
  }

  get closed () {
    return this.core.closed
  }

  checkout (length) {
    const root = length === 0 ? EMPTY : new TreeNodePointer(length - 1, 0, false, null)
    return new Hyperbee(this.core.session(), { cache: this.cache, root })
  }

  snapshot () {
    if (this.root) return new Hyperbee(this.core.session(), { cache: this.cache, root: this.root })
    if (this.opened) return this.checkout(this.core.length)
    return new Hyperbee(this.core.session(), { cache: this.cache, root: null })
  }

  write (opts) {
    return new WriteBatch(this, opts)
  }

  async ready () {
    if (!this.core.opened) await this.core.ready()
    if (this.root) return

    this.root = this.core.length === 0
      ? EMPTY
      : new TreeNodePointer(this.core.length - 1, 0, false, null)
  }

  async close () {
    await this.core.close()
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
    if (ptr.changed) return ptr.value
    this.cache.bump(ptr)
    this.cache.gc()
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

  async _bootstrap () {
    await this.ready()
    return this.root === EMPTY ? null : this.root
  }

  update () {
    this.root = null
  }

  async get (key) {
    let ptr = await this._bootstrap()
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
}

module.exports = Hyperbee

function noop () {}
