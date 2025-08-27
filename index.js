const b4a = require('b4a')
const KeyValueStream = require('./lib/key-value-stream.js')
const NodeCache = require('./lib/cache.js')
const WriteBatch = require('./lib/write.js')
const CoreContext = require('./lib/context.js')
const { DataPointer, TreeNode, TreeNodePointer, EMPTY } = require('./lib/tree.js')

class Hyperbee {
  constructor (store, options = {}) {
    const {
      core = null,
      context = new CoreContext(store, core || store.get({ name: 'bee' })),
      maxCacheSize = 4096,
      cache = new NodeCache(maxCacheSize),
      root = null,
      activeRequests = [],
      view = false
    } = options

    this.store = store
    this.root = root
    this.cache = cache
    this.context = context
    this.activeRequests = activeRequests
    this.view = view

    this.ready().catch(noop)
  }

  get core () {
    return this.context.core
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

  checkout (length, key) {
    const context = key ? this.context.getContextByKey(key) : this.context
    const root = length === 0 ? EMPTY : new TreeNodePointer(context, 0, length - 1, 0, false, null)
    return new Hyperbee(this.store, { context: this.context, cache: this.cache, root, view: true })
  }

  snapshot () {
    return new Hyperbee(this.store, { context: this.context, cache: this.cache, root: this.root, view: true })
  }

  write (opts) {
    return new WriteBatch(this, opts)
  }

  async ready () {
    if (!this.core.opened) await this.core.ready()
    if (this.root) return

    this.root = this.context.core.length === 0
      ? EMPTY
      : new TreeNodePointer(this.context, 0, this.core.length - 1, 0, false, null)
  }

  async close () {
    if (this.activeRequests.length) this.core.clearRequests(this.activeRequests)
    if (!this.view) await this.store.close()
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

    const block = await ptr.context.getBlock(ptr.seq, ptr.core, this.activeRequests)
    const context = await ptr.context.getContext(ptr.core, this.activeRequests)
    const tree = block.tree[ptr.offset]

    const keys = new Array(tree.keys.length)
    const children = new Array(tree.children.length)

    for (let i = 0; i < keys.length; i++) {
      const k = tree.keys[i]
      const blk = k.seq === ptr.seq && k.core === ptr.core ? block : await context.getBlock(k.seq, k.core, this.activeRequests)
      const d = blk.data[k.offset]
      keys[i] = new DataPointer(context, k.core, k.seq, k.offset, false, d.key, d.value)
    }

    for (let i = 0; i < children.length; i++) {
      const c = tree.children[i]
      children[i] = new TreeNodePointer(context, c.core, c.seq, c.offset, false, null)
    }

    ptr.value = new TreeNode(keys, children)
    this.bump(ptr)

    return ptr.value
  }

  async _bootstrap () {
    await this.ready()
    return this.root === EMPTY ? null : this.root
  }

  update (root = null) {
    this.root = root
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
