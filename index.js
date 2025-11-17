const b4a = require('b4a')
const Hypercore = require('hypercore')
const KeyValueStream = require('./lib/key-value-stream.js')
const ChangesStream = require('./lib/changes-stream.js')
const NodeCache = require('./lib/cache.js')
const WriteBatch = require('./lib/write.js')
const CoreContext = require('./lib/context.js')
const { DataPointer, TreeNode, TreeNodePointer, EMPTY } = require('./lib/tree.js')

class Hyperbee {
  constructor(store, options = {}) {
    const {
      key = null,
      encryption = null,
      core = key ? store.get(key) : store.get({ key, name: 'bee', encryption }),
      context = new CoreContext(store, core, core, encryption),
      maxCacheSize = 4096,
      cache = new NodeCache(maxCacheSize),
      root = null,
      activeRequests = [],
      view = false,
      writable = true,
      unbatch = 0
    } = options

    this.store = store
    this.root = root
    this.cache = cache
    this.context = context
    this.activeRequests = activeRequests
    this.view = view
    this.writable = writable
    this.unbatch = unbatch

    this.ready().catch(noop)
  }

  head() {
    if (!this.root) return null
    if (this.root === EMPTY) return { length: 0, key: this.context.core.key }
    return { length: this.root.seq + 1, key: this.root.context.core.key }
  }

  get core() {
    return this.context.core
  }

  get opening() {
    return this.store.opening
  }

  get opened() {
    return this.store.opened
  }

  get closing() {
    return this.store.closing
  }

  get closed() {
    return this.store.closed
  }

  replicate(...opts) {
    return this.store.replicate(...opts)
  }

  _makeView(context, root, writable, unbatch) {
    return new Hyperbee(this.store, {
      context,
      cache: this.cache,
      root,
      view: true,
      writable,
      unbatch
    })
  }

  checkout({ length = this.core.length, key = null, writable = false } = {}) {
    const context = key ? this.context.getContextByKey(key) : this.context
    const root = length === 0 ? EMPTY : new TreeNodePointer(context, 0, length - 1, 0, false, null)
    return this._makeView(context, root, writable, 0)
  }

  snapshot() {
    return this._makeView(this.context, this.root, false, 0)
  }

  undo(n) {
    return this._makeView(this.context, this.root, true, n)
  }

  write(opts) {
    if (!this.writable) throw new Error('Not writable')
    return new WriteBatch(this, opts)
  }

  async ready() {
    if (!this.core.opened) await this.core.ready()
    if (this.root) return

    this.root =
      this.context.core.length === 0
        ? EMPTY
        : new TreeNodePointer(this.context, 0, this.core.length - 1, 0, false, null)
  }

  async close() {
    if (this.activeRequests.length) Hypercore.clearRequests(this.activeRequests)
    if (!this.view) await this.store.close()
  }

  createReadStream(range) {
    return new KeyValueStream(this, range)
  }

  createChangesStream(options) {
    return new ChangesStream(this, options)
  }

  async peek(range = {}) {
    const rs = new KeyValueStream(this, { ...range, limit: 1 })
    let entry = null
    for await (const data of rs) entry = data
    return entry
  }

  bump(ptr) {
    if (ptr.changed) return ptr.value
    this.cache.bump(ptr)
    this.cache.gc()
    return ptr.value
  }

  async inflate(ptr, activeRequests = this.activeRequests) {
    if (ptr.value) {
      this.bump(ptr)
      return ptr.value
    }

    const block = await ptr.context.getBlock(ptr.seq, ptr.core, activeRequests)
    const context = await ptr.context.getContext(ptr.core, activeRequests)
    const tree = block.tree[ptr.offset]

    const keys = new Array(tree.keys.length)
    const children = new Array(tree.children.length)

    for (let i = 0; i < keys.length; i++) {
      const k = tree.keys[i]
      const blk =
        k.seq === ptr.seq && k.core === 0 && ptr.core === 0
          ? block
          : await context.getBlock(k.seq, k.core, activeRequests)
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

  async bootstrap(activeRequests = this.activeRequests) {
    if (!this.root) await this.ready()
    if (this.unbatch) await this._rollback(activeRequests)
    return this.root === EMPTY ? null : this.root
  }

  async _rollback(activeRequests) {
    const expected = this.unbatch

    let n = expected
    let length = this.root === EMPTY ? 0 : this.root.seq + 1
    let context = this.context

    while (n > 0 && length > 0 && expected === this.unbatch) {
      const seq = length - 1
      const blk = await context.getBlock(seq, 0, activeRequests)

      if (!blk.previous) {
        length = 0
        break
      }

      context = await context.getContext(blk.previous.core, activeRequests)
      length = blk.previous.seq + 1
      n--
    }

    if (expected === this.unbatch) {
      this.context = context
      this.root = length === 0 ? EMPTY : new TreeNodePointer(context, 0, length - 1, 0, false, null)
      this.unbatch = 0
    }
  }

  update(root = null) {
    this.root = root
    this.unbatch = 0
  }

  async get(key, { activeRequests = this.activeRequests } = {}) {
    let ptr = await this.bootstrap(activeRequests)
    if (!ptr) return null

    while (true) {
      const v = ptr.value ? this.bump(ptr) : await this.inflate(ptr, activeRequests)

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

function noop() {}
