const b4a = require('b4a')
const Hypercore = require('hypercore')
const EventEmitter = require('events')
const { RangeStream } = require('./lib/ranges.js')
const { DiffStream } = require('./lib/diff.js')
const { ChangesStream } = require('./lib/changes.js')
const NodeCache = require('./lib/cache.js')
const WriteBatch = require('./lib/write.js')
const CoreContext = require('./lib/context.js')
const SessionConfig = require('./lib/session-config.js')
const { inflate, inflateValue } = require('./lib/inflate.js')
const { EMPTY } = require('./lib/tree.js')

class Hyperbee extends EventEmitter {
  constructor(store, options = {}) {
    super()

    const {
      t = 128, // legacy number for now, should be 128 now
      key = null,
      encryption = null,
      maxCacheSize = 4096,
      config = new SessionConfig([], 0, true),
      activeRequests = config.activeRequests,
      timeout = config.timeout,
      wait = config.wait,
      core = key ? store.get(key) : store.get({ key, name: 'bee', encryption }),
      context = new CoreContext(store, core, new NodeCache(maxCacheSize), core, encryption, t),
      root = null,
      view = false,
      writable = true,
      unbatch = 0,
      autoUpdate = false,
      preload = null
    } = options

    this.store = store
    this.root = root
    this.context = context
    this.config = config.sub(activeRequests, timeout, wait)
    this.view = view
    this.writable = writable
    this.unbatch = unbatch

    this.autoUpdate = autoUpdate
    this.preload = preload

    this.ready().catch(noop)
  }

  static isHyperbee(bee) {
    return bee instanceof Hyperbee
  }

  head() {
    if (!this.root) return null
    if (this.root === EMPTY) return { length: 0, key: this.context.core.key }
    return { length: this.root.seq + 1, key: this.root.context.core.key }
  }

  get cache() {
    return this.context.cache
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
      config: this.config,
      core: context.core,
      context,
      root,
      view: true,
      writable,
      unbatch,
      autoUpdate: false,
      preload: this.preload
    })
  }

  checkout({ length = this.core.length, key = null, writable = false } = {}) {
    const context = key ? this.context.getContextByKey(key) : this.context
    const root = length === 0 ? EMPTY : context.createTreeNode(0, length - 1, 0, false, null)
    return this._makeView(context, root, writable, 0)
  }

  move({ length = this.core.length, key = null, writable = this.writable } = {}) {
    const context = key ? this.context.getContextByKey(key) : this.context
    const root = length === 0 ? EMPTY : context.createTreeNode(0, length - 1, 0, false, null)
    this.context = context
    this.writable = writable
    this.root = root
    this.emit('update')
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
    if (this.preload) await this.preload()
    if (this.root) return

    this.root =
      this.context.core.length === 0
        ? EMPTY
        : this.context.createTreeNode(0, this.core.length - 1, 0, false, null)

    if (this.autoUpdate) {
      this.core.on('append', () => {
        this.update()
      })
    }

    this.emit('ready')
  }

  async close() {
    if (!this.root) await this.ready()
    if (this.config.activeRequests.length) Hypercore.clearRequests(this.config.activeRequests)
    if (this.view) return
    if (this.store.closing) return this.store.close()
    await this.store.close()
    this.emit('close')
  }

  createReadStream(options) {
    return new RangeStream(this, options)
  }

  createDiffStream(right, options) {
    return new DiffStream(this, right, options)
  }

  createChangesStream(options) {
    return new ChangesStream(this, options)
  }

  async peek(range = {}) {
    const rs = new RangeStream(this, { ...range, limit: 1 })
    let entry = null
    for await (const data of rs) entry = data
    return entry
  }

  download(range = {}) {
    const rs = new RangeStream(this, range)
    rs.resume()
    return new Promise((resolve, reject) => {
      rs.once('error', reject)
      rs.once('close', resolve)
    })
  }

  bump(ptr) {
    if (ptr.changed) return ptr.value
    this.context.cache.bump(ptr)
    this.context.cache.gc()
    return ptr.value
  }

  async inflate(ptr, config) {
    if (!ptr.value) {
      await inflate(ptr, config)
    }
    this.bump(ptr)
    return ptr.value
  }

  async finalizeKeyPointer(key, config) {
    const value = key.value || (await inflateValue(key, config))

    return {
      core: key.context.getCore(key.core),
      offset: key.offset,
      seq: key.seq,
      key: key.key,
      value
    }
  }

  async bootstrap(config) {
    if (!this.root) await this.ready()
    if (this.unbatch) await this._rollback(config)
    return this.root === EMPTY ? null : this.root
  }

  async _rollback(config) {
    const expected = this.unbatch

    let n = expected
    let length = this.root === EMPTY ? 0 : this.root.seq + 1
    let context = this.context

    while (n > 0 && length > 0 && expected === this.unbatch) {
      const seq = length - 1
      const blk = await context.getBlock(seq, 0, config)

      if (!blk.previous) {
        length = 0
        break
      }

      context = await context.getContext(blk.previous.core, config)
      length = blk.previous.seq + 1
      n--
    }

    if (expected === this.unbatch) {
      this.context = context
      this.root = length === 0 ? EMPTY : context.createTreeNode(0, length - 1, 0, false, null)
      this.unbatch = 0
      this.emit('update')
    }
  }

  update(root = null) {
    this.root = root
    this.unbatch = 0
    this.emit('update')
  }

  async get(key, opts) {
    const config = this.config.options(opts)

    let ptr = await this.bootstrap(config)
    if (!ptr) return null

    while (true) {
      const v = ptr.value ? this.bump(ptr) : await this.inflate(ptr, config)

      let s = 0
      let e = v.keys.length
      let c = 0

      while (s < e) {
        const mid = (s + e) >> 1
        const m = v.keys.get(mid)

        c = b4a.compare(key, m.key)

        if (c === 0) return this.finalizeKeyPointer(m, config)

        if (c < 0) e = mid
        else s = mid + 1
      }

      if (!v.children.length) return null

      const i = c < 0 ? e : s
      ptr = v.children.get(i)
    }
  }
}

module.exports = Hyperbee

function noop() {}
