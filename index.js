const b4a = require('b4a')
const Hypercore = require('hypercore')
const EventEmitter = require('events')
const { RangeStream } = require('./lib/ranges.js')
const { DiffStream } = require('./lib/diff.js')
const { ChangesStream } = require('./lib/changes.js')
const NodeCache = require('./lib/cache.js')
const WriteBatch = require('./lib/write.js')
const copyChanges = require('./lib/copy.js')
const CoreContext = require('./lib/context.js')
const SessionConfig = require('./lib/session-config.js')
const { inflate, inflateValue } = require('./lib/inflate.js')
const { EMPTY, T } = require('./lib/tree.js')
const { Lifetime } = require('./lib/lifetimes.js')

class Hyperbee extends EventEmitter {
  constructor(store, opts = {}) {
    super()

    const {
      t = T,
      key = null,
      encryption = null,
      getEncryptionProvider = toEncryptionProvider(encryption),
      maxCacheSize = 4096,
      config = new SessionConfig([], 0, true, null),
      activeRequests = config.activeRequests,
      timeout = config.timeout,
      wait = config.wait,
      trace = config.trace,
      core = getRootCore(store, key, getEncryptionProvider),
      context = new CoreContext(
        store,
        core,
        new NodeCache(maxCacheSize),
        core,
        getEncryptionProvider
      ),
      root = null,
      view = false,
      writable = true,
      unbatch = 0,
      autoUpdate = !writable && !view,
      preload = null,
      lifetime = new Lifetime()
    } = opts

    this.store = store
    this.root = root
    this.t = t
    this.context = context
    this.config = config.sub(activeRequests, timeout, wait, trace)
    this.lifetime = lifetime
    this.view = view
    this.writable = writable
    this.unbatch = unbatch

    this.autoUpdate = autoUpdate
    this.preload = preload

    // The root session for top-level Hyperbee methods, all other sessions
    // are descendants of this and will be ended when this one ends
    // (i.e. when Hyperbee.close() called)
    this.session = new HyperbeeSession(this, lifetime)

    this.ready().catch(noop)
  }

  static isHyperbee(bee) {
    return bee instanceof Hyperbee
  }

  isGenesis() {
    const head = this.head()
    if (!head) return false
    return head.length === 0
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

  _makeView(context, root, writable, unbatch, timeout, wait) {
    return new Hyperbee(this.store, {
      t: this.t,
      config: this.config,
      lifetime: this.lifetime,
      core: context.core,
      timeout,
      wait,
      context,
      root,
      view: true,
      writable,
      unbatch,
      autoUpdate: false,
      preload: this.preload
    })
  }

  checkout({
    length = this.core.length,
    key = null,
    writable = false,
    timeout = this.config.timeout,
    wait = this.config.wait
  } = {}) {
    const context = key ? this.context.getContextByKey(key) : this.context
    const root = length === 0 ? EMPTY : context.createTreeNode(0, length - 1, 0, false, null)
    return this._makeView(context, root, writable, 0, timeout, wait)
  }

  move({ length = this.core.length, key = null, writable = this.writable } = {}) {
    this.context = key ? this.context.getContextByKey(key) : this.context
    this.writable = writable

    this._setRoot(this._nodeAtSeq(length - 1), true)
  }

  snapshot({ timeout = this.config.timeout, wait = this.config.wait } = {}) {
    return this._makeView(this.context, this.root, false, 0, timeout, wait)
  }

  undo(n, { timeout = this.config.timeout, wait = this.config.wait } = {}) {
    return this._makeView(this.context, this.root, true, n, timeout, wait)
  }

  update(root = null) {
    if (root === null) {
      if (this.context.lock.locked) return
      root = this._lastNodeInCore()
    }
    this._setRoot(root, true)
  }

  _lastNodeInCore() {
    return this._nodeAtSeq(this.context.core.length - 1)
  }

  _nodeAtSeq(seq) {
    return seq < 0 ? EMPTY : this.context.createTreeNode(0, seq, 0, false, null)
  }

  async ready() {
    if (!this.core.opened) await this.core.ready()

    if (this.root) return
    if (this.preload) await this.preload()
    if (this.root) return

    this._setRoot(this._lastNodeInCore(), false)
    if (this.autoUpdate) {
      this.core.on('append', this.update.bind(this, null))
    }

    this.emit('ready')
  }

  async close() {
    if (!this.root) await this.ready()
    // Stop in-flight requests
    this.session.close()
    if (this.view) return
    if (this.store.closing) return this.store.close()
    await this.store.close()
    this.emit('close')
  }

  bump(ptr) {
    if (ptr.changed) return ptr.value
    this.context.cache.bump(ptr)
    this.context.cache.gc()
    return ptr.value
  }

  _setRoot(root, emit) {
    if (root === null || !root.equivalentTo(this.root)) {
      this.root = root
      if (emit) this.emit('update')
    }
    this.unbatch = 0
  }

  session() {
    return this.session.fork()
  }

  write(...opts) {
    return this.session.write(...opts)
  }

  createReadStream(...opts) {
    return this.session.createReadStream(...opts)
  }

  createDiffStream(...opts) {
    return this.session.createDiffStream(...opts)
  }

  createChangesStream(...opts) {
    return this.session.createChangesStream(...opts)
  }

  peek(...opts) {
    return this.session.peek(...opts)
  }

  download(...opts) {
    return this.session.download(...opts)
  }

  get(...opts) {
    return this.session.get(...opts)
  }

  // TODO: used by write batches but presumably it should not be part of public API
  // outside this package?
  inflate(...opts) {
    return this.session.inflate(...opts)
  }

  reindex(...opts) {
    return this.session.reindex(...opts)
  }

  cores(...opts) {
    return this.session.cores(...opts)
  }

  compat(...opts) {
    return this.session.compat(...opts)
  }

  bootstrap(...opts) {
    return this.session.bootstrap(...opts)
  }

  asSession() {
    return this.session
  }
}

class HyperbeeSession {
  constructor(bee, lifetime) {
    this.bee = bee
    this.config = bee.config.detach({})

    // Fork from main hyperbee instance lifetime so if the underlying
    // hyperbee is closed, this session's requests also get cancelled.
    this.lifetime = lifetime.fork()
    this.lifetime.wait(() => {
      // When this session ends, cancel its hypercore requests.
      Hypercore.destroyRequests(this.config.activeRequests, null)
    })
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

  async cores({ local = true, ...opts } = {}) {
    const config = this.config.options(opts)
    await this.bee.ready()
    await this.bee.context.update(config)

    const cores = []

    // index 0 is the core this bee is viewing, the rest are referenced cores
    for (let i = 0; i <= this.bee.context.cores.length; i++) {
      const key = this.bee.context.getCoreKey(i)
      if (!local && b4a.equals(key, this.bee.context.local.key)) continue
      cores.push(key)
    }

    return cores
  }

  async compat() {
    await this.bee.ready()
    return this.bee.context.getBlockType(0, this.config)
  }

  async bootstrap(config) {
    if (!this.bee.root) await this.bee.ready()
    if (this.bee.unbatch) await this._rollback(config)
    return this.bee.root === EMPTY ? null : this.bee.root
  }

  async _rollback(config) {
    const expected = this.bee.unbatch

    let n = expected
    let length = this.bee.root === EMPTY ? 0 : this.bee.root.seq + 1
    let context = this.bee.context

    while (n > 0 && length > 0 && expected === this.bee.unbatch) {
      const seq = length - 1
      const blk = await context.getBlock(seq, 0, config, null)

      if (!blk.previous) {
        length = 0
        break
      }

      context = await context.getContext(blk.previous.core, config, blk)
      length = blk.previous.seq + 1
      n--
    }

    if (expected === this.bee.unbatch) {
      this.bee.context = context
      this.bee._setRoot(this.bee._nodeAtSeq(length - 1), true)
    }
  }

  write(opts = {}) {
    if (!this.bee.writable) throw new Error('Not writable')
    if (!opts.key) {
      opts.key = this.bee.root ? this.bee.head().key : null
    }
    return new WriteBatch(this, opts)
  }

  async reindex(
    until,
    { timeout = this.config.timeout, wait = this.config.wait, prefetch = 128, map = null } = {}
  ) {
    if (!this.bee.writable) throw new Error('Not writable')

    const config = this.config.options({ timeout, wait })

    const changes = []

    for await (const data of new ChangesStream(this, { timeout, wait, prefetch })) {
      if (await until(data)) break
      changes.push(data)
    }

    if (changes.length === 0) return 0

    const local = this.bee.context.getLocalContext()

    await local.lock.lock()

    try {
      await copyChanges(this.bee, changes, config, map)
    } finally {
      local.lock.unlock()
    }

    this.bee.context = local
    this.bee._setRoot(this.bee._lastNodeInCore(), true)

    return changes.length
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

  createReadStream(options) {
    return new RangeStream(this, options)
  }

  createDiffStream(right, options) {
    return new DiffStream(this, right.asSession(), options)
  }

  createChangesStream(options) {
    return new ChangesStream(this, options)
  }

  async inflate(ptr, config) {
    if (!ptr.value) {
      await inflate(ptr, config, this)
    }
    this.bee.bump(ptr)
    return ptr.value
  }

  async get(key, opts) {
    const config = this.config.options(opts)

    let ptr = await this.bootstrap(config)
    if (!ptr) return null

    while (true) {
      const v = ptr.value ? this.bee.bump(ptr) : await this.inflate(ptr, config)

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

  async close() {
    await this.lifetime.end()
  }

  fork() {
    return new HyperbeeSession(this.bee, this.lifetime)
  }

  asSession() {
    return this
  }
}

module.exports = Hyperbee

function noop() {}

function toEncryptionProvider(encryption) {
  if (encryption) return (key) => encryption
  return () => null
}

function getRootCore(store, key, getEncryptionProvider) {
  const encryption = getEncryptionProvider(key)

  return key
    ? store.get({ key, encryption, inflightRange: [256, 512] })
    : store.get({ key, name: 'bee', encryption, inflightRange: [256, 512] })
}
