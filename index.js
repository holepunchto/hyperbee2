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
const { Pointer, KeyPointer, ValuePointer, TreeNode, EMPTY } = require('./lib/tree.js')
const { DeltaOp, DeltaCohort, OP_COHORT } = require('./lib/compression.js')

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

  // TODO: unslab these
  async inflate(ptr, config) {
    if (ptr.value) {
      this.bump(ptr)
      return ptr.value
    }

    const [block, context] = await Promise.all([
      ptr.context.getBlock(ptr.seq, ptr.core, config),
      ptr.context.getContext(ptr.core, config)
    ])

    const tree = block.tree[ptr.offset]

    const keys = new Array(tree.keys.length)
    const children = new Array(tree.children.length)

    for (let i = 0; i < keys.length; i++) {
      const d = tree.keys[i]
      keys[i] = inflateKey(context, d, ptr, block, config)
    }

    for (let i = 0; i < children.length; i++) {
      const d = tree.children[i]
      children[i] = inflateChild(context, d, ptr, block, config)
    }

    const [k, c] = await Promise.all([Promise.all(keys), Promise.all(children)])

    const value = new TreeNode(k, c)
    if (!ptr.value) ptr.value = value

    this.bump(ptr)

    return ptr.value
  }

  async finalizeKeyPointer(key, config) {
    const value = key.value || (await this.inflateValue(key, config))

    return {
      core: key.context.getCore(key.core),
      offset: key.offset,
      seq: key.seq,
      key: key.key,
      value
    }
  }

  async inflateValue(key, config) {
    if (key.value) return key.value
    if (!key.valuePointer) return null

    const ptr = key.valuePointer

    if (ptr.split === 0) {
      const block = await ptr.context.getBlock(ptr.seq, ptr.core, config)
      return block.values[ptr.offset]
    }

    const blockPromises = new Array(ptr.split + 1)
    for (let i = 0; i < blockPromises.length; i++) {
      blockPromises[i] = ptr.context.getBlock(ptr.seq - ptr.split + i, ptr.core, config)
    }
    const blocks = await Promise.all(blockPromises)
    const splitValue = new Array(blockPromises.length)
    for (let i = 0; i < splitValue.length - 1; i++) {
      splitValue[i] = blocks[i].values[0]
    }
    splitValue[splitValue.length - 1] = blocks[blocks.length - 1].buffer[ptr.offset]
    return b4a.concat(splitValue)
  }

  async bootstrap(config) {
    if (!this.root) await this.ready()
    if (this.unbatch) await this._rollback(config)
    return this.root === EMPTY ? null : this.root
  }

  async _rollback(config) {
    let seq = this.root.seq
    let context = this.context
    let count = this.unbatch

    while (count) {
      const blk = await context.getBlock(seq, 0, config)
      // TODO: should this be an error case? Attempting to
      // rollback more batches than exist in the store seems like
      // a programming error.
      if (!blk.previous) break
      seq = blk.previous.seq
      context = await context.getContext(blk.previous.core, config)
      count--
    }

    if (count) {
      // TODO: should this be an error case? Attempting to
      // rollback more batches than exist in the store seems like
      // a programming error.
      this.root = EMPTY
    } else {
      this.root = context.createTreeNode(0, seq, 0, false, null)
    }
    this.context = context
    this.unbatch = 0
    this.emit('update')
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

function inflateKey(context, d, ptr, block, config) {
  if (d.type === OP_COHORT) return inflateKeyCohort(context, d, ptr, block, config)
  return inflateKeyDelta(context, d, ptr, block, config)
}

async function inflateKeyDelta(context, d, ptr, block, config) {
  const k = d.pointer

  if (!k) return new DeltaOp(false, d.type, d.index, null)

  const blk =
    k.seq === ptr.seq && k.core === 0 && ptr.core === 0
      ? block
      : await context.getBlock(k.seq, k.core, config)

  const bk = blk.keys[k.offset]

  let vp = null

  if (bk.valuePointer) {
    const p = bk.valuePointer
    const ctx = await context.getContext(k.core, config)
    vp = new ValuePointer(ctx, p.core, p.seq, p.offset, p.split)
  }

  const kp = new KeyPointer(context, k.core, k.seq, k.offset, false, bk.key, bk.value, vp)
  return new DeltaOp(false, d.type, d.index, kp)
}

async function inflateKeyCohort(context, d, ptr, block, config) {
  const co = d.pointer

  const blk =
    co.seq === ptr.seq && co.core === 0 && ptr.core === 0
      ? block
      : await context.getBlock(co.seq, co.core, config)

  const cohort = blk.cohorts[co.offset]
  const promises = new Array(cohort.length)

  for (let i = 0; i < cohort.length; i++) {
    const p = cohort[i]
    const k = inflateKeyDelta(context, p, co, blk, config)
    promises[i] = k
  }

  const p = new Pointer(context, co.core, co.seq, co.offset)
  return new DeltaCohort(false, p, await Promise.all(promises))
}

async function inflateChild(context, d, ptr, block, config) {
  if (d.type === OP_COHORT) return inflateChildCohort(context, d, ptr, block, config)
  if (d.pointer && !context.hasCore(d.pointer.core)) await context.update(config)
  return inflateChildDelta(context, d, ptr, block, config)
}

function inflateChildDelta(context, d, ptr, block, config) {
  const p = d.pointer
  const c = p && context.createTreeNode(p.core, p.seq, p.offset, false, null)
  return new DeltaOp(false, d.type, d.index, c)
}

async function inflateChildCohort(context, d, ptr, block, config) {
  const co = d.pointer

  const blk =
    co.seq === ptr.seq && co.core === 0 && ptr.core === 0
      ? block
      : await context.getBlock(co.seq, co.core, config)

  const cohort = blk.cohorts[co.offset]
  const deltas = new Array(cohort.length)

  for (let i = 0; i < cohort.length; i++) {
    const c = cohort[i]
    if (c.pointer && !context.hasCore(c.pointer.core)) await context.update(config)
    deltas[i] = inflateChildDelta(context, c, co, blk, config)
  }

  const p = new Pointer(context, co.core, co.seq, co.offset)
  return new DeltaCohort(false, p, deltas)
}
