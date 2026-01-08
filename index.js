const b4a = require('b4a')
const Hypercore = require('hypercore')
const { RangeStream } = require('./lib/ranges.js')
const { DiffStream } = require('./lib/diff.js')
const { ChangesStream } = require('./lib/changes.js')
const NodeCache = require('./lib/cache.js')
const WriteBatch = require('./lib/write.js')
const CoreContext = require('./lib/context.js')
const {
  Pointer,
  KeyPointer,
  ValuePointer,
  TreeNode,
  TreeNodePointer,
  EMPTY
} = require('./lib/tree.js')
const { DeltaOp, DeltaCohort, OP_COHORT } = require('./lib/compression.js')

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
    this.cache.bump(ptr)
    this.cache.gc()
    return ptr.value
  }

  // TODO: unslab these and parallize
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
      const d = tree.keys[i]
      keys[i] = await inflateKey(context, d, ptr, block, activeRequests)
    }

    for (let i = 0; i < children.length; i++) {
      const d = tree.children[i]
      children[i] = await inflateChild(context, d, ptr, block, activeRequests)
    }

    ptr.value = new TreeNode(keys, children)
    this.bump(ptr)

    return ptr.value
  }

  async finalizeKeyPointer(key, activeRequests = this.activeRequests) {
    const value = key.value || (await this.inflateValue(key, activeRequests))

    return {
      core: key.core,
      offset: key.offset,
      seq: key.seq,
      key: key.key,
      value
    }
  }

  async inflateValue(key, activeRequests = this.activeRequests) {
    if (key.value) return key.value
    if (!key.valuePointer) return null

    const ptr = key.valuePointer

    if (ptr.split === 0) {
      const block = await ptr.context.getBlock(ptr.seq, ptr.core, activeRequests)
      return block.values[ptr.offset]
    }

    const blockPromises = new Array(ptr.split + 1)
    for (let i = 0; i < blockPromises.length; i++) {
      blockPromises[i] = ptr.context.getBlock(ptr.seq - ptr.split + i, ptr.core, activeRequests)
    }
    const blocks = await Promise.all(blockPromises)
    const splitValue = new Array(blockPromises.length)
    for (let i = 0; i < splitValue.length - 1; i++) {
      splitValue[i] = blocks[i].values[0]
    }
    splitValue[splitValue.length - 1] = blocks[blocks.length - 1].buffer[ptr.offset]
    return b4a.concat(splitValue)
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
        const m = v.keys.get(mid)

        c = b4a.compare(key, m.key)

        if (c === 0) return this.finalizeKeyPointer(m, activeRequests)

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

function inflateKey(context, d, ptr, block, activeRequests) {
  if (d.type === OP_COHORT) return inflateKeyCohort(context, d, ptr, block, activeRequests)
  return inflateKeyDelta(context, d, ptr, block, activeRequests)
}

async function inflateKeyDelta(context, d, ptr, block, activeRequests) {
  const k = d.pointer

  if (!k) return new DeltaOp(false, d.type, d.index, null)

  const blk =
    k.seq === ptr.seq && k.core === 0 && ptr.core === 0
      ? block
      : await context.getBlock(k.seq, k.core, activeRequests)

  const bk = blk.keys[k.offset]

  let vp = null

  if (bk.valuePointer) {
    const p = bk.valuePointer
    const ctx = await context.getContext(k.core, activeRequests)
    vp = new ValuePointer(ctx, p.core, p.seq, p.offset, p.split)
  }

  const kp = new KeyPointer(context, k.core, k.seq, k.offset, false, bk.key, bk.value, vp)
  return new DeltaOp(false, d.type, d.index, kp)
}

async function inflateKeyCohort(context, d, ptr, block, activeRequests) {
  const co = d.pointer

  const blk =
    co.seq === ptr.seq && co.core === 0 && ptr.core === 0
      ? block
      : await context.getBlock(co.seq, co.core, activeRequests)

  const cohort = blk.cohorts[co.offset]
  const promises = new Array(cohort.length)

  for (let i = 0; i < cohort.length; i++) {
    const p = cohort[i]
    const k = inflateKeyDelta(context, p, co, blk, activeRequests)
    promises[i] = k
  }

  const p = new Pointer(context, co.core, co.seq, co.offset)
  return new DeltaCohort(false, p, await Promise.all(promises))
}

function inflateChild(context, d, ptr, block, activeRequests) {
  if (d.type === OP_COHORT) return inflateChildCohort(context, d, ptr, block, activeRequests)
  return Promise.resolve(inflateChildDelta(context, d, ptr, block, activeRequests))
}

function inflateChildDelta(context, d, ptr, block, activeRequests) {
  const p = d.pointer
  const c = p && new TreeNodePointer(context, p.core, p.seq, p.offset, false, null)
  return new DeltaOp(false, d.type, d.index, c)
}

async function inflateChildCohort(context, d, ptr, block, activeRequests) {
  const co = d.pointer

  const blk =
    co.seq === ptr.seq && co.core === 0 && ptr.core === 0
      ? block
      : await context.getBlock(co.seq, co.core, activeRequests)

  const cohort = blk.cohorts[co.offset]
  const deltas = new Array(cohort.length)

  for (let i = 0; i < cohort.length; i++) {
    const c = cohort[i]
    deltas[i] = inflateChildDelta(context, c, co, blk, activeRequests)
  }

  const p = new Pointer(context, co.core, co.seq, co.offset)
  return new DeltaCohort(false, p, deltas)
}
