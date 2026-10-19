const b4a = require('b4a')
const ScopeLock = require('scope-lock')
const c = require('compact-encoding')
const { BLOCK_NOT_AVAILABLE } = require('hypercore-errors')
const { TreeNodePointer } = require('./tree.js')
const { decodeBlock, TYPE_LATEST } = require('./encoding.js')
const Mappings = require('./mappings.js')

const CHECKPOINT_KEY = 'hyperbee/checkpoint'
const INFLIGHT_RANGE = [256, 512]

let ids = 0

class CoreContext {
  constructor(
    store,
    local,
    cache,
    core,
    getEncryptionProvider,
    mappings = new Mappings(),
    lock = new ScopeLock(),
    other = new Map(),
    key = core.key
  ) {
    this.id = ids++
    this.store = store
    this.local = local
    this.cache = cache
    this.core = core
    this.key = key
    this.getEncryptionProvider = getEncryptionProvider
    this.lock = lock
    this.other = other
    this.mappings = mappings
    this.length = 0
    this.checkpoint = 0
    this.invalidation = 0
    this.opened = []
    this.cores = []
    this.changed = false
    this.updateLock = new ScopeLock()
  }

  // TODO: remove, left here for easier debugging for now
  // [Symbol.for('nodejs.util.inspect.custom')]() {
  //   return '[CoreContext]'
  // }

  async update(config) {
    await this.core.ready()

    if (this.length >= this.core.length) return
    await this._inflateCheckpointFromLength(this.core.length, config, true)
  }

  // origin is the (already decoded) block in this core that yielded the
  // reference to `core`, or null. Its checkpoint is enough to resolve the core
  // table, so prefer it over reading the tip of the core, which might not be available.
  async updateMaybe(config, core, origin) {
    await this.core.ready()

    if (!this.needsUpdate(core, origin)) return

    if (origin !== null) {
      await this._inflateCheckpoint(origin.seq + 1, origin, config, false)
      if (!this.needsUpdate(core, origin)) return
    }

    const buf = await this.core.getUserData(CHECKPOINT_KEY)
    const cached = buf ? c.decode(c.uint, buf) : 0

    if (cached > this.length && cached <= this.core.length) {
      await this._inflateCheckpointFromLength(cached, config, false)
      if (!this.needsUpdate(core, origin)) return
    }

    await this.update(config)
  }

  hasCore(core) {
    return core <= this.opened.length
  }

  needsUpdate(core, origin) {
    if (!this.hasCore(core)) return true
    return origin !== null && origin.invalidation > this.invalidation
  }

  async _inflateCheckpointFromLength(length, config, persist) {
    const seq = length - 1
    const buffer = await this.core.get(seq, config)
    if (buffer === null) throw BLOCK_NOT_AVAILABLE()
    const block = decodeBlock(buffer, seq)

    await this._inflateCheckpoint(length, block, config, persist)
  }

  async _inflateCheckpoint(length, block, config, persist) {
    if (length <= this.length) return

    const checkpoint = block.checkpoint
    let cores = null

    if (checkpoint > 0) {
      const seq = checkpoint - 1
      const buffer = await this.core.get(seq, config)
      if (buffer === null) throw BLOCK_NOT_AVAILABLE()
      const block = decodeBlock(buffer, seq)

      cores = block.metadata ? block.metadata.cores : []
      if (cores.length > 0 && block.metadata.mappings !== null) {
        this.mappings.add(block.metadata.mappings)
      }
    }

    if (length <= this.length) return
    if (!(await this.updateLock.lock())) return

    try {
      if (length <= this.length) return

      if (cores !== null) {
        this.cores = cores
        while (this.opened.length < this.cores.length) this.opened.push(null)
      }

      this.checkpoint = checkpoint
      this.invalidation = block.invalidation
      this.length = length

      if (persist) {
        await this.core.setUserData(CHECKPOINT_KEY, c.encode(c.uint, length))
      }
    } finally {
      this.updateLock.unlock()
    }
  }

  createTreeNode(core, seq, offset, changed, value) {
    const ptr = new TreeNodePointer(this, core, seq, offset, changed, value)
    if (ptr.value || changed) return ptr
    const existing = this.cache.get(ptr)
    if (existing) return existing
    return ptr
  }

  async getBlockType(core, config) {
    const hc = this.getCore(core)
    if (hc.length === 0) return TYPE_LATEST
    const block = await this.getBlock(hc.length - 1, core, config, null)
    return block.type
  }

  // the key of the core this context reads, as opened once it is
  coreKey() {
    return this.core.key || this.key
  }

  getCoreOffsetLocalByKey(key) {
    if (b4a.equals(key, this.coreKey())) return 0

    for (let i = 0; i < this.cores.length; i++) {
      const k = this.cores[i].key
      if (b4a.equals(k, key)) {
        return i + 1
      }
    }

    return -1
  }

  getCoreOffsetLocal(context, core) {
    if (context === this) return core

    const key = core === 0 ? context.coreKey() : context.cores[core - 1].key
    const offset = this.getCoreOffsetLocalByKey(key)

    if (offset > -1) return offset

    this.changed = true
    this.cores.push({ key, fork: 0, length: 0, treeHash: null })
    this.opened.push(null)

    return this.cores.length
  }

  async getCoreOffset(context, core, config) {
    if (core !== 0 && core - 1 >= context.cores.length) {
      await context.updateMaybe(config, core, null)
    }
    return this.getCoreOffsetLocal(context, core)
  }

  async getCoreOffsetByKey(key, config) {
    await this.core.ready()

    let offset = this.getCoreOffsetLocalByKey(key)
    if (offset > -1) return offset

    await this.update(config)

    offset = this.getCoreOffsetLocalByKey(key)
    if (offset > -1) return offset

    this.changed = true
    this.cores.push({ key, fork: 0, length: 0, treeHash: null })
    this.opened.push(null)
    return this.cores.length
  }

  // the table keeps keys as they were referenced, a core is mapped when opened
  getCore(index) {
    if (index === 0) return this.core
    if (index > this.cores.length) throw new Error('Bad core index: ' + index)

    const key = this.cores[index - 1].key
    const opened = this.opened[index - 1]
    if (
      opened !== null &&
      (opened.key === null || b4a.equals(opened.key, this.mappings.resolve(key)))
    ) {
      return opened
    }

    this.opened[index - 1] = this._openCore(key)
    return this.opened[index - 1]
  }

  getCoreKey(index) {
    if (index === 0) return this.coreKey()
    if (index > this.cores.length) throw new Error('Bad core index: ' + index)
    const opened = this.opened[index - 1]
    if (opened !== null && opened.key !== null) return opened.key
    return this.mappings.resolve(this.cores[index - 1].key)
  }

  // the core a pointer reads from once opened, null before that
  openedCore(index) {
    if (index === 0) return this.core
    return index > this.opened.length ? null : this.opened[index - 1]
  }

  _openCore(key) {
    return openCore(this.store, key, this.getEncryptionProvider, this.mappings)
  }

  async getBlock(seq, core, config, origin) {
    const buffer = await this.getBlockBuffer(seq, core, config, origin)
    const block = decodeBlock(buffer, seq)

    if (block.invalidation > 0) {
      const ctx = this.getLoadedContext(core)
      if (block.invalidation > ctx.invalidation) await ctx.updateMaybe(config, 0, block)
    }

    return block
  }

  async getBlockBuffer(seq, core, config, origin) {
    if (core !== 0 && core - 1 >= this.cores.length) {
      await this.updateMaybe(config, core, origin)
    }

    const hc = this.getCore(core)
    const buffer = await hc.get(seq, config)
    if (buffer === null) throw BLOCK_NOT_AVAILABLE()

    if (config.trace !== null) config.trace(core, seq)

    return buffer
  }

  getLocalContext() {
    return this.getContextByKey(this.local.key)
  }

  getContextByKey(key) {
    key = this.mappings.resolve(key)
    const mine = this.coreKey()
    if (mine !== null && b4a.equals(key, mine)) return this

    const hex = b4a.toString(key, 'hex')
    if (this.other.has(hex)) return this.other.get(hex)

    const ctx = this._createContext(this._openCore(key), key)
    this.other.set(hex, ctx)
    return ctx
  }

  async getContext(core, config, origin) {
    if (core === 0) return this
    if (core > this.cores.length) await this.updateMaybe(config, core, origin)
    return this.getLoadedContext(core)
  }

  // context of a core this context already knows, eg after reading one of its blocks
  getLoadedContext(core) {
    if (core === 0) return this
    if (core > this.cores.length) throw new Error('Bad core index: ' + core)

    const hc = this.getCore(core)
    const key = this.getCoreKey(core)
    const hex = b4a.toString(key, 'hex')
    if (this.other.has(hex)) return this.other.get(hex)

    const ctx = this._createContext(hc, key)
    this.other.set(hex, ctx)
    return ctx
  }

  flush() {
    this.changed = false
    return {
      cores: this.cores,
      mappings: this.mappings.flush()
    }
  }

  _createContext(core, key) {
    const store = this.store
    const local = this.local
    const cache = this.cache
    const getEncryptionProvider = this.getEncryptionProvider
    const mappings = this.mappings
    const lock = this.lock
    const other = this.other
    return new CoreContext(
      store,
      local,
      cache,
      core,
      getEncryptionProvider,
      mappings,
      lock,
      other,
      key
    )
  }
}

// a core whose mapping is not known yet is opened through a preload that asks
// for it, so the session only ever opens on the core it maps to
function openCore(store, key, getEncryptionProvider, mappings) {
  const known = mappings.get(key)
  if (known !== null || mappings.hook === null) {
    const mapped = known === null ? key : known
    return store.get({
      key: mapped,
      encryption: getEncryptionProvider(mapped),
      inflightRange: INFLIGHT_RANGE
    })
  }

  return store.get({
    key,
    encryption: getEncryptionProvider(key),
    inflightRange: INFLIGHT_RANGE,
    preload: preloadCore(key, getEncryptionProvider, mappings)
  })
}

async function preloadCore(key, getEncryptionProvider, mappings) {
  const mapped = await mappings.map(key)
  return { key: mapped, encryption: getEncryptionProvider(mapped) }
}

CoreContext.openCore = openCore

module.exports = CoreContext
