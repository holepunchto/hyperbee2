const b4a = require('b4a')
const ScopeLock = require('scope-lock')
const c = require('compact-encoding')
const { BLOCK_NOT_AVAILABLE } = require('hypercore-errors')
const { TreeNodePointer } = require('./tree.js')
const { decodeBlock, TYPE_LATEST } = require('./encoding.js')

const CHECKPOINT_KEY = 'hyperbee/checkpoint'

class CoreContext {
  constructor(
    store,
    local,
    cache,
    core,
    getEncryptionProvider,
    lock = new ScopeLock(),
    other = new Map()
  ) {
    this.store = store
    this.local = local
    this.cache = cache
    this.core = core
    this.getEncryptionProvider = getEncryptionProvider
    this.lock = lock
    this.other = other
    this.length = 0
    this.checkpoint = 0
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

    if (this.hasCore(core)) return

    if (origin !== null) {
      await this._inflateCheckpoint(origin.seq + 1, origin, config, false)
      if (this.hasCore(core)) return
    }

    const buf = await this.core.getUserData(CHECKPOINT_KEY)
    const cached = buf ? c.decode(c.uint, buf) : 0

    if (cached > this.length && cached <= this.core.length) {
      await this._inflateCheckpointFromLength(cached, config, false)
      if (this.hasCore(core)) return
    }

    await this.update(config)
  }

  hasCore(core) {
    return core <= this.opened.length
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

  getCoreOffsetLocalByKey(key) {
    if (b4a.equals(key, this.core.key)) return 0

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

    const key = core === 0 ? context.core.key : context.cores[core - 1].key
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

  getCore(index) {
    if (index === 0) return this.core
    if (index > this.cores.length) throw new Error('Bad core index: ' + index)
    if (this.opened[index - 1] === null) {
      const key = this.cores[index - 1].key
      this.opened[index - 1] = this.store.get({
        key,
        encryption: this.getEncryptionProvider(key),
        inflightRange: [256, 512]
      })
    }
    return this.opened[index - 1]
  }

  getCoreKey(index) {
    if (index === 0) return this.core.key
    if (index > this.cores.length) throw new Error('Bad core index: ' + index)
    if (this.opened[index - 1] !== null && this.opened[index - 1].key) {
      return this.opened[index - 1].key
    }
    return this.cores[index - 1].key
  }

  async getBlock(seq, core, config, origin) {
    if (core !== 0 && core - 1 >= this.cores.length) {
      await this.updateMaybe(config, core, origin)
    }

    const hc = this.getCore(core)
    const buffer = await hc.get(seq, config)
    if (buffer === null) throw BLOCK_NOT_AVAILABLE()

    if (config.trace !== null) config.trace(core, seq)

    const block = decodeBlock(buffer, seq)
    return block
  }

  getLocalContext() {
    return this.getContextByKey(this.local.key)
  }

  getContextByKey(key) {
    if (b4a.equals(key, this.core.key)) return this

    const hex = b4a.toString(key, 'hex')
    if (this.other.has(hex)) return this.other.get(hex)

    const ctx = this._createContext(
      this.store.get({
        key,
        encryption: this.getEncryptionProvider(key),
        inflightRange: [256, 512]
      })
    )
    this.other.set(hex, ctx)
    return ctx
  }

  async getContext(core, config, origin) {
    if (core === 0) return this
    if (core > this.cores.length) await this.updateMaybe(config, core, origin)
    if (core > this.cores.length) throw new Error('Bad core index: ' + core)

    const hex = b4a.toString(this.cores[core - 1].key, 'hex')
    if (this.other.has(hex)) return this.other.get(hex)

    const ctx = this._createContext(this.getCore(core))
    this.other.set(hex, ctx)
    return ctx
  }

  flush() {
    this.changed = false
    return {
      cores: this.cores
    }
  }

  _createContext(core) {
    const store = this.store
    const local = this.local
    const cache = this.cache
    const getEncryptionProvider = this.getEncryptionProvider
    const lock = this.lock
    const other = this.other
    return new CoreContext(store, local, cache, core, getEncryptionProvider, lock, other)
  }
}

module.exports = CoreContext
