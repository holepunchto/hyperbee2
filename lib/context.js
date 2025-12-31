const b4a = require('b4a')
const ScopeLock = require('scope-lock')
const { decodeBlock } = require('./encoding.js')

class CoreContext {
  constructor(store, local, core, encryption, lock = new ScopeLock(), other = new Map()) {
    this.store = store
    this.local = local
    this.core = core
    this.encryption = encryption
    this.lock = lock
    this.other = other
    this.length = 0
    this.checkpoint = 0
    this.opened = []
    this.cores = []
    this.changed = false
  }

  async update(activeRequests) {
    await this.core.ready()

    if (this.length === this.core.length || this.core.length === 0) return

    const length = this.core.length
    const seq = length - 1
    const buffer = await this.core.get(seq, { activeRequests })
    const block = decodeBlock(buffer, seq)
    const checkpoint = block.checkpoint

    if (checkpoint > 0) {
      const buffer = await this.core.get(checkpoint - 1)
      const block = decodeBlock(buffer, checkpoint - 1)

      if (length < this.length) return

      this.cores = block.cores || []
      while (this.opened.length < this.cores.length) this.opened.push(null)
    }

    if (length < this.length) return

    this.checkpoint = checkpoint
    this.length = length
  }

  async getCoreOffset(context, core, activeRequests) {
    if (core !== 0 && core - 1 >= context.cores.length) await context.update(activeRequests)
    const key = core === 0 ? context.core.key : context.cores[core - 1].key

    if (b4a.equals(key, this.core.key)) return 0

    // TODO: prop use a map...
    for (let i = 0; i < this.cores.length; i++) {
      const k = this.cores[i].key
      if (b4a.equals(k, key)) {
        return i + 1
      }
    }

    this.changed = true
    this.cores.push({ key, fork: 0, length: 0, treeHash: null })
    this.opened.push(null)

    return this.cores.length
  }

  async getCoreOffsetByKey(key, activeRequests) {
    await this.core.ready()

    if (b4a.equals(key, this.core.key)) return 0

    for (let i = 0; i < this.cores.length; i++) {
      const k = this.cores[i].key
      if (b4a.equals(k, key)) {
        return i + 1
      }
    }

    await this.update(activeRequests)

    for (let i = 0; i < this.cores.length; i++) {
      const k = this.cores[i].key
      if (b4a.equals(k, key)) {
        return i + 1
      }
    }

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
      this.opened[index - 1] = this.store.get({ key, encryption: this.encryption })
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

  async getBlock(seq, core, activeRequests) {
    if (core !== 0 && core - 1 >= this.cores.length) await this.update(activeRequests)
    const hc = this.getCore(core)
    const buffer = await hc.get(seq, { activeRequests })
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

    const hc = this.store.get({ key, encryption: this.encryption })
    const ctx = new CoreContext(this.store, this.local, hc, this.encryption, this.lock, this.other)
    this.other.set(hex, ctx)
    return ctx
  }

  async getContext(core, activeRequests) {
    if (core === 0) return this
    if (core > this.cores.length) await this.update(activeRequests)
    if (core > this.cores.length) throw new Error('Bad core index: ' + core)

    const hex = b4a.toString(this.cores[core - 1].key, 'hex')
    if (this.other.has(hex)) return this.other.get(hex)

    const hc = this.getCore(core)
    const ctx = new CoreContext(this.store, this.local, hc, this.encryption, this.lock, this.other)
    this.other.set(hex, ctx)
    return ctx
  }
}

module.exports = CoreContext
