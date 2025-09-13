const b4a = require('b4a')
const { decodeBlock } = require('./encoding.js')

class CoreContext {
  constructor (store, core, other = new Map()) {
    this.store = store
    this.core = core
    this.other = other
    this.length = 0
    this.checkpoint = 0
    this.opened = []
    this.cores = []
    this.changed = false
  }

  async update (activeRequests) {
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

  async getCoreOffset (context, core, activeRequests) {
    if (core !== 0 && core - 1 >= context.cores.length) await context.update(activeRequests)
    const key = core === 0 ? context.core.key : context.cores[core - 1]

    if (b4a.equals(key, this.core.key)) return 0

    // TODO: prop use a map...
    for (let i = 0; i < this.cores.length; i++) {
      const k = this.cores[i]
      if (b4a.equals(k, key)) {
        return i + 1
      }
    }

    this.changed = true
    this.cores.push(key)
    return this.cores.length
  }

  async getCoreOffsetByKey (key, activeRequests) {
    await this.core.ready()

    if (b4a.equals(key, this.core.key)) return 0

    for (let i = 0; i < this.cores.length; i++) {
      const k = this.cores[i]
      if (b4a.equals(k, key)) {
        return i + 1
      }
    }

    await this.update(activeRequests)

    for (let i = 0; i < this.cores.length; i++) {
      const k = this.cores[i]
      if (b4a.equals(k, key)) {
        return i + 1
      }
    }

    this.changed = true
    this.cores.push(key)
    return this.cores.length
  }

  getCore (index) {
    if (index === 0) return this.core
    if (index > this.cores.length) throw new Error('Bad core index: ' + index)
    if (this.opened[index - 1] === null) this.opened[index - 1] = this.store.get(this.cores[index - 1])
    return this.opened[index - 1]
  }

  async getBlock (seq, core, activeRequests) {
    if (core !== 0) await this.update(activeRequests)
    const hc = this.getCore(core)
    const buffer = await hc.get(seq, { activeRequests })
    const block = decodeBlock(buffer, seq)
    return block
  }

  getContextByKey (key) {
    const hex = b4a.toString(key, 'hex')
    if (this.other.has(hex)) return this.other.get(hex)

    const ctx = new CoreContext(this.store, this.store.get(key))
    this.other.set(hex, ctx)
    return ctx
  }

  async getContext (core, activeRequests) {
    if (core === 0) return this
    if (core > this.cores.length) await this.update(activeRequests)
    if (core > this.cores.length) throw new Error('Bad core index: ' + core)

    const hex = b4a.toString(this.cores[core - 1], 'hex')
    if (this.other.has(hex)) return this.other.get(hex)

    const hc = this.getCore(core)
    const ctx = new CoreContext(this.store, hc)
    this.other.set(hex, ctx)
    return ctx
  }
}

module.exports = CoreContext
