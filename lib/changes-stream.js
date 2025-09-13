const { Readable } = require('streamx')

module.exports = class ChangesStream extends Readable {
  constructor (tree, options = {}) {
    super({ eagerOpen: true })

    const {
      head = null,
      activeRequests = tree.activeRequests
    } = options

    this.tree = tree

    this._head = head
    this._context = null
    this._activeRequests = activeRequests
  }

  async _openp () {
    await this.tree.ready()
    if (this._head === null) this._head = this.tree.head()
    if (this._head !== null) this._context = this.tree.context.getContextByKey(this._head.key)
  }

  async _readp () {
    if (!this._context || this._head.length === 0) {
      this.push(null)
      return
    }

    const data = {
      head: this._head,
      batch: []
    }

    const seq = this._head.length - 1
    const blk = await this._context.getBlock(seq, 0, this._activeRequests)
    const batchStart = seq - blk.batch.start
    const remaining = new Array(blk.batch.start)

    for (let i = 0; i < remaining.length; i++) {
      remaining[i] = this._context.getBlock(batchStart + i, 0, this._activeRequests)
    }

    for (const blk of await Promise.all(remaining)) data.batch.push(blk)
    data.batch.push(blk)

    this.push(data)

    if (!blk.previous) {
      this._head = null
      this._context = null
      return
    }

    this._context = await this._context.getContext(blk.previous.core, this._activeRequests)
    this._head = { key: this._context.core.key, length: blk.previous.seq + 1 }
  }

  async _open (cb) {
    try {
      await this._openp()
    } catch (err) {
      cb(err)
      return
    }

    cb(null)
  }

  async _read (cb) {
    try {
      await this._readp()
    } catch (err) {
      cb(err)
      return
    }

    cb(null)
  }
}
