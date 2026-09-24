const { Readable } = require('streamx')
const Hypercore = require('hypercore')

class ChangesStream extends Readable {
  constructor(tree, opts = {}) {
    const { highWaterMark, head = null, prefetch = 128 } = opts
    super({ eagerOpen: true, highWaterMark })

    this.tree = tree

    this.head = head
    this.context = null
    this.config = tree.config.detach(opts)

    this.prefetch = prefetch
    this.range = null
    this.rangeCore = null
    this.rangeStart = 0
  }

  async _openp() {
    await this.tree.bootstrap(this.config)
    if (this.head === null) this.head = this.tree.head()
    if (this.head !== null) this.context = this.tree.context.getContextByKey(this.head.key)
  }

  async _readp() {
    if (!this.context || this.head.length === 0) {
      this.push(null)
      return
    }

    const data = {
      head: this.head,
      tail: null,
      batch: []
    }

    const seq = this.head.length - 1

    this._prefetch(this.context.core, seq)

    const blk = await this.context.getBlock(seq, 0, this.config, null)
    const batchStart = seq - blk.batch.start
    const remaining = new Array(blk.batch.start)

    for (let i = 0; i < remaining.length; i++) {
      remaining[i] = this.context.getBlock(batchStart + i, 0, this.config, null)
    }

    for (const blk of await Promise.all(remaining)) data.batch.push(blk)
    data.batch.push(blk)

    if (!blk.previous) {
      this.head = null
      this.context = null
      this._stopPrefetch()
      this.push(data)
      return
    }

    this.context = await this.context.getContext(blk.previous.core, this.config, blk)
    this.head = data.tail = { key: this.context.core.key, length: blk.previous.seq + 1 }

    this.push(data)
  }

  _prefetch(core, seq) {
    if (!this.prefetch) return
    if (this.range !== null && this.rangeCore === core && seq >= this.rangeStart) return

    this._stopPrefetch()

    const end = seq + 1

    this.rangeStart = Math.max(0, end - this.prefetch)
    this.rangeCore = core
    this.range = core.download({
      start: this.rangeStart,
      end,
      linear: true,
      activeRequests: this.config.activeRequests
    })
  }

  _stopPrefetch() {
    if (this.range === null) return
    this.range.destroy()
    this.range = null
    this.rangeCore = null
  }

  async _open(cb) {
    try {
      await this._openp()
    } catch (err) {
      cb(err)
      return
    }

    cb(null)
  }

  async _read(cb) {
    try {
      await this._readp()
    } catch (err) {
      cb(err)
      return
    }

    cb(null)
  }

  _predestroy() {
    this._stopPrefetch()
    Hypercore.destroyRequests(this.config.activeRequests, null)
  }
}

exports.ChangesStream = ChangesStream
