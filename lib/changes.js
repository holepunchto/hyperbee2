const { Readable } = require('streamx')

class ChangesStream extends Readable {
  constructor(tree, opts = {}) {
    const { highWaterMark, head = null } = opts
    super({ eagerOpen: true, highWaterMark })

    this.tree = tree

    this.head = head
    this.context = null
    this.config = tree.config.options(opts)
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
    const blk = await this.context.getBlock(seq, 0, this.config)
    const batchStart = seq - blk.batch.start
    const remaining = new Array(blk.batch.start)

    for (let i = 0; i < remaining.length; i++) {
      remaining[i] = this.context.getBlock(batchStart + i, 0, this.config)
    }

    for (const blk of await Promise.all(remaining)) data.batch.push(blk)
    data.batch.push(blk)

    if (!blk.previous) {
      this.head = null
      this.context = null
      this.push(data)
      return
    }

    this.context = await this.context.getContext(blk.previous.core, this.config)
    this.head = data.tail = { key: this.context.core.key, length: blk.previous.seq + 1 }

    this.push(data)
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
}

exports.ChangesStream = ChangesStream
