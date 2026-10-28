const { Readable } = require('streamx')

// Readable that registers itself on the owning Hyperbee so close() can destroy it
class HyperbeeStream extends Readable {
  constructor(tree, { highWaterMark } = {}) {
    super({ eagerOpen: true, highWaterMark })

    this.tree = tree
    this.index = 0
    tree.addStream(this)
  }

  _destroy(cb) {
    this.tree.removeStream(this)
    cb()
  }
}

module.exports = HyperbeeStream
