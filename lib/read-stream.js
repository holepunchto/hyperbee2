const { Readable } = require('streamx')
const b4a = require('b4a')

module.exports = class KeyValueStream extends Readable {
  constructor (tree, options = {}) {
    super({ eagerOpen: true })

    this.tree = tree
    this.root = null

    this._stack = []
    this._limit = options.limit === undefined ? -1 : options.limit
    this._reverse = !!options.reverse
    this._start = options.gte || options.gt || null
    this._end = options.lte || options.lt || null
    this._startCompare = options.gte ? 0 : 1
    this._endCompare = options.lte ? 0 : -1

    if (this._reverse) throw new Error('Not implmented')
  }

  async _openp () {
    if (!this.tree.root) await this.tree.bootstrap()
    if (!this.root) this.root = this.tree.root
    if (!this.root) return

    this._stack.push({ node: this.root, offset: 0 })
    if (!this._start || this._limit === 0) return

    while (true) {
      const top = this._stack[this._stack.length - 1]
      if (!top.node.value) await this.tree.inflate(top.node)

      for (let i = 0; i < top.node.value.keys.length; i++) {
        const c = b4a.compare(this._start, top.node.value.keys[i].key)
        if (c < 0) break
        top.offset = 2 * i + 1 + (c === 0 ? this._startCompare : 1)
      }

      const child = (top.offset & 1) === 0
      const k = top.offset >> 1

      if (!child || k >= top.node.value.children.length) break

      this._stack.push({
        node: top.node.value.children[k],
        offset: 0
      })

      top.offset++
    }
  }

  async _readp () {
    while (this._stack.length && (this._limit === -1 || this._limit > 0)) {
      const top = this._stack.pop()
      if (!top.node.value) await this.tree.inflate(top.node)

      const offset = top.offset++
      const child = (offset & 1) === 0
      const k = offset >> 1

      if (child) {
        this._stack.push(top)
        if (k < top.node.value.children.length) {
          this._stack.push({ node: top.node.value.children[k], offset: 0 })
        }
        continue
      }

      if (k < top.node.value.keys.length) {
        const data = top.node.value.keys[k]
        const c = this._end ? b4a.compare(data.key, this._end) : -1
        if (c > this._endCompare) break
        this._stack.push(top)
        this.push(this._finalize(data))
        if (this._limit !== -1) this._limit--
        return
      }
    }

    this.push(null)
  }

  _finalize (kv) {
    return kv
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
