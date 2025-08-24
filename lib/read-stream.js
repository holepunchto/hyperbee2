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
    this._compare = (this._reverse ? options.lte : options.gte) ? 0 : 1
  }

  async _openp () {
    if (!this.tree.root) await this.tree.bootstrap()
    if (!this.root) this.root = this.tree.root
    if (!this.root) return

    if (this._limit === 0) return

    this._stack.push({ node: this.root, offset: 0 })

    if (this._reverse ? !this._end : !this._start) return

    while (true) {
      const top = this._stack[this._stack.length - 1]

      if (top.node.value) this.tree.bump(top.node)
      else await this.tree.inflate(top.node)

      const v = top.node.value

      for (let i = 0; i < v.keys.length; i++) {
        const j = this._reverse
          ? v.keys.length - 1 - i
          : i

        const c = this._reverse
          ? b4a.compare(v.keys[j].key, this._end)
          : b4a.compare(this._start, v.keys[j].key)

        if (c < 0) break
        top.offset = 2 * i + 1 + (c === 0 ? this._compare : 1)
      }

      const child = (top.offset & 1) === 0
      const k = top.offset >> 1

      if (!child || k >= v.children.length) break

      const j = this._reverse
        ? v.children.length - 1 - k
        : k

      this._stack.push({
        node: v.children[j],
        offset: 0
      })

      top.offset++
    }
  }

  async _readp () {
    while (this._stack.length && (this._limit === -1 || this._limit > 0)) {
      const top = this._stack.pop()

      if (top.node.value) this.tree.bump(top.node)
      else await this.tree.inflate(top.node)

      const v = top.node.value
      const offset = top.offset++
      const child = (offset & 1) === 0
      const k = offset >> 1

      if (child) {
        this._stack.push(top)
        if (k < v.children.length) {
          const j = this._reverse
            ? v.children.length - 1 - k
            : k
          this._stack.push({ node: v.children[j], offset: 0 })
        }
        continue
      }

      if (k < v.keys.length) {
        const j = this._reverse
          ? v.keys.length - 1 - k
          : k

        const data = top.node.value.keys[j]

        const c = this._reverse
          ? this._start ? b4a.compare(this._start, data.key) : -1
          : this._end ? b4a.compare(data.key, this._end) : -1

        if (c > this._compare) break

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
