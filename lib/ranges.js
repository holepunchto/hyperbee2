const { Readable } = require('streamx')
const b4a = require('b4a')

class RangeIterator {
  constructor(tree, { activeRequests = [], reverse = false, limit = -1, gte, gt, lte, lt } = {}) {
    this.tree = tree
    this.root = null
    this.stack = []
    this.activeRequests = activeRequests
    this.reverse = reverse
    this.limit = limit
    this.start = gte || gt || null
    this.end = lte || lt || null
    this.inclusive = !!(reverse ? lte : gte)
    this.compare = (reverse ? gte : lte) ? 0 : -1
  }

  async open() {
    if (!this.root) this.root = await this.tree.bootstrap(this.activeRequests)
    if (!this.root) return

    if (this.limit === 0) return

    this.stack.push({ node: this.root, offset: 0 })

    if (this.reverse ? !this.end : !this.start) return

    const offset = this.inclusive ? 0 : 1

    while (true) {
      const top = this.stack[this.stack.length - 1]
      const v = top.node.value
        ? this.tree.bump(top.node)
        : await this.tree.inflate(top.node, this.activeRequests)

      for (let i = 0; i < v.keys.length; i++) {
        const j = this.reverse ? v.keys.length - 1 - i : i

        const c = this.reverse
          ? b4a.compare(v.keys[j].key, this.end)
          : b4a.compare(this.start, v.keys[j].key)

        if (c < 0) break
        top.offset = 2 * i + 1 + (c === 0 ? offset : 1)
      }

      const child = (top.offset & 1) === 0
      const k = top.offset >> 1

      if (!child || k >= v.children.length) break

      const j = this.reverse ? v.children.length - 1 - k : k

      this.stack.push({
        node: v.children[j],
        offset: 0
      })

      top.offset++
    }
  }

  async next() {
    while (this.stack.length && (this.limit === -1 || this.limit > 0)) {
      const top = this.stack.pop()
      const v = top.node.value
        ? this.tree.bump(top.node)
        : await this.tree.inflate(top.node, this.activeRequests)

      const offset = top.offset++
      const child = (offset & 1) === 0
      const k = offset >> 1

      if (child) {
        this.stack.push(top)
        if (k < v.children.length) {
          const j = this.reverse ? v.children.length - 1 - k : k
          this.stack.push({ node: v.children[j], offset: 0 })
        }
        continue
      }

      if (k < v.keys.length) {
        const j = this.reverse ? v.keys.length - 1 - k : k

        const data = top.node.value.keys[j]

        const c = this.reverse
          ? this.start
            ? b4a.compare(this.start, data.key)
            : -1
          : this.end
            ? b4a.compare(data.key, this.end)
            : -1

        if (c > this._compare) break

        this.stack.push(top)
        if (this.limit !== -1) this.limit--
        return data
      }
    }

    return null
  }
}

class RangeStream extends Readable {
  constructor(tree, options = {}) {
    const { highWaterMark } = options
    super({ eagerOpen: true, highWaterMark })

    this.tree = tree
    this.iterator = new RangeIterator(tree, options)
  }

  async _open(cb) {
    try {
      await this.iterator.open()
    } catch (err) {
      cb(err)
      return
    }
    cb(null)
  }

  async _read(cb) {
    try {
      this.push(await this.iterator.next())
    } catch (err) {
      cb(err)
      return
    }
    cb(null)
  }
}

exports.RangeIterator = RangeIterator
exports.RangeStream = RangeStream
