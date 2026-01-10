const { Readable } = require('streamx')
const b4a = require('b4a')

class RangeIterator {
  constructor(tree, opts = {}) {
    const {
      activeRequests = [],
      prefetch = true,
      reverse = false,
      limit = -1,
      gte,
      gt,
      lte,
      lt
    } = opts

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
    this.prefetching = prefetch
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
          ? b4a.compare(v.keys.get(j).key, this.end)
          : b4a.compare(this.start, v.keys.get(j).key)

        if (c < 0) break
        top.offset = 2 * i + 1 + (c === 0 ? offset : 1)
      }

      const child = (top.offset & 1) === 0
      const k = top.offset >> 1

      if (!child || k >= v.children.length) break

      const j = this.reverse ? v.children.length - 1 - k : k

      this.stack.push({
        node: v.children.get(j),
        offset: 0
      })

      top.offset++
    }
  }

  async next() {
    const key = await this.nextKey()
    return key === null ? key : this.tree.finalizeKeyPointer(key, this.activeRequests)
  }

  async nextKey() {
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
          this.stack.push({ node: v.children.get(j), offset: 0 })
        }
        continue
      }

      if (k < v.keys.length) {
        const j = this.reverse ? v.keys.length - 1 - k : k

        const data = v.keys.get(j)

        const c = this.reverse
          ? this.start
            ? b4a.compare(this.start, data.key)
            : -1
          : this.end
            ? b4a.compare(data.key, this.end)
            : -1

        if (c > this.compare) break

        if (this.prefetching && !v.children.length && this.stack.length) this.prefetch()

        this.stack.push(top)
        if (this.limit !== -1) this.limit--
        return data
      }
    }

    return null
  }

  prefetch() {
    // TODO: dbl check this for off-by-ones with the offset and keys and children
    // TODO: rerun this on the next leaf node that wasnt warm
    this.prefetching = false
    let limit = this.limit

    if (limit < this.tree.context.minKeys) return

    const parent = this.stack[this.stack.length - 1]
    const pv = parent.node.value
    if (!pv) return

    for (let i = parent.offset >> 1; i < pv.children.length; i++) {
      const k = pv.keys.get(i)

      const cmp = this.reverse
        ? this.start
          ? b4a.compare(this.start, k.key)
          : -1
        : this.end
          ? b4a.compare(k.key, this.end)
          : -1

      if (cmp > this.compare) break

      const c = pv.children.get(i)
      if (!c.value) this.tree.inflate(c, this.activeRequests).catch(noop)

      limit = Math.max(limit - this.tree.context.minKeys, 0)
      if (limit === 0) break
    }
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

function noop() {}
