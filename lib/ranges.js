const { Readable } = require('streamx')
const b4a = require('b4a')

class RangeIterator {
  constructor(tree, client, opts = {}) {
    const { prefetch = true, reverse = false, limit = -1, gte, gt, lte, lt } = opts

    this.tree = tree
    this.client = client
    this.root = null
    this.stack = []
    this.reverse = reverse
    this.limit = limit
    this.start = gte || gt || null
    this.end = lte || lt || null
    this.inclusive = !!(reverse ? lte : gte)
    this.compare = (reverse ? gte : lte) ? 0 : -1
    this.prefetch = prefetch
    this.prefetching = null
  }

  async open() {
    if (!this.root) this.root = await this.tree.bootstrap(this.client)
    if (!this.root) return

    if (this.limit === 0) return

    this.stack.push({ node: this.root, offset: 0 })

    if (this.reverse ? !this.end : !this.start) return

    const offset = this.inclusive ? 0 : 1

    while (true) {
      const top = this.stack[this.stack.length - 1]
      const v = top.node.value
        ? this.tree.bump(top.node)
        : await this.tree.inflate(top.node, this.client)

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
    return key === null ? key : this.tree.finalizeKeyPointer(key, this.client)
  }

  async nextKey() {
    while (this.stack.length && (this.limit === -1 || this.limit > 0)) {
      const top = this.stack.pop()
      const v = top.node.value
        ? this.tree.bump(top.node)
        : await this.tree.inflate(top.node, this.client)

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

        if (this.prefetch && !v.children.length && this.stack.length) {
          this.prefetchNext(v.t - 1)
        }

        this.stack.push(top)
        if (this.limit !== -1) this.limit--
        return data
      }
    }

    return null
  }

  prefetchNext(minKeys) {
    // every sibling covers at least minKeys keys plus the parent separator
    // emitted after it
    const size = minKeys + 1

    let limit = this.limit === -1 ? Infinity : this.limit

    // the current leaf (whose key is being emitted now) covers the rest
    if (limit <= size) return

    const parent = this.stack[this.stack.length - 1]
    const pv = parent.node.value
    if (!pv || pv === this.prefetching) return

    this.prefetching = pv

    for (let i = parent.offset >> 1; i < pv.children.length; i++) {
      const j = this.reverse ? pv.children.length - 1 - i : i

      // If the separator key bounding this child is beyond the range,
      // stop fetching child nodes.
      if (i > 0) {
        const k = this.reverse ? pv.keys.get(j) : pv.keys.get(j - 1)

        const cmp = this.reverse
          ? this.start
            ? b4a.compare(this.start, k.key)
            : -1
          : this.end
            ? b4a.compare(k.key, this.end)
            : -1

        if (cmp > this.compare) return
      }

      const c = pv.children.get(j)
      if (!c.value) this.tree.inflate(c, this.client).catch(noop)

      limit -= size
      if (limit <= 0) break
    }
  }
}

class RangeStream extends Readable {
  constructor(tree, options = {}) {
    const { highWaterMark } = options
    super({ eagerOpen: true, highWaterMark })

    this.tree = tree
    this.session = tree.session.fork()

    const client = this.session.client(options)
    this.iterator = new RangeIterator(tree, client, options)
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

  _predestroy() {
    this.session.destroy()
  }
}

exports.RangeIterator = RangeIterator
exports.RangeStream = RangeStream

function noop() {}
