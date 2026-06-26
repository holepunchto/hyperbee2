const { Readable } = require('streamx')
const Hypercore = require('hypercore')
const b4a = require('b4a')

class RangeIterator {
  constructor(tree, opts = {}) {
    const {
      config = tree.config.detach(opts),
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
    this.config = config
    this.reverse = reverse
    this.limit = limit
    this.start = gte || gt || null
    this.end = lte || lt || null
    this.inclusive = !!(reverse ? lte : gte)
    this.compare = (reverse ? gte : lte) ? 0 : -1
    this.prefetch = prefetch && !this.config.localOnly
    this.prefetching = null
  }

  async open() {
    if (!this.root) this.root = await this.tree.bootstrap(this.config)
    if (!this.root) return

    if (this.limit === 0) return

    this.stack.push({ node: this.root, offset: 0 })

    if (this.reverse ? !this.end : !this.start) return

    const offset = this.inclusive ? 0 : 1

    while (true) {
      const top = this.stack[this.stack.length - 1]
      const v = top.node.value
        ? this.tree.bump(top.node)
        : await this.tree.inflate(top.node, this.config)
      if (!v) {
        this.stack.pop()
        if (this.stack.length === 0) break
        continue
      }

      for (let i = 0; i < v.keys.length; i++) {
        const j = this.reverse ? v.keys.length - 1 - i : i
        const key = v.keys.get(j)

        // skip non-local keys
        if (this.config.localOnly && !key) continue

        const c = this.reverse ? b4a.compare(key.key, this.end) : b4a.compare(this.start, key.key)

        if (c < 0) break
        top.offset = 2 * i + 1 + (c === 0 ? offset : 1)
      }

      const child = (top.offset & 1) === 0
      const k = top.offset >> 1

      if (!child || k >= v.children.length) break

      const j = this.reverse ? v.children.length - 1 - k : k
      const node = v.children.get(j)

      if (!this.config.localOnly || node) {
        this.stack.push({ node, offset: 0 })
      }

      top.offset++
      if (this.config.localOnly && !node) break
    }
  }

  async next() {
    let key = await this.nextKey()
    if (key === null) return null

    if (!this.config.localOnly) {
      return this.tree.finalizeKeyPointer(key, this.config)
    }

    // for local read-stream, iterate until we find a non null entry or run out of keys
    while (true) {
      const entry = await this.tree.finalizeKeyPointer(key, this.config)
      if (entry !== null) return entry
      key = await this.nextKey()
      if (key === null) return null
    }
  }

  async nextKey() {
    while (this.stack.length && (this.limit === -1 || this.limit > 0)) {
      const top = this.stack.pop()
      const v = top.node.value
        ? this.tree.bump(top.node)
        : await this.tree.inflate(top.node, this.config)
      if (!v) continue

      const offset = top.offset++
      const child = (offset & 1) === 0
      const k = offset >> 1

      if (child) {
        this.stack.push(top)
        if (k < v.children.length) {
          const j = this.reverse ? v.children.length - 1 - k : k
          const node = v.children.get(j)
          if (!this.config.localOnly || node) {
            this.stack.push({ node, offset: 0 })
          }
        }
        continue
      }

      if (k < v.keys.length) {
        const j = this.reverse ? v.keys.length - 1 - k : k
        const data = v.keys.get(j)

        if (this.config.localOnly && !data) {
          this.stack.push(top)
          continue
        }

        const c = this.reverse
          ? this.start
            ? b4a.compare(this.start, data.key)
            : -1
          : this.end
            ? b4a.compare(data.key, this.end)
            : -1

        if (c > this.compare) break

        if (this.prefetch && !v.children.length && this.stack.length) this.prefetchNext()

        this.stack.push(top)
        if (this.limit !== -1) this.limit--
        return data
      }
    }

    return null
  }

  prefetchNext() {
    // TODO: dbl check this for off-by-ones with the offset and keys and children
    let limit = this.limit

    // TODO: if limit === -1, don't return early here
    if (limit < this.tree.context.minKeys) return

    const parent = this.stack[this.stack.length - 1]
    const pv = parent.node.value
    if (!pv || pv === this.prefetching) return

    this.prefetching = pv

    for (let i = parent.offset >> 1; i < pv.children.length; i++) {
      // If the preceding key in parent is beyond upper bound,
      // stop fetching child nodes.
      if (i > 0) {
        const k = pv.keys.get(i - 1)

        const cmp = this.reverse
          ? this.start
            ? b4a.compare(this.start, k.key)
            : -1
          : this.end
            ? b4a.compare(k.key, this.end)
            : -1

        if (cmp > this.compare) return
      }

      const c = pv.children.get(i)
      if (!c.value) this.tree.inflate(c, this.config).catch(noop)

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

  _predestroy() {
    Hypercore.destroyRequests(this.iterator.config.activeRequests, null)
  }
}

exports.RangeIterator = RangeIterator
exports.RangeStream = RangeStream

function noop() {}
