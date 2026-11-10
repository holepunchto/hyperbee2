module.exports = class NodeCache {
  constructor(maxSize) {
    this.size = 0
    this.maxSize = maxSize
    this.latest = null
    this.retained = 0
    this.byId = new Map()
    this.dirty = []
  }

  oldest() {
    return this.latest ? this.latest.next : null
  }

  empty() {
    let node = this.oldest()
    while (node !== null) {
      const next = node === this.latest ? null : node.next
      this.remove(node)
      node = next
    }
  }

  flush() {
    while (this.dirty.length > 0) {
      this.dirty[this.dirty.length - 1].unmark()
    }

    this.retained++
    this.gc()
  }

  gc() {
    while (this.size > this.maxSize) {
      if (!this.remove(this.oldest())) break
    }
  }

  bump(node) {
    if (node === this.latest) return

    if (node.prev) {
      this._unlink(node)
    } else {
      const id = cacheId(node)
      const existing = this.byId.get(id)
      // one entry per id, the newer pointer wins unless the old one is pinned by a write
      if (existing !== undefined && !this._remove(existing, null)) return
      this.byId.set(id, node)
    }

    this.size++

    if (!this.latest) {
      node.prev = node.next = node
      this.latest = node
    } else {
      node.prev = this.latest
      node.next = this.latest.next
      this.latest.next.prev = node
      this.latest.next = node
      this.latest = node
    }
  }

  get(node) {
    return this.byId.get(cacheId(node)) || null
  }

  remove(node) {
    return this._remove(node, cacheId(node))
  }

  _remove(node, id) {
    if (node.retained > this.retained) return false
    if (node.prev) this._unlink(node)
    if (id !== null && this.byId.get(id) === node) this.byId.delete(id)
    node.value = null
    return true
  }

  _unlink(node) {
    this.size--

    if (node === this.latest) {
      this.latest = node.next === node ? null : node.prev
    }

    node.prev.next = node.next
    node.next.prev = node.prev
    node.prev = node.next = null
  }

  *[Symbol.iterator]() {
    const node = this.latest
    if (node === null) return

    let next = node

    do {
      yield next
      next = next.next
    } while (node !== next)
  }
}

function cacheId(node) {
  const id = node.context.getCore(node.core).id
  return id + '@' + node.seq + '.' + node.offset
}
