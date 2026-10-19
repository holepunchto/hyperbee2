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
    while (this.size > 0) {
      const old = this.oldest()
      this.remove(old)
      old.value = null
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
      const old = this.oldest()
      if (old.retained > this.retained) break
      this.remove(old)
      old.value = null
    }
  }

  bump(node) {
    if (node === this.latest) return

    if (node.prev) this._unlink(node)
    else this.byId.set(cacheId(node), node)

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
    if (node.prev) this._unlink(node)

    const id = cacheId(node)
    if (this.byId.get(id) === node) {
      this.byId.delete(id)
    }
  }

  _unlink(node) {
    this.size--

    if (node === this.latest) {
      this.latest = node.next === node ? null : node.next
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

// nodes are cached under the core they were read from, so a block reached
// through a mapped core and through the core it maps to share an entry. A
// pointer whose core is not open yet can only miss
function cacheId(node) {
  const core = node.context.openedCore(node.core)
  const id = core !== null && core.key !== null ? core.id : node.context.id + ':' + node.core
  return id + '@' + node.seq + '.' + node.offset
}
