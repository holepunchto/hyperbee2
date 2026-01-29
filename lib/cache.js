module.exports = class NodeCache {
  constructor(maxSize) {
    this.size = 0
    this.maxSize = maxSize
    this.latest = null
    this.retained = 0
    this.byId = new Map()
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

    if (node.next === null && node.prev === null) {
      this.byId.set(cacheId(node), node)
    }

    if (node.prev) this.remove(node)
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
    if (node.prev) {
      this.size--

      if (node === this.latest) {
        this.latest = node.next === node ? null : node.next
      }

      node.prev.next = node.next
      node.next.prev = node.prev
    }

    node.prev = node.next = null

    const id = cacheId(node)
    if (this.byId.get(id) === node) {
      this.byId.delete(id)
    }
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
