module.exports = class NodeCache {
  constructor () {
    this.latest = null
    this.size = 0
  }

  oldest () {
    return this.latest ? this.latest.next : null
  }

  bump (node) {
    if (node === this.latest) return

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

  remove (node) {
    if (node.prev) {
      this.size--

      if (node === this.latest) {
        this.latest = node.next === node ? null : node.next
      }

      node.prev.next = node.next
      node.next.prev = node.prev
    }

    node.prev = node.next = null
  }

  * [Symbol.iterator] () {
    const node = this.latest
    if (node === null) return

    let next = node

    do {
      yield next
      next = next.next
    } while (node !== next)
  }
}
