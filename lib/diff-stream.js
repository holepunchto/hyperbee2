const { Readable } = require('streamx')
const b4a = require('b4a')

module.exports = class DiffStream extends Readable {
  constructor(left, right, options = {}) {
    super({ eagerOpen: true })

    this.left = new TreeCursor(left, options)
    this.right = new TreeCursor(right, options)

    this._limit = options.limit === undefined ? -1 : options.limit
  }

  async _openp() {
    await Promise.all([this.left.open(), this.right.open()])
  }

  async _readp() {
    if (this._limit === 0) {
      this.push(null)
      return
    }

    const result = await this._next()

    if (!result) {
      this.push(null)
      return
    }

    if (this._limit > 0) this._limit--
    this.push(result)
  }

  async _next() {
    const a = this.left
    const b = this.right

    while (true) {
      const [l, r] = await Promise.all([a.peek(), b.peek()])

      if (!l && !r) return null

      if (!l) {
        const right = await b.nextKey()
        if (right === null) return null
        return { left: null, right }
      }

      if (!r) {
        const left = await a.nextKey()
        if (left === null) return null
        return { left, right: null }
      }

      if (samePosition(l, r)) {
        a.skip()
        b.skip()
        continue
      }

      if (l.isKey && !r.isKey) {
        await b.descend()
        continue
      }

      if (!l.isKey && r.isKey) {
        await a.descend()
        continue
      }

      if (l.isKey && r.isKey) {
        const c = cmp(l.key, r.key)

        if (c === 0) {
          const left = await a.nextKey()
          const right = await b.nextKey()
          if (left === null && right === null) continue
          return { left, right }
        }

        if (c < 0) {
          const left = await a.nextKey()
          if (left === null) continue
          return { left, right: null }
        }

        const right = await b.nextKey()
        if (right === null) continue
        return { left: null, right }
      }

      const c = cmp(l.key, r.key)
      if (c === 0) {
        await Promise.all([a.descend(), b.descend()])
      } else if (c < 0) {
        await b.descend()
      } else {
        await a.descend()
      }
    }
  }

  async _open(cb) {
    try {
      await this._openp()
    } catch (err) {
      cb(err)
      return
    }
    cb(null)
  }

  async _read(cb) {
    try {
      await this._readp()
    } catch (err) {
      cb(err)
      return
    }
    cb(null)
  }
}

class TreeCursor {
  constructor(tree, options = {}) {
    this.tree = tree
    this.root = null
    this.stack = []

    this._activeRequests = options.activeRequests || tree.activeRequests
    this._start = options.gte || options.gt || null
    this._end = options.lte || options.lt || null
    this._startInclusive = !!options.gte
    this._endInclusive = !!options.lte
  }

  async open() {
    this.root = await this.tree.bootstrap(this._activeRequests)
    if (!this.root) return

    this.stack.push({ node: this.root, offset: 0, value: null })

    if (!this._start) return

    await this._seek()
  }

  async _seek() {
    const offset = this._startInclusive ? 0 : 1

    while (this.stack.length) {
      const top = this.stack[this.stack.length - 1]
      const v = await this._inflate(top)

      for (let i = 0; i < v.keys.length; i++) {
        const c = b4a.compare(this._start, v.keys[i].key)

        if (c < 0) break
        top.offset = 2 * i + 1 + (c === 0 ? offset : 1)
      }

      const child = (top.offset & 1) === 0
      const k = top.offset >> 1

      if (!child || k >= v.children.length) break

      this.stack.push({
        node: v.children[k],
        offset: 0,
        value: null
      })

      top.offset++
    }
  }

  async _inflate(entry) {
    if (entry.value) {
      this.tree.bump(entry.node)
      return entry.value
    }

    entry.value = await this.tree.inflate(entry.node, this._activeRequests)
    return entry.value
  }

  async peek() {
    while (this.stack.length) {
      const top = this.stack[this.stack.length - 1]
      const v = await this._inflate(top)

      // For leaf nodes (no children), skip child positions by advancing to key position
      if (v.children.length === 0 && (top.offset & 1) === 0) {
        top.offset |= 1
      }

      const isKey = (top.offset & 1) === 1
      const index = top.offset >> 1

      if (isKey) {
        if (index >= v.keys.length) {
          this.stack.pop()
          continue
        }
        const k = v.keys[index]
        return { isKey: true, coreKey: coreKey(k), seq: k.seq, offset: k.offset, key: k.key }
      }

      if (index >= v.children.length) {
        this.stack.pop()
        continue
      }

      const c = v.children[index]
      const key = index < v.keys.length ? v.keys[index].key : await this._parentKey()
      return { isKey: false, coreKey: coreKey(c), seq: c.seq, offset: c.offset, key }
    }

    return null
  }

  skip() {
    if (!this.stack.length) return

    const top = this.stack[this.stack.length - 1]
    top.offset++

    if (!top.value) {
      this.stack.pop()
      return
    }

    const v = top.value
    const maxOffset = v.children.length ? v.keys.length * 2 + 1 : v.keys.length * 2
    if (top.offset >= maxOffset) this.stack.pop()
  }

  async descend() {
    if (!this.stack.length) return

    const top = this.stack[this.stack.length - 1]
    const v = await this._inflate(top)

    const isKey = (top.offset & 1) === 1
    if (isKey) return

    const index = top.offset >> 1
    if (index >= v.children.length) return

    top.offset++

    this.stack.push({
      node: v.children[index],
      offset: 0,
      value: null
    })
  }

  async nextKey() {
    while (this.stack.length) {
      const top = this.stack[this.stack.length - 1]
      const v = await this._inflate(top)

      const offset = top.offset++
      const isKey = (offset & 1) === 1
      const index = offset >> 1

      const maxOffset = v.children.length ? v.keys.length * 2 + 1 : v.keys.length * 2
      if (top.offset >= maxOffset) this.stack.pop()

      if (!isKey) {
        if (index < v.children.length) {
          this.stack.push({
            node: v.children[index],
            offset: 0,
            value: null
          })
        }
        continue
      }

      if (index >= v.keys.length) continue

      const data = v.keys[index]

      if (this._end) {
        const c = b4a.compare(data.key, this._end)
        if (this._endInclusive ? c > 0 : c >= 0) {
          this.stack = []
          return null
        }
      }

      return data
    }

    return null
  }

  async _parentKey() {
    for (let i = this.stack.length - 2; i >= 0; i--) {
      const entry = this.stack[i]
      const v = await this._inflate(entry)

      const index = (entry.offset - 1) >> 1
      if (index < v.keys.length) {
        return v.keys[index].key
      }
    }
    return null
  }
}

function samePosition(a, b) {
  if (!a || !b) return false
  if (a.isKey !== b.isKey) return false
  return b4a.equals(a.coreKey, b.coreKey) && a.seq === b.seq && a.offset === b.offset
}

function coreKey(ptr) {
  return ptr.core === 0 ? ptr.context.core.key : ptr.context.cores[ptr.core - 1].key
}

function cmp(a, b) {
  if (!a) return b ? 1 : 0
  if (!b) return a ? -1 : 0
  return b4a.compare(a, b)
}
