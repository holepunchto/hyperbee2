const { Readable } = require('streamx')
const b4a = require('b4a')
const { RangeIterator } = require('./ranges.js')

class DiffIterator {
  constructor(left, right, { limit = -1 } = {}) {
    this.left = left
    this.right = right

    this.left.limit = this.right.limit = -1
    this.limit = limit

    this.nextLeft = null
    this.nextRight = null
  }

  async open() {
    await Promise.all([this.left.open(), this.right.open()])
  }

  _shiftRight() {
    const right = this.nextRight
    this.nextRight = null
    if (this.limit > 0) this.limit--
    return { left: null, right }
  }

  _shiftLeft() {
    const left = this.nextLeft
    this.nextLeft = null
    if (this.limit > 0) this.limit--
    return { left, right: null }
  }

  _fastForward() {
    while (this.left.stack.length && this.right.stack.length) {
      const a = this.left.stack[this.left.stack.length - 1]
      const b = this.right.stack[this.right.stack.length - 1]

      if (a.offset !== b.offset) return
      if (!isSame(a.node, b.node)) return

      this.left.stack.pop()
      this.right.stack.pop()
    }
  }

  async next() {
    while (this.limit === -1 || this.limit > 0) {
      const leftPromise = this.nextLeft ? null : this.left.next()
      const rightPromise = this.nextRight ? null : this.right.next()

      const [l, r] = await Promise.all([leftPromise, rightPromise])

      if (leftPromise) this.nextLeft = l
      if (rightPromise) this.nextRight = r

      if (!this.nextLeft && !this.nextRight) return null
      if (!this.nextLeft) return this._shiftRight()
      if (!this.nextRight) return this._shiftLeft()

      const cmp = b4a.compare(this.nextLeft.key, this.nextRight.key)

      if (cmp === 0) {
        const left = this.nextLeft
        const right = this.nextRight

        this.nextRight = this.nextLeft = null

        if (isSame(left, right)) {
          this._fastForward()
          continue
        }

        if (this.limit > 0) this.limit--
        return { left, right }
      }

      if (cmp < 0) return this._shiftLeft()
      return this._shiftRight()
    }

    return null
  }
}

class DiffStream extends Readable {
  constructor(left, right, options = {}) {
    const { highWaterMark } = options
    super({ eagerOpen: true, highWaterMark })

    this.left = left
    this.right = right
    this.iterator = new DiffIterator(
      new RangeIterator(left, options),
      new RangeIterator(right, options),
      options
    )
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

exports.DiffIterator = DiffIterator
exports.DiffStream = DiffStream

function isSame(a, b) {
  if (a.seq !== b.seq || a.offset !== b.offset) return false

  const k1 = a.context.getCoreKey(a.core)
  const k2 = b.context.getCoreKey(b.core)

  return k1 === k2 || b4a.equals(k1, k2) ? true : false
}
