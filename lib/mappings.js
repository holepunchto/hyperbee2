const b4a = require('b4a')

class Mappings {
  constructor() {
    this.known = new Map()
    this.reverse = new Map()
    this.dirty = false
    this._flushed = null
  }

  resolve(key) {
    if (this.known.size === 0) return key
    const known = this.known.get(b4a.toString(key, 'hex'))
    return known === undefined ? key : known
  }

  unresolve(key) {
    if (this.reverse.size === 0) return key
    const from = this.reverse.get(b4a.toString(key, 'hex'))
    return from === undefined ? key : from
  }

  add(mappings, { persisted = false } = {}) {
    for (const { from, to } of mappings) {
      if (b4a.equals(from, to)) continue
      const hex = b4a.toString(from, 'hex')
      if (this.known.has(hex)) continue
      this.known.set(hex, to)
      this.reverse.set(b4a.toString(to, 'hex'), from)
      this._flushed = null
      if (!persisted) this.dirty = true
    }
  }

  flush() {
    this.dirty = false
    if (this.known.size === 0) return null

    if (this._flushed === null) {
      this._flushed = []
      for (const [hex, to] of this.known) this._flushed.push({ from: b4a.from(hex, 'hex'), to })
    }

    return this._flushed
  }
}

module.exports = Mappings
