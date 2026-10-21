const b4a = require('b4a')

class Mappings {
  constructor() {
    this.known = new Map()
    this.dirty = false
  }

  resolve(key) {
    const known = this.known.get(b4a.toString(key, 'hex'))
    return known === undefined ? key : known
  }

  add(mappings, { persisted = false } = {}) {
    for (const { from, to } of mappings) {
      if (b4a.equals(from, to)) continue
      const hex = b4a.toString(from, 'hex')
      if (this.known.has(hex)) continue
      this.known.set(hex, to)
      if (!persisted) this.dirty = true
    }
  }

  flush() {
    this.dirty = false

    const mappings = []
    for (const [hex, to] of this.known) mappings.push({ from: b4a.from(hex, 'hex'), to })

    return mappings.length === 0 ? null : mappings
  }
}

module.exports = Mappings
