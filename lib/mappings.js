const b4a = require('b4a')

// a mapping redirects every read of a core to another core holding the same
// blocks. Mappings are immutable facts, shared by every context of a tree and
// persisted next to the core table on the next checkpoint
class Mappings {
  constructor(mappings = null) {
    this.known = new Map()
    this.dirty = false
    if (mappings !== null) this.add(mappings)
  }

  // the mapping of a key once known, null until then
  get(key) {
    const known = this.known.get(b4a.toString(key, 'hex'))
    return known === undefined ? null : known
  }

  // the mapping of a key as far as it is known, the key itself otherwise
  resolve(key) {
    const known = this.get(key)
    return known === null ? key : known
  }

  // what is already in a checkpoint of our own core needs no writing
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
