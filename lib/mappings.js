const b4a = require('b4a')

// a mapping redirects every read of a core to another core holding the same
// blocks. Mappings are immutable facts, shared by every context of a tree and
// persisted next to the core table on the next checkpoint
class Mappings {
  constructor(hook = null) {
    this.hook = hook
    this.known = new Map()
    this.pending = new Map()
    this.dirty = false
  }

  // the mapping of a key as far as it is known, the key itself otherwise
  resolve(key) {
    const known = this.known.get(b4a.toString(key, 'hex'))
    return known === undefined ? key : known
  }

  // every core of a table is asked for before the table is used
  discover(cores) {
    const pending = []
    for (const { key } of cores) pending.push(this.map(key))
    return Promise.all(pending)
  }

  map(key) {
    const hex = b4a.toString(key, 'hex')

    const known = this.known.get(hex)
    if (known !== undefined) return Promise.resolve(known)
    if (this.hook === null) return Promise.resolve(key)

    const pending = this.pending.get(hex)
    if (pending !== undefined) return pending

    const promise = this._ask(key, hex)
    this.pending.set(hex, promise)
    return promise
  }

  async _ask(key, hex) {
    let to = null
    try {
      to = await this.hook(key)
    } finally {
      this.pending.delete(hex)
    }

    if (to === null || to === undefined) to = key

    // a record met while the hook was running wins over no mapping
    const known = this.known.get(hex)
    if (known !== undefined && b4a.equals(to, key)) return known

    if (!b4a.equals(to, key)) this.dirty = true
    this.known.set(hex, to)
    return to
  }

  add(mappings) {
    for (const { from, to } of mappings) {
      const hex = b4a.toString(from, 'hex')
      const known = this.known.get(hex)
      if (known !== undefined && !b4a.equals(known, from)) continue
      this.known.set(hex, to)
    }
  }

  flush() {
    this.dirty = false

    const mappings = []
    for (const [hex, to] of this.known) {
      const from = b4a.from(hex, 'hex')
      if (!b4a.equals(from, to)) mappings.push({ from, to })
    }

    return mappings.length === 0 ? null : mappings
  }
}

module.exports = Mappings
