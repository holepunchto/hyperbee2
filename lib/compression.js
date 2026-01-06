const OP_SET = 0
const OP_INSERT = 1
const OP_DEL = 2
const OP_COHORT = 3

class Delta {
  constructor (changed, type) {
    this.type = type
    this.changed = changed
  }
}

class DeltaOp extends Delta {
  constructor (changed, type, index, entry) {
    super(changed, type)
    this.index = index
    this.entry = entry
  }
}

class DeltaCohort extends Delta {
  constructor (changed, deltas) {
    super(changed, OP_COHORT)
    this.deltas = deltas
  }
}

class CompressedArray {
  constructor (delta) {
    this.entries = []
    this.delta = delta

    for (const d of delta) {
      if (d.type === OP_COHORT) {
        for (const dd of d.deltas) {
          this.apply(dd.type, dd.index, dd.entry)
        }
      } else {
        this.apply(d.type, d.index, d.entry)
      }
    }
  }

  apply(type, index, entry) {
    if (type === OP_INSERT) {
      return this._insert(index, entry)
    }
    if (type === OP_DEL) {
      return this._delete(index)
    }
    if (type === OP_SET) {
      return this._set(index, entry)
    }
    return false
  }

  insert(index, entry) {
    if (!this._insert(index, entry)) return
    this.delta.push(new DeltaOp(OP_INSERT, index, entry))
  }

  delete(index) {
    if (!this._delete(index)) return
    this.delta.push(new DeltaOp(OP_DEL, index, null))
  }

  set(index, entry) {
    if (!this._set(index, entry)) return
    this.delta.push(new DeltaOp(OP_SET, index, entry))
  }

  flush(max, min) {
    if (this.delta.length <= max) return this.delta

    const direct = []
    while (this.delta.length && this.delta[this.delta.length - 1].type !== OP_COHORT) {
      direct.push(this.delta.pop())
    }
    direct.reverse()

    if (direct.length <= min) {
      const co = new DeltaCohort(true, [])
      for (const d of direct) {
        co.deltas.push(d)
      }
      this.delta.push(co)
    }

    return this.delta
  }

  _delete(index) {
    if (index >= this.entries.length) return false
    this.entries.splice(index, 1)
    return true
  }

  _insert(index, entry) {
    if (index >= this.entries.length + 1) return false
    this.entries.splice(index, 0, entry)
    return true
  }

  _set(index, entry) {
    if (index >= this.entries.length) return false
    this.entries[index] = entry
    return true
  }
}
