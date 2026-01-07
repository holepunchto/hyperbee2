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
  constructor (changed, type, index, pointer) {
    super(changed, type)
    this.index = index
    this.pointer = pointer
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
          this.apply(dd.type, dd.index, dd.pointer)
        }
      } else {
        this.apply(d.type, d.index, d.pointer)
      }
    }
  }

  apply(type, index, pointer) {
    if (type === OP_INSERT) {
      return this._insert(index, pointer)
    }
    if (type === OP_DEL) {
      return this._delete(index)
    }
    if (type === OP_SET) {
      return this._set(index, pointer)
    }
    return false
  }

  push(pointer) {
    this.insert(this.entries.length, pointer)
  }

  pop() {
    if (this.entries.length === 0) return null
    const head = this.entries[this.entries.length - 1]
    this.delete(this.entries.length - 1)
    return head
  }

  insert(index, pointer) {
    if (!this._insert(index, pointer)) return
    this.delta.push(new DeltaOp(true, OP_INSERT, index, pointer))
  }

  delete(index) {
    if (!this._delete(index)) return
    this.delta.push(new DeltaOp(true, OP_DEL, index, null))
  }

  set(index, pointer) {
    if (!this._set(index, pointer)) return
    this.delta.push(new DeltaOp(true, OP_SET, index, pointer))
  }

  flush(max, min) {
    if (this.delta.length <= max) return this.delta

    const direct = []
    while (this.delta.length && this.delta[this.delta.length - 1].type !== OP_COHORT) {
      direct.push(this.delta.pop())
    }
    direct.reverse()

    if (direct.length > min) {
      const co = new DeltaCohort(true, [])
      for (const d of direct) {
        co.deltas.push(d)
      }
      this.delta.push(co)
    } else {
      const co = new DeltaCohort(true, [])
      for (let i = 0; i < this.entries.length; i++) {
        const d = new DeltaOp(true, OP_INSERT, i, this.entries[i])
        co.deltas.push(d)
      }
      this.delta = [co]
    }

    return this.delta
  }

  _delete(index) {
    if (index >= this.entries.length) return false
    this.entries.splice(index, 1)
    return true
  }

  _insert(index, pointer) {
    if (index >= this.entries.length + 1) return false
    this.entries.splice(index, 0, pointer)
    return true
  }

  _set(index, pointer) {
    if (index >= this.entries.length) return false
    this.entries[index] = pointer
    return true
  }
}

exports.CompressedArray = CompressedArray
exports.Delta = Delta
exports.DeltaOp = DeltaOp
exports.DeltaCohort = DeltaCohort

exports.OP_SET = OP_SET
exports.OP_INSERT = OP_INSERT
exports.OP_DEL = OP_DEL
exports.OP_COHORT = OP_COHORT

// const a = new CompressedArray([])

// const max = 2
// const min = 1

// a.insert(0, { value: 1 })
// console.log(a.flush(max, min))
// a.insert(0, { value: 0 })
// console.log(a.flush(max, min))
// a.insert(2, { value: 2 })
// console.log(a.flush(max, min))
// a.insert(3, { value: 3 })
// console.log(a.flush(max, min))
// a.insert(4, { value: 4 })
// console.log(a.flush(max, min))
// a.insert(5, { value: 5 })
// console.log(a.flush(max, min))
// a.insert(6, { value: 6 })
// console.log(a.flush(max, min))
