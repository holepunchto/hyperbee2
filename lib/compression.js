const OP_SET = 0
const OP_INSERT = 1
const OP_DEL = 2
const OP_COHORT = 3

class DeltaOp {
  constructor(changed, type, index, pointer) {
    this.type = type
    this.index = index
    this.pointer = pointer
    this.changed = changed
  }
}

class DeltaCohort extends DeltaOp {
  constructor(changed, pointer, deltas) {
    super(changed, OP_COHORT, 0, pointer)
    this.deltas = deltas
  }
}

class CompressedArray {
  constructor(delta) {
    this.entries = []
    this.delta = delta

    for (const d of delta) {
      if (d.type === OP_COHORT) {
        for (const dd of d.deltas) {
          apply(this.entries, dd.type, dd.index, dd.pointer)
        }
      } else {
        apply(this.entries, d.type, d.index, d.pointer)
      }
    }
  }

  get length() {
    return this.entries.length
  }

  touch(index) {
    const pointer = this.entries[index]
    if (pointer.changedBy === this) return
    this.set(index, pointer)
  }

  get(index) {
    return this.entries[index]
  }

  push(pointer) {
    this.insert(this.entries.length, pointer)
  }

  unshift(pointer) {
    this.insert(0, pointer)
  }

  pop() {
    if (this.entries.length === 0) return
    const head = this.entries[this.entries.length - 1]
    this.delete(this.entries.length - 1)
    return head
  }

  shift() {
    if (this.entries.length === 0) return
    const tail = this.entries[0]
    this.delete(0)
    return tail
  }

  _touch(pointer) {
    if (pointer) pointer.changedBy = this
  }

  insert(index, pointer) {
    if (!insert(this.entries, index, pointer)) return
    this._touch(pointer)
    this.delta.push(new DeltaOp(true, OP_INSERT, index, pointer))
  }

  delete(index) {
    if (!del(this.entries, index)) return
    this._touch(null)
    this.delta.push(new DeltaOp(true, OP_DEL, index, null))
  }

  set(index, pointer) {
    if (!set(this.entries, index, pointer)) return
    this._touch(pointer)
    this.delta.push(new DeltaOp(true, OP_SET, index, pointer))
  }

  flush(max, min) {
    if (this.delta.length <= max) return this.delta

    const direct = []
    while (this.delta.length && this.delta[this.delta.length - 1].type !== OP_COHORT) {
      direct.push(this.delta.pop())
    }
    direct.reverse()

    if (direct.length > min && direct.length < this.entries.length) {
      const co = new DeltaCohort(true, null, [])
      for (const d of direct) {
        co.deltas.push(d)
      }
      this.delta.push(co)
    } else {
      const co = new DeltaCohort(true, null, [])
      for (let i = 0; i < this.entries.length; i++) {
        const d = new DeltaOp(true, OP_INSERT, i, this.entries[i])
        co.deltas.push(d)
      }
      this.delta = [co]
    }

    return this.delta
  }
}

exports.CompressedArray = CompressedArray
exports.DeltaOp = DeltaOp
exports.DeltaCohort = DeltaCohort

exports.OP_SET = OP_SET
exports.OP_INSERT = OP_INSERT
exports.OP_DEL = OP_DEL
exports.OP_COHORT = OP_COHORT

function del(entries, index) {
  if (index >= entries.length) return false
  entries.splice(index, 1)
  return true
}

function insert(entries, index, pointer) {
  if (index >= entries.length + 1) return false
  entries.splice(index, 0, pointer)
  return true
}

function set(entries, index, pointer) {
  if (index >= entries.length) return false
  // if (entries[index] === pointer) return false
  entries[index] = pointer
  return true
}

function apply(entries, type, index, pointer) {
  if (type === OP_INSERT) {
    return insert(entries, index, pointer)
  }
  if (type === OP_DEL) {
    return del(entries, index)
  }
  if (type === OP_SET) {
    return set(entries, index, pointer)
  }
  return false
}
