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
    this.uentries = null
    this.updates = 0

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

  get ulength() {
    return this.uentries ? this.uentries.length : this.entries.length
  }

  commit() {
    const c = new CompressedArray([])

    c.delta = this.delta
    c.entries = this.uentries || this.entries.slice(0)

    this.delta = this.delta.slice(0, this.updates)
    this.uentries = null
    this.updates = 0

    return c
  }

  _update() {
    if (this.uentries) return
    this.uentries = this.entries.slice(0)
  }

  touch(index, pointer) {
    if (pointer) this.entries[index] = pointer
    else pointer = this.entries[index]

    if (pointer.changedBy === this) return
    this.set(index, pointer)
  }

  get(index) {
    return this.entries[index]
  }

  uget(index) {
    return this.uentries ? this.uentries[index] : this.entries[index]
  }

  push(pointer) {
    if (!this.uentries) this._update()
    this.insert(this.uentries.length, pointer)
  }

  unshift(pointer) {
    this.insert(0, pointer)
  }

  pop() {
    if (!this.uentries) this._update()
    if (this.uentries.length === 0) return
    const head = this.uentries[this.uentries.length - 1]
    this.delete(this.uentries.length - 1)
    return head
  }

  shift() {
    if (!this.uentries) this._update()
    if (this.uentries.length === 0) return
    const tail = this.uentries[0]
    this.delete(0)
    return tail
  }

  _touch(pointer) {
    this.updates++
    if (pointer) pointer.changedBy = this
  }

  insert(index, pointer) {
    if (!this.uentries) this._update()
    if (!insert(this.uentries, index, pointer)) return
    this._touch(pointer)
    this.delta.push(new DeltaOp(true, OP_INSERT, index, pointer))
  }

  delete(index) {
    if (!this.uentries) this._update()
    if (!del(this.uentries, index)) return
    this._touch(null)
    this.delta.push(new DeltaOp(true, OP_DEL, index, null))
  }

  set(index, pointer) {
    if (!this.uentries) this._update()
    if (!set(this.uentries, index, pointer)) return
    this._touch(pointer)
    this.delta.push(new DeltaOp(true, OP_SET, index, pointer))
  }

  flush(max, min) {
    let overflow = false
    for (const d of this.delta) {
      if (d.index < 256) continue // has to be uint3, only happens in rebalances/splits
      overflow = true
      break
    }

    if (this.delta.length <= max && !overflow) return this.delta

    const direct = []
    while (this.delta.length && this.delta[this.delta.length - 1].type !== OP_COHORT) {
      direct.push(this.delta.pop())
    }
    direct.reverse()

    if (direct.length > min && direct.length < this.entries.length && !overflow) {
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
