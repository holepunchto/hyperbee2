const b4a = require('b4a')
const { CompressedArray } = require('./compression.js')

const T = 128 // default degree, only non default degrees are stored on the nodes

const INSERTED = -1
const NEEDS_SPLIT = -2

class ValuePointer {
  constructor(context, core, seq, offset, split) {
    this.context = context

    this.core = core
    this.seq = seq
    this.offset = offset
    this.split = split
  }
}

class Pointer {
  constructor(context, core, seq, offset, changed) {
    this.context = context

    this.core = core
    this.seq = seq
    this.offset = offset
    this.changed = changed
    this.retained = 0

    this.changedBy = null
  }

  mark() {
    this.changed = true
  }

  unmark() {
    this.changed = false
    this.changedBy = null
  }

  // Compare two pointers to see if they point to equivalent positions
  equivalentTo(other) {
    // EMPTY is a special case that can look equivalent to a first entry
    // in a hypercore but actually contains different data.
    if (other === exports.EMPTY) return this === exports.EMPTY
    if (!other) return false

    return (
      this.seq === other.seq &&
      this.offset === other.offset &&
      this.changed === other.changed &&
      this.context === other.context &&
      this.core === other.core
    )
  }

  retain() {
    this.retained = this.context.cache.retained + 1
  }
}

class KeyPointer extends Pointer {
  constructor(context, core, seq, offset, changed, key, value, valuePointer) {
    super(context, core, seq, offset, changed)

    this.key = key
    this.value = value
    this.valuePointer = valuePointer
  }

  // TODO: remove, left here for easier debugging for now
  [Symbol.for('nodejs.util.inspect.custom')]() {
    return `[KeyPointer core=${this.core} seq=${this.seq}, offset=${this.offset}, key="${b4a.toString(this.key)}"]`
  }
}

class TreeNodePointer extends Pointer {
  constructor(context, core, seq, offset, changed, value) {
    super(context, core, seq, offset, false)

    this.value = value

    this.next = null
    this.prev = null

    this.inflating = null

    if (changed) this.mark()
  }

  mark() {
    if (this.changed) return
    this.changed = true
    this.dirty = this.context.cache.dirty.push(this) - 1
  }

  unmark() {
    if (!this.changed) return

    this.changed = false
    this.changedBy = null

    if (this.value) {
      this.value.keys.reset()
      this.value.children.reset()
    }

    const head = this.context.cache.dirty.pop()
    if (head === this) return

    this.context.cache.dirty[this.dirty] = head
    head.dirty = this.dirty
  }

  commit() {
    const value = new TreeNode(this.value.t, [], [])
    value.keys = this.value.keys.commit()
    value.children = this.value.children.commit()
    const ptr = this.context.createTreeNode(this.core, 0, 0, true, value)
    this.unmark()
    return ptr
  }

  equivalentTo(other) {
    return (
      super.equivalentTo(other) &&
      this.value === other.value &&
      this.prev === other.prev &&
      this.next === other.next
    )
  }

  // TODO: remove, left here for easier debugging for now
  [Symbol.for('nodejs.util.inspect.custom')]() {
    return `[TreeNodePointer core=${this.core} seq=${this.seq} offset=${this.offset} changed=${this.changed}]`
  }
}

class TreeNode {
  constructor(t, keys, children) {
    this.t = t
    this.keys = new CompressedArray(keys)
    this.children = new CompressedArray(children)
  }

  isEmpty() {
    return this.keys.ulength === 0 && this.children.ulength === 0
  }

  insertLeaf(context, key, value) {
    let s = 0
    let e = this.keys.ulength
    let c = 0

    while (s < e) {
      const mid = (s + e) >> 1
      const k = this.keys.uget(mid)

      c = b4a.compare(key, k.key)

      if (c === 0) return mid

      if (c < 0) e = mid
      else s = mid + 1
    }

    const i = c < 0 ? e : s
    const maxKeys = 2 * this.t - 1

    this.keys.insert(i, new KeyPointer(context, 0, 0, 0, true, key, value, null))

    return this.keys.ulength <= maxKeys ? INSERTED : NEEDS_SPLIT
  }

  insertNode(keyPointer, treePointer) {
    let s = 0
    let e = this.keys.ulength
    let c = 0

    while (s < e) {
      const mid = (s + e) >> 1
      const k = this.keys.uget(mid)

      c = b4a.compare(keyPointer.key, k.key)

      if (c === 0) return mid

      if (c < 0) e = mid
      else s = mid + 1
    }

    const i = c < 0 ? e : s
    const maxKeys = 2 * this.t - 1

    this.keys.insert(i, keyPointer)
    this.children.insert(i + 1, treePointer)

    return this.keys.ulength <= maxKeys ? INSERTED : NEEDS_SPLIT
  }

  setValue(context, i, value) {
    this.keys.set(i, new KeyPointer(context, 0, 0, 0, true, this.keys.uget(i).key, value, null))
  }

  removeKey(i) {
    this.keys.delete(i)
    if (this.children.ulength) {
      this.children.delete(i + 1)
    }
  }

  siblings(parent) {
    const pc = parent.children

    for (let i = 0; i < pc.ulength; i++) {
      if (pc.uget(i).value !== this) continue // TODO: move to a seq/offset check instead

      const left = i ? pc.uget(i - 1) : null
      const right = i < pc.ulength - 1 ? pc.uget(i + 1) : null
      return { left, index: i, right }
    }

    // TODO: assert
    throw new Error('Bad parent')
  }

  merge(node, median) {
    const keys = node.keys
    const children = node.children

    this.keys.push(median)

    for (let i = 0; i < keys.ulength; i++) this.keys.push(keys.uget(i))
    for (let i = 0; i < children.ulength; i++) this.children.push(children.uget(i))
  }

  split(context) {
    const len = this.keys.ulength >> 1
    const right = context.createTreeNode(0, 0, 0, true, new TreeNode(this.t, [], []))

    const k = []
    while (k.length < len) k.push(this.keys.pop())
    for (let i = k.length - 1; i >= 0; i--) right.value.keys.push(k[i])

    const median = this.keys.pop()

    if (this.children.ulength) {
      const c = []
      while (c.length < len + 1) c.push(this.children.pop())
      for (let i = c.length - 1; i >= 0; i--) right.value.children.push(c[i])
    }

    return {
      left: this,
      median,
      right
    }
  }
}

exports.T = T
exports.INSERTED = INSERTED
exports.NEEDS_SPLIT = NEEDS_SPLIT

exports.TreeNodePointer = TreeNodePointer
exports.TreeNode = TreeNode
exports.KeyPointer = KeyPointer
exports.ValuePointer = ValuePointer
exports.Pointer = Pointer

exports.EMPTY = new TreeNodePointer(null, 0, 0, 0, false, null)
