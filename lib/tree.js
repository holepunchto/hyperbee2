const b4a = require('b4a')
const { CompressedArray } = require('./compression')

const T = 5
const MIN_KEYS = T - 1
const MAX_CHILDREN = MIN_KEYS * 2 + 1

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

class KeyPointer {
  constructor(context, core, seq, offset, changed, key, value, valuePointer) {
    this.context = context

    this.core = core
    this.seq = seq
    this.offset = offset
    this.changed = changed

    this.key = key
    this.value = value
    this.valuePointer = valuePointer
  }
}

class TreeNodePointer {
  constructor(context, core, seq, offset, changed, value) {
    this.context = context

    this.core = core
    this.seq = seq
    this.offset = offset
    this.changed = changed

    this.value = value

    this.next = null
    this.prev = null
  }
}

class TreeNode {
  constructor(keys, children) {
    this.keys = new CompressedArray(keys)
    this.children = new CompressedArray(children)
  }

  isEmpty() {
    return this.keys.entries.length === 0 && this.children.entries.length === 0
  }

  insert(context, key, value, valuePointer, child) {
    const keys = this.keys.entries

    let s = 0
    let e = keys.length
    let c = 0

    while (s < e) {
      const mid = (s + e) >> 1
      const k = keys[mid]

      c = b4a.compare(key, k.key)

      if (c === 0) return mid

      if (c < 0) e = mid
      else s = mid + 1
    }

    const i = c < 0 ? e : s
    this.keys.insert(i, new KeyPointer(context, 0, 0, 0, true, key, value, valuePointer))
    if (child) this.children.insert(i + 1, child)

    return keys.length < MAX_CHILDREN ? INSERTED : NEEDS_SPLIT
  }

  setValue(context, i, value) {
    this.keys.set(i, new KeyPointer(context, 0, 0, 0, true, this.keys[i].key, value, null))
  }

  removeKey(i) {
    this.keys.delete(i)
    if (this.children.entries.length) {
      this.children.delete(i + 1)
    }
  }

  siblings(parent) {
    const pc = parent.children.entries

    for (let i = 0; i < pc.length; i++) {
      if (pc[i].value !== this) continue // TODO: move to a seq/offset check instead

      const left = i ? pc[i - 1] : null
      const right = i < pc.length - 1 ? pc[i + 1] : null
      return { left, index: i, right }
    }

    // TODO: assert
    throw new Error('Bad parent')
  }

  merge(node, median) {
    const keys = node.keys.entries
    const children = node.children.entries

    this.keys.push(median)

    for (let i = 0; i < keys.length; i++) this.keys.push(keys[i])
    for (let i = 0; i < children.length; i++) this.children.push(children[i])
  }

  split(context) {
    const len = this.keys.entries.length >> 1
    const right = new TreeNodePointer(context, 0, 0, 0, true, new TreeNode([], []))

    const k = []
    while (k.length < len) k.push(this.keys.pop())
    for (let i = k.length - 1; i >= 0; i--) right.value.keys.push(k[i])

    const median = this.keys.pop()

    if (this.children.entries.length) {
      const c = []
      while (right.value.children.entries.length < len + 1) c.push(this.children.pop())
      for (let i = k.length - 1; i >= 0; i--) right.value.children.push(c[i])
    }

    return {
      left: this,
      median,
      right
    }
  }
}

exports.T = T
exports.MIN_KEYS = MIN_KEYS
exports.MAX_CHILDREN = MAX_CHILDREN

exports.INSERTED = INSERTED
exports.NEEDS_SPLIT = NEEDS_SPLIT

exports.TreeNodePointer = TreeNodePointer
exports.TreeNode = TreeNode
exports.KeyPointer = KeyPointer
exports.ValuePointer = ValuePointer

exports.EMPTY = new TreeNodePointer(null, 0, 0, 0, false, null)
