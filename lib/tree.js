const b4a = require('b4a')

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
    this.keys = keys
    this.children = children
  }

  isEmpty() {
    return this.keys.length === 0 && this.children.length === 0
  }

  insert(context, key, value, valuePointer, child) {
    let s = 0
    let e = this.keys.length
    let c = 0

    while (s < e) {
      const mid = (s + e) >> 1
      const k = this.keys[mid]

      c = b4a.compare(key, k.key)

      if (c === 0) return mid

      if (c < 0) e = mid
      else s = mid + 1
    }

    const i = c < 0 ? e : s
    this.keys.splice(i, 0, new KeyPointer(context, 0, 0, 0, true, key, value, valuePointer))
    if (child) this.children.splice(i + 1, 0, child)

    return this.keys.length < MAX_CHILDREN ? INSERTED : NEEDS_SPLIT
  }

  setValue(context, i, value) {
    this.keys[i] = new KeyPointer(context, 0, 0, 0, true, this.keys[i].key, value, null)
  }

  removeKey(i) {
    this.keys.splice(i, 1)
    if (this.children.length) {
      this.children.splice(i + 1, 1)
    }
  }

  siblings(parent) {
    for (let i = 0; i < parent.children.length; i++) {
      if (parent.children[i].value !== this) continue // TODO: move to a seq/offset check instead

      const left = i ? parent.children[i - 1] : null
      const right = i < parent.children.length - 1 ? parent.children[i + 1] : null
      return { left, index: i, right }
    }

    // TODO: assert
    throw new Error('Bad parent')
  }

  merge(node, median) {
    this.keys.push(median)
    for (let i = 0; i < node.keys.length; i++) this.keys.push(node.keys[i])
    for (let i = 0; i < node.children.length; i++) this.children.push(node.children[i])
  }

  split(context) {
    const len = this.keys.length >> 1
    const right = new TreeNodePointer(context, 0, 0, 0, true, new TreeNode([], []))

    while (right.value.keys.length < len) right.value.keys.push(this.keys.pop())
    right.value.keys.reverse()

    const median = this.keys.pop()

    if (this.children.length) {
      while (right.value.children.length < len + 1) right.value.children.push(this.children.pop())
      right.value.children.reverse()
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
