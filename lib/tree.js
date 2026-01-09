const b4a = require('b4a')
const { CompressedArray } = require('./compression.js')

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

    this.changedBy = null
  }
}

class KeyPointer extends Pointer {
  constructor(context, core, seq, offset, changed, key, value, valuePointer) {
    super(context, core, seq, offset, changed)

    this.key = key
    this.value = value
    this.valuePointer = valuePointer
  }

  [Symbol.for('nodejs.util.inspect.custom')]() {
    return (
      '[KeyPointer ' +
      this.core +
      ' ' +
      this.seq +
      ' ' +
      this.offset +
      ' "' +
      this.key.toString() +
      '"]'
    )
  }
}

class TreeNodePointer extends Pointer {
  constructor(context, core, seq, offset, changed, value) {
    super(context, core, seq, offset, changed)

    this.value = value

    this.next = null
    this.prev = null
  }

  [Symbol.for('nodejs.util.inspect.custom')]() {
    return (
      '[TreeNodePointer ' +
      this.core +
      ' ' +
      this.seq +
      ' ' +
      this.offset +
      ' ' +
      this.changed +
      ']'
    )
  }
}

class TreeNode {
  constructor(keys, children) {
    this.keys = new CompressedArray(keys)
    this.children = new CompressedArray(children)
  }

  isEmpty() {
    return this.keys.length === 0 && this.children.length === 0
  }

  insertLeaf(context, key, value) {
    let s = 0
    let e = this.keys.length
    let c = 0

    while (s < e) {
      const mid = (s + e) >> 1
      const k = this.keys.get(mid)

      c = b4a.compare(key, k.key)

      if (c === 0) return mid

      if (c < 0) e = mid
      else s = mid + 1
    }

    const i = c < 0 ? e : s
    this.keys.insert(i, new KeyPointer(context, 0, 0, 0, true, key, value, null))

    return this.keys.length <= context.maxKeys ? INSERTED : NEEDS_SPLIT
  }

  insertNode(context, keyPointer, treePointer) {
    let s = 0
    let e = this.keys.length
    let c = 0

    while (s < e) {
      const mid = (s + e) >> 1
      const k = this.keys.get(mid)

      c = b4a.compare(keyPointer.key, k.key)

      if (c === 0) return mid

      if (c < 0) e = mid
      else s = mid + 1
    }

    const i = c < 0 ? e : s
    this.keys.insert(i, keyPointer)
    this.children.insert(i + 1, treePointer)

    return this.keys.length <= context.maxKeys ? INSERTED : NEEDS_SPLIT
  }

  setValue(context, i, value) {
    this.keys.set(i, new KeyPointer(context, 0, 0, 0, true, this.keys.get(i).key, value, null))
  }

  removeKey(i) {
    this.keys.delete(i)
    if (this.children.length) {
      this.children.delete(i + 1)
    }
  }

  siblings(parent) {
    const pc = parent.children

    for (let i = 0; i < pc.length; i++) {
      if (pc.get(i).value !== this) continue // TODO: move to a seq/offset check instead

      const left = i ? pc.get(i - 1) : null
      const right = i < pc.length - 1 ? pc.get(i + 1) : null
      return { left, index: i, right }
    }

    // TODO: assert
    throw new Error('Bad parent')
  }

  merge(node, median) {
    const keys = node.keys
    const children = node.children

    this.keys.push(median)

    for (let i = 0; i < keys.length; i++) this.keys.push(keys.get(i))
    for (let i = 0; i < children.length; i++) this.children.push(children.get(i))
  }

  split(context) {
    const len = this.keys.length >> 1
    const right = new TreeNodePointer(context, 0, 0, 0, true, new TreeNode([], []))

    const k = []
    while (k.length < len) k.push(this.keys.pop())
    for (let i = k.length - 1; i >= 0; i--) right.value.keys.push(k[i])

    const median = this.keys.pop()

    if (this.children.length) {
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

exports.INSERTED = INSERTED
exports.NEEDS_SPLIT = NEEDS_SPLIT

exports.TreeNodePointer = TreeNodePointer
exports.TreeNode = TreeNode
exports.KeyPointer = KeyPointer
exports.ValuePointer = ValuePointer
exports.Pointer = Pointer

exports.EMPTY = new TreeNodePointer(null, 0, 0, 0, false, null)
