const b4a = require('b4a')
const { Pointer, KeyPointer, ValuePointer, TreeNode } = require('./tree.js')
const { DeltaOp, DeltaCohort, OP_COHORT } = require('./compression.js')
const { decodeBlock } = require('./encoding.js')

// Every caller runs the inflation IO under its own config, so timeout, wait and
// cancellation are per caller and hypercore dedups the fetches. Decoded blocks
// land in slots on ptr.inflating shared by all callers, filled by whoever wakes first.

exports.inflate = async function inflate(ptr, config) {
  if (ptr.value) return ptr.value
  if (ptr.inflating === null) ptr.inflating = new Inflation(ptr)

  const value = await ptr.inflating.run(config)

  if (!ptr.value) {
    ptr.value = value
    ptr.inflating = null
  }

  return ptr.value
}

exports.inflateValue = async function inflateValue(key, config) {
  if (key.value) return key.value
  if (!key.valuePointer) return null

  const ptr = key.valuePointer

  if (ptr.split === 0) {
    const block = await ptr.context.getBlock(ptr.seq, ptr.core, config, null)
    return block.values[ptr.offset]
  }

  const blockPromises = new Array(ptr.split + 1)
  for (let i = 0; i < blockPromises.length; i++) {
    blockPromises[i] = ptr.context.getBlock(ptr.seq - ptr.split + i, ptr.core, config, null)
  }
  const blocks = await Promise.all(blockPromises)
  const splitValue = new Array(blockPromises.length)
  for (let i = 0; i < splitValue.length - 1; i++) {
    splitValue[i] = blocks[i].values[0]
  }
  splitValue[splitValue.length - 1] = blocks[blocks.length - 1].buffer[ptr.offset]
  return b4a.concat(splitValue)
}

class Inflation {
  constructor(ptr) {
    this.ptr = ptr
    this.block = null
    this.keys = null // per delta key: its block, or { block, entries } for a cohort
    this.children = null // per delta child: { block, entries } for a cohort, else unused
  }

  async run(config) {
    const ptr = this.ptr

    // round 0, the ptr block
    if (this.block === null) {
      const buffer = await ptr.context.getBlockBuffer(ptr.seq, ptr.core, config, null)
      if (this.block === null) this.block = decodeBlock(buffer, ptr.seq)
      const update = ptr.context.updateCheckpointMaybe(this.block, ptr.core, config)
      if (update !== null) await update
    }

    if (ptr.value) return ptr.value

    const block = this.block
    const context = ptr.context.getLoadedContext(ptr.core)
    const tree = block.tree[ptr.offset]

    if (this.keys === null) {
      this.keys = new Array(tree.keys.length)
      this.children = new Array(tree.children.length)
    }

    // round 1, the blocks the keys point at, the cohorts (round 2 inside) and
    // the cores the children live in
    const promises = []

    for (let i = 0; i < tree.keys.length; i++) {
      const d = tree.keys[i]
      if (d.type === OP_COHORT) {
        promises.push(this._cohort(this.keys, i, context, d, ptr, block, config, true))
      } else if (d.pointer && this.keys[i] === undefined) {
        if (sameBlock(d.pointer, ptr)) this.keys[i] = block
        else promises.push(fillBlock(this.keys, i, context, d.pointer, block, config))
      }
    }

    for (let i = 0; i < tree.children.length; i++) {
      const d = tree.children[i]
      if (d.type === OP_COHORT) {
        promises.push(this._cohort(this.children, i, context, d, ptr, block, config, false))
      } else if (needsCore(context, d)) {
        promises.push(fillCore(context, d, block, config))
      }
    }

    await Promise.all(promises)

    if (ptr.value) return ptr.value

    // done, build the node from the slots
    const keys = new Array(tree.keys.length)
    const children = new Array(tree.children.length)

    for (let i = 0; i < keys.length; i++) {
      const d = tree.keys[i]
      if (d.type === OP_COHORT) keys[i] = keyCohort(context, d, this.keys[i])
      else keys[i] = key(context, d, this.keys[i])
    }

    for (let i = 0; i < children.length; i++) {
      const d = tree.children[i]
      if (d.type === OP_COHORT) children[i] = childCohort(context, d, this.children[i])
      else children[i] = child(context, d)
    }

    return new TreeNode(block.t, keys, children)
  }

  async _cohort(slots, i, context, d, ptr, block, config, isKey) {
    const co = d.pointer

    // round 1, the cohort block
    if (slots[i] === undefined) {
      if (sameBlock(co, ptr)) slots[i] = { block, entries: null }
      else await fillCohortBlock(slots, i, context, co, block, config)
    }

    const slot = slots[i]
    const blk = slot.block
    const ctx = context.getLoadedContext(co.core)
    const cohort = blk.cohorts[co.offset]

    if (slot.entries === null) slot.entries = new Array(cohort.length)

    // round 2, the blocks the cohort keys point at or the cores its children live in
    const promises = []

    for (let j = 0; j < cohort.length; j++) {
      const p = cohort[j]
      if (isKey) {
        if (p.pointer && slot.entries[j] === undefined) {
          if (sameBlock(p.pointer, co)) slot.entries[j] = blk
          else promises.push(fillBlock(slot.entries, j, ctx, p.pointer, blk, config))
        }
      } else if (needsCore(ctx, p)) {
        promises.push(fillCore(ctx, p, blk, config))
      }
    }

    await Promise.all(promises)
  }
}

// IO, run by every caller. Slots are filled once.

function sameBlock(p, ptr) {
  return p.seq === ptr.seq && p.core === 0 && ptr.core === 0
}

async function fillBlock(slots, i, context, p, origin, config) {
  const buffer = await context.getBlockBuffer(p.seq, p.core, config, origin)
  // callers woken by the same block decode it once
  if (slots[i] === undefined) slots[i] = decodeBlock(buffer, p.seq)
  const update = context.updateCheckpointMaybe(slots[i], p.core, config)
  if (update !== null) await update
}

async function fillCohortBlock(slots, i, context, co, origin, config) {
  const buffer = await context.getBlockBuffer(co.seq, co.core, config, origin)
  if (slots[i] === undefined) slots[i] = { block: decodeBlock(buffer, co.seq), entries: null }
  const update = context.updateCheckpointMaybe(slots[i].block, co.core, config)
  if (update !== null) await update
}

// a child in a core the context does not know yet needs the checkpoint first
function needsCore(context, d) {
  return d.pointer !== null && !context.hasCore(d.pointer.core)
}

function fillCore(context, d, block, config) {
  return context.updateMaybe(config, d.pointer.core, block)
}

// Building from filled slots, sync.

function key(context, d, blk) {
  const k = d.pointer
  if (!k) return new DeltaOp(false, d.type, d.index, null)

  const bk = blk.keys[k.offset]

  let vp = null

  if (bk.valuePointer) {
    const p = bk.valuePointer
    const ctx = context.getLoadedContext(k.core)
    vp = new ValuePointer(ctx, p.core, p.seq, p.offset, p.split)
  }

  const kp = new KeyPointer(context, k.core, k.seq, k.offset, false, bk.key, bk.value, vp)
  return new DeltaOp(false, d.type, d.index, kp)
}

function keyCohort(context, d, slot) {
  const co = d.pointer
  const ctx = context.getLoadedContext(co.core)
  const cohort = slot.block.cohorts[co.offset]

  const keys = new Array(cohort.length)
  for (let j = 0; j < keys.length; j++) keys[j] = key(ctx, cohort[j], slot.entries[j])

  return new DeltaCohort(false, new Pointer(context, co.core, co.seq, co.offset), keys)
}

function child(context, d) {
  const p = d.pointer
  const c = p && context.createTreeNode(p.core, p.seq, p.offset, false, null)
  return new DeltaOp(false, d.type, d.index, c)
}

function childCohort(context, d, slot) {
  const co = d.pointer
  const ctx = context.getLoadedContext(co.core)
  const cohort = slot.block.cohorts[co.offset]

  const children = new Array(cohort.length)
  for (let j = 0; j < children.length; j++) children[j] = child(ctx, cohort[j])

  return new DeltaCohort(false, new Pointer(context, co.core, co.seq, co.offset), children)
}
