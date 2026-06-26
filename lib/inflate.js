const b4a = require('b4a')
const { Pointer, KeyPointer, ValuePointer, TreeNode } = require('./tree.js')
const { DeltaOp, DeltaCohort, OP_COHORT } = require('./compression.js')

exports.inflate = async function inflate(ptr, config) {
  if (ptr.value) return ptr.value

  const [block, context] = await Promise.all([
    ptr.context.getBlock(ptr.seq, ptr.core, config),
    ptr.context.getContext(ptr.core, config)
  ])
  if (config.localOnly && (!block || !context)) return null

  const tree = block.tree[ptr.offset]

  const keys = new Array(tree.keys.length)
  const children = new Array(tree.children.length)

  for (let i = 0; i < keys.length; i++) {
    const d = tree.keys[i]
    keys[i] = inflateKey(context, d, ptr, block, config)
  }

  for (let i = 0; i < children.length; i++) {
    const d = tree.children[i]
    children[i] = inflateChild(context, d, ptr, block, config)
  }

  const [k, c] = await Promise.all([Promise.all(keys), Promise.all(children)])

  const value = new TreeNode(k, c)
  if (!ptr.value) ptr.value = value
  return ptr.value
}

function missingDelta(d) {
  return new DeltaOp(false, d.type, d.index, null)
}

function inflateKey(context, d, ptr, block, config) {
  if (d.type === OP_COHORT) return inflateKeyCohort(context, d, ptr, block, config)
  return inflateKeyDelta(context, d, ptr, block, config)
}

async function inflateKeyDelta(context, d, ptr, block, config) {
  const k = d.pointer

  if (!k) return missingDelta(d)

  const blk =
    k.seq === ptr.seq && k.core === 0 && ptr.core === 0
      ? block
      : await context.getBlock(k.seq, k.core, config)

  if (config.localOnly && !blk) return missingDelta(d)

  const bk = blk.keys[k.offset]

  let vp = null

  if (bk.valuePointer) {
    const p = bk.valuePointer
    const ctx = await context.getContext(k.core, config)
    if (config.localOnly && !ctx) return missingDelta(d)
    vp = new ValuePointer(ctx, p.core, p.seq, p.offset, p.split)
  }

  const kp = new KeyPointer(context, k.core, k.seq, k.offset, false, bk.key, bk.value, vp)
  return new DeltaOp(false, d.type, d.index, kp)
}

exports.inflateValue = async function inflateValue(key, config) {
  if (key.value) return key.value
  if (!key.valuePointer) return null

  const ptr = key.valuePointer

  if (ptr.split === 0) {
    const block = await ptr.context.getBlock(ptr.seq, ptr.core, config)
    if (config.localOnly && !block) return null
    return block.values[ptr.offset]
  }

  const blockPromises = new Array(ptr.split + 1)
  for (let i = 0; i < blockPromises.length; i++) {
    blockPromises[i] = ptr.context.getBlock(ptr.seq - ptr.split + i, ptr.core, config)
  }
  const blocks = await Promise.all(blockPromises)

  // if any block is missing, treat the whole value as missing.
  if (config.localOnly && blocks.includes(null)) return null

  const splitValue = new Array(blockPromises.length)
  for (let i = 0; i < splitValue.length - 1; i++) {
    splitValue[i] = blocks[i].values[0]
  }
  splitValue[splitValue.length - 1] = blocks[blocks.length - 1].buffer[ptr.offset]
  return b4a.concat(splitValue)
}

async function inflateKeyCohort(context, d, ptr, block, config) {
  const co = d.pointer

  const blk =
    co.seq === ptr.seq && co.core === 0 && ptr.core === 0
      ? block
      : await context.getBlock(co.seq, co.core, config)

  if (config.localOnly && !blk) return missingDelta(d)

  const cohort = blk.cohorts[co.offset]
  const promises = new Array(cohort.length)
  const ctx = await context.getContext(co.core, config)
  if (config.localOnly && !ctx) return missingDelta(d)

  for (let i = 0; i < cohort.length; i++) {
    const p = cohort[i]
    const k = inflateKeyDelta(ctx, p, co, blk, config)
    promises[i] = k
  }

  const p = new Pointer(context, co.core, co.seq, co.offset)
  return new DeltaCohort(false, p, await Promise.all(promises))
}

async function inflateChild(context, d, ptr, block, config) {
  if (d.type === OP_COHORT) return inflateChildCohort(context, d, ptr, block, config)
  const missingCore = d.pointer && !context.hasCore(d.pointer.core)

  if (config.localOnly && missingCore) return missingDelta(d)
  else if (missingCore) await context.update(config)

  return inflateChildDelta(context, d, ptr, block, config)
}

function inflateChildDelta(context, d, ptr, block, config) {
  const p = d.pointer
  const c = p && context.createTreeNode(p.core, p.seq, p.offset, false, null)
  return new DeltaOp(false, d.type, d.index, c)
}

async function inflateChildCohort(context, d, ptr, block, config) {
  const co = d.pointer

  const blk =
    co.seq === ptr.seq && co.core === 0 && ptr.core === 0
      ? block
      : await context.getBlock(co.seq, co.core, config)

  if (config.localOnly && !blk) return missingDelta(d)

  const cohort = blk.cohorts[co.offset]
  const deltas = new Array(cohort.length)
  const ctx = await context.getContext(co.core, config)
  if (config.localOnly && !ctx) return missingDelta(d)

  for (let i = 0; i < cohort.length; i++) {
    const c = cohort[i]
    if (config.localOnly) {
      if (c.pointer && !ctx.hasCore(c.pointer.core)) {
        deltas[i] = missingDelta(c)
        continue
      }
    } else if (!ctx.hasCore(c.pointer.core)) {
      await ctx.update(config)
    }
    deltas[i] = inflateChildDelta(ctx, c, co, blk, config)
  }

  const p = new Pointer(context, co.core, co.seq, co.offset)
  return new DeltaCohort(false, p, deltas)
}
