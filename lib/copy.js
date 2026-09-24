const b4a = require('b4a')
const { encodeBlock, TYPE_COMPAT } = require('./encoding.js')

module.exports = async function copyChanges(tree, changes, config, map) {
  const local = tree.context.getLocalContext()

  await local.core.ready()
  await local.update(config)

  const copied = new Map()
  const blocks = []

  const oldest = changes[changes.length - 1]
  const base = local.core.length

  for (let i = changes.length - 1; i >= 0; i--) {
    const change = changes[i]
    const src = tree.context.getContextByKey(change.head.key)
    const srcHex = b4a.toString(src.core.key, 'hex')
    const batch = change.batch
    const start = base + blocks.length
    const first = change.head.length - batch.length

    for (let j = 0; j < batch.length; j++) copied.set(srcHex + ':' + (first + j), start + j)

    if (map !== null) await mapValues(batch, first, change, map)

    for (let j = 0; j < batch.length; j++) {
      const blk = batch[j]

      if (blk.type === TYPE_COMPAT) throw new Error('Cannot index compat blocks')

      if (blk.tree !== null) {
        for (const t of blk.tree) {
          for (const d of t.keys) await remap(src, d.pointer, blk)
          for (const d of t.children) await remap(src, d.pointer, blk)
        }
      }

      if (blk.cohorts !== null) {
        for (const cohort of blk.cohorts) {
          for (const d of cohort) await remap(src, d.pointer, blk)
        }
      }

      if (blk.keys !== null) {
        for (const k of blk.keys) await remap(src, k.valuePointer, blk)
      }

      blk.metadata = null
      blk.previous = null

      blocks.push(blk)
    }

    const last = batch[batch.length - 1]

    if (change === oldest) {
      last.previous = await previousForOldest(change.tail)
    } else {
      last.previous = { core: 0, seq: start - 1 }
    }
  }

  if (local.changed) {
    local.checkpoint = base + blocks.length
    blocks[blocks.length - 1].metadata = local.flush()
  }

  const buffers = new Array(blocks.length)

  for (let i = 0; i < blocks.length; i++) {
    blocks[i].checkpoint = local.checkpoint
    buffers[i] = encodeBlock(blocks[i])
  }

  await local.core.append(buffers)

  return changes.length

  async function remap(src, p, origin) {
    if (p === null) return

    if (p.core !== 0 && !src.hasCore(p.core)) await src.updateMaybe(config, p.core, origin)

    const key = src.getCoreKey(p.core)
    const seq = copied.get(b4a.toString(key, 'hex') + ':' + p.seq)

    if (seq !== undefined) {
      p.core = 0
      p.seq = seq
      return
    }

    p.core = local.getCoreOffsetLocal(src, p.core)
  }

  async function previousForOldest(tail) {
    if (tail === null) return null
    const core = await local.getCoreOffsetByKey(tail.key, config)
    return { core, seq: tail.length - 1 }
  }
}

// values live in the same batch as their key
async function mapValues(batch, first, change, map) {
  for (const blk of batch) {
    if (blk.type === TYPE_COMPAT) throw new Error('Cannot index compat blocks')
    if (blk.keys === null) continue

    for (const k of blk.keys) {
      const ptr = k.value === null ? k.valuePointer : null
      if (k.value === null && ptr === null) continue

      const vals = ptr === null ? null : getValues(batch, first, ptr)
      const value = ptr === null ? k.value : vals[ptr.offset]
      const next = await map(k.key, value, change)

      if (!next || next === value) continue

      if (ptr === null) k.value = next
      else vals[ptr.offset] = next
    }
  }
}

function getValues(batch, first, ptr) {
  const i = ptr.seq - first
  const blk = ptr.core === 0 && ptr.split === 0 && i >= 0 && i < batch.length ? batch[i] : null
  if (blk === null || blk.values === null || ptr.offset >= blk.values.length) {
    throw new Error('Cannot map a value stored outside its batch')
  }
  return blk.values
}
