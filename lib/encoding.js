const c = require('compact-encoding')
const { getEncoding } = require('../spec/hyperschema')
const { OP_COHORT, OP_INSERT } = require('./compression.js')
const compat = require('./compat.js')

const Block0 = getEncoding('@bee/block-0')
const Block1 = getEncoding('@bee/block-1')

const TYPE_COMPAT = 10 // compat with bee1, all blocks starts with 0x10

exports.TYPE_COMPAT = TYPE_COMPAT
exports.TYPE_LATEST = 1

exports.Block = Block1
exports.encodeBlock = encodeBlock
exports.decodeBlock = decodeBlock

function encodeBlock(block) {
  if (block.type === 1) {
    return c.encode(Block1, block)
  }

  if (block.type === 0) {
    return encodeBlock0(block)
  }

  if (block.type === TYPE_COMPAT) {
    return compat.encode(block)
  }

  throw new Error('Unknown block type: ' + block.type)
}

function decodeBlock(buffer, seq) {
  const state = { start: 0, end: buffer.byteLength, buffer }
  const type = state.end > 0 ? c.uint.decode(state) : 0

  state.start = 0

  if (type === 1) {
    return Block1.decode(state)
  }

  if (type === 0) {
    return decodeBlock0(state, seq)
  }

  if (type === TYPE_COMPAT) {
    return compat.decode(buffer, seq)
  }

  throw new Error('Unknown block type: ' + type)
}

function dataToKey(d) {
  return { ...d, valuePointer: null }
}

function keyToData(d) {
  return { key: d.key, value: d.value }
}

function decodeBlock0(state, seq) {
  const cohorts = []
  const blk = Block0.decode(state)
  const tree = []

  for (const t of blk.tree) {
    const next = {
      keys: toCohort(seq, t.keys, cohorts),
      children: toCohort(seq, t.children, cohorts)
    }

    tree.push(next)
  }

  return {
    type: 0,
    checkpoint: blk.checkpoint,
    batch: blk.batch,
    previous: blk.previous,
    metadata: { cores: blk.cores },
    tree,
    keys: blk.data.map(dataToKey),
    values: null,
    cohorts
  }
}

function encodeBlock0(blk) {
  const tree = []

  for (const t of blk.tree) {
    tree.push({
      keys: fromCohort(t.keys, blk.cohorts),
      children: fromCohort(t.children, blk.cohorts)
    })
  }

  return c.encode(Block0, {
    type: 0,
    checkpoint: blk.checkpoint,
    batch: blk.batch,
    previous: blk.previous,
    tree,
    data: blk.keys.map(keyToData),
    cores: blk.metadata ? blk.metadata.cores : null
  })
}

function fromCohort(deltas, cohorts) {
  if (deltas.length === 0) return []
  const cohort = cohorts[deltas[0].pointer.offset]
  const pointers = []
  for (const d of cohort) pointers.push(d.pointer)
  return pointers
}

function toCohort(seq, pointers, cohorts) {
  if (!pointers.length) return []

  const cohort = []
  const offset = cohorts.push(cohort) - 1

  for (let i = 0; i < pointers.length; i++) {
    cohort.push({
      type: OP_INSERT,
      index: i,
      pointer: pointers[i]
    })
  }

  return [
    {
      type: OP_COHORT,
      index: 0,
      pointer: {
        core: 0,
        seq,
        offset
      }
    }
  ]
}
