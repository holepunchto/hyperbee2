const c = require('compact-encoding')
const { getEncoding } = require('../spec/hyperschema')
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
    return decodeBlock0(state)
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

function decodeBlock0(state) {
  const blk = Block0.decode(state)

  return {
    type: 0,
    checkpoint: blk.checkpoint,
    batch: blk.batch,
    previous: blk.previous,
    cores: blk.cores,
    tree: blk.tree,
    keys: blk.data.map(dataToKey),
    values: null
  }
}

function encodeBlock0(blk) {
  return c.encode(Block0, {
    type: 0,
    checkpoint: blk.checkpoint,
    batch: blk.batch,
    previous: blk.previous,
    tree: blk.tree,
    data: blk.keys.map(keyToData),
    cores: blk.cores
  })
}
