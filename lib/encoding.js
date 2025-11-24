const c = require('compact-encoding')
const { getEncoding } = require('../spec/hyperschema')
const compat = require('./compat.js')

const Block = getEncoding('@bee/block')

exports.Block = Block
exports.encodeBlock = encodeBlock
exports.decodeBlock = decodeBlock

function encodeBlock(block, format) {
  if (format === 2) console.log('omcpat', compat.encode(block))
  return c.encode(Block, block)
}

function decodeBlock(buffer, seq) {
  const isCompat = buffer.length > 0 && buffer[0] === 0x0a
  const block = isCompat ? compat.decode(buffer, seq) : c.decode(Block, buffer)
  return block
}
