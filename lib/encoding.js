const c = require('compact-encoding')
const { getEncoding } = require('../spec/hyperschema')
const decodeCompatBlock = require('./compat.js')

const Block = getEncoding('@bee/block')

exports.Block = Block
exports.decodeBlock = decodeBlock

function decodeBlock (buffer, seq) {
  const isCompat = buffer.length > 0 && buffer[0] === 0x0a
  const block = isCompat ? decodeCompatBlock(buffer, seq) : c.decode(Block, buffer)
  return block
}
