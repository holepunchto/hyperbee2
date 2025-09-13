// ported from the old protocol buffer impl in old bee

const encodings = require('protocol-buffers-encodings')
const varint = encodings.varint
const skip = encodings.skip

module.exports = decodeCompat

function decodeCompat (buffer, seq) {
  const node = decodeNode(buffer)
  const index = decodeYoloIndex(node.index)
  const morphed = {
    type: 0,
    checkpoint: 0,
    batch: { start: 0, end: 0 },
    previous: seq > 1 ? { core: 0, seq: seq - 1 } : null,
    tree: [],
    data: [{ key: node.key, value: node.value }],
    cores: null
  }

  for (const lvl of index.levels) {
    const t = { keys: [], children: [] }

    for (let i = 0; i < lvl.keys.length; i++) {
      const seq = lvl.keys[i]
      t.keys.push({ core: 0, seq, offset: 0 })
    }
    for (let i = 0; i < lvl.children.length; i += 2) {
      const seq = lvl.children[i]
      const offset = lvl.children[i + 1]
      t.children.push({ core: 0, seq, offset })
    }

    morphed.tree.push(t)
  }

  return morphed
}

function decodeLevel (buf, offset, end) {
  if (!offset) offset = 0
  if (!end) end = buf.length
  if (!(end <= buf.length && offset <= buf.length)) throw new Error('Decoded message is not valid')

  const oldOffset = offset

  const obj = {
    keys: [],
    children: []
  }

  while (true) {
    if (end <= offset) {
      decodeLevel.bytes = offset - oldOffset
      return obj
    }
    const prefix = varint.decode(buf, offset)
    offset += varint.decode.bytes
    const tag = prefix >> 3
    switch (tag) {
      case 1: {
        let packedEnd = varint.decode(buf, offset)
        offset += varint.decode.bytes
        packedEnd += offset
        while (offset < packedEnd) {
          obj.keys.push(encodings.varint.decode(buf, offset))
          offset += encodings.varint.decode.bytes
        }
        break
      }
      case 2: {
        let packedEnd = varint.decode(buf, offset)
        offset += varint.decode.bytes
        packedEnd += offset
        while (offset < packedEnd) {
          obj.children.push(encodings.varint.decode(buf, offset))
          offset += encodings.varint.decode.bytes
        }
        break
      }
      default: {
        offset = skip(prefix & 7, buf, offset)
      }
    }
  }
}

function decodeYoloIndex (buf, offset, end) {
  if (!offset) offset = 0
  if (!end) end = buf.length
  if (!(end <= buf.length && offset <= buf.length)) throw new Error('Decoded message is not valid')
  const oldOffset = offset
  const obj = {
    levels: []
  }
  while (true) {
    if (end <= offset) {
      decodeYoloIndex.bytes = offset - oldOffset
      return obj
    }
    const prefix = varint.decode(buf, offset)
    offset += varint.decode.bytes
    const tag = prefix >> 3
    switch (tag) {
      case 1: {
        const len = varint.decode(buf, offset)
        offset += varint.decode.bytes
        obj.levels.push(decodeLevel(buf, offset, offset + len))
        offset += decodeLevel.bytes
        break
      }
      default: {
        offset = skip(prefix & 7, buf, offset)
      }
    }
  }
}

function decodeNode (buf, offset, end) {
  if (!offset) offset = 0
  if (!end) end = buf.length
  if (!(end <= buf.length && offset <= buf.length)) throw new Error('Decoded message is not valid')
  const oldOffset = offset
  const obj = {
    index: null,
    key: null,
    value: null
  }
  let found0 = false
  let found1 = false
  while (true) {
    if (end <= offset) {
      if (!found0 || !found1) throw new Error('Decoded message is not valid')
      decodeNode.bytes = offset - oldOffset
      return obj
    }
    const prefix = varint.decode(buf, offset)
    offset += varint.decode.bytes
    const tag = prefix >> 3
    switch (tag) {
      case 1: {
        obj.index = encodings.bytes.decode(buf, offset)
        offset += encodings.bytes.decode.bytes
        found0 = true
        break
      }
      case 2: {
        obj.key = encodings.bytes.decode(buf, offset)
        offset += encodings.bytes.decode.bytes
        found1 = true
        break
      }
      case 3: {
        obj.value = encodings.bytes.decode(buf, offset)
        offset += encodings.bytes.decode.bytes
        break
      }
      default: {
        offset = skip(prefix & 7, buf, offset)
      }
    }
  }
}
