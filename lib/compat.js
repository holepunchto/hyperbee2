// ported from the old protocol buffer impl in old bee

const encodings = require('protocol-buffers-encodings')
const b4a = require('b4a')
const varint = encodings.varint
const skip = encodings.skip

exports.decode = decodeCompat
exports.encode = encodeCompat

function decodeCompat(buffer, seq) {
  const node = buffer.length > 1 ? decodeNode(buffer) : null
  const index = node ? decodeYoloIndex(node.index) : { levels: [] }
  const morphed = {
    type: 0,
    checkpoint: 0,
    batch: { start: 0, end: 0 },
    previous: seq > 1 ? { core: 0, seq: seq - 1 } : null,
    tree: [],
    data: node ? [{ key: node.key, value: node.value }] : [],
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

function encodeCompat(node) {
  const index = { levels: [] }

  if (node.tree) {
    for (const t of node.tree) {
      const lvl = { keys: [], children: [] }
      for (const k of t.keys) lvl.keys.push(k.seq)
      for (const c of t.children) lvl.children.push(c.seq, c.offset)
      index.levels.push(lvl)
    }
  }

  const bufIndex = b4a.allocUnsafe(encodingLengthYoloIndex(index))
  encodeYoloIndex(index, bufIndex, 0)

  const n = {
    key: node.keys[0].key,
    value: node.keys[0].value,
    index: bufIndex
  }

  const buf = b4a.allocUnsafe(encodingLengthNode(n))
  return encodeNode(n, buf, 0)
}

function decodeLevel(buf, offset, end) {
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

function encodingLengthLevel(obj) {
  let length = 0
  if (obj.keys) {
    let packedLen = 0
    for (let i = 0; i < obj.keys.length; i++) {
      if (!obj.keys[i]) continue
      const len = encodings.varint.encodingLength(obj.keys[i])
      packedLen += len
    }
    if (packedLen) {
      length += 1 + packedLen + varint.encodingLength(packedLen)
    }
  }
  if (obj.children) {
    let packedLen = 0
    for (let i = 0; i < obj.children.length; i++) {
      if (!obj.children[i]) continue
      const len = encodings.varint.encodingLength(obj.children[i])
      packedLen += len
    }
    if (packedLen) {
      length += 1 + packedLen + varint.encodingLength(packedLen)
    }
  }
  return length
}

function encodeLevel(obj, buf, offset) {
  if (!offset) offset = 0
  const oldOffset = offset
  if (obj.keys) {
    let packedLen = 0
    for (let i = 0; i < obj.keys.length; i++) {
      if (!obj.keys[i]) continue
      packedLen += encodings.varint.encodingLength(obj.keys[i])
    }
    if (packedLen) {
      buf[offset++] = 10
      varint.encode(packedLen, buf, offset)
      offset += varint.encode.bytes
    }
    for (let i = 0; i < obj.keys.length; i++) {
      if (!obj.keys[i]) continue
      encodings.varint.encode(obj.keys[i], buf, offset)
      offset += encodings.varint.encode.bytes
    }
  }
  if (obj.children) {
    let packedLen = 0
    for (let i = 0; i < obj.children.length; i++) {
      if (!obj.children[i]) continue
      packedLen += encodings.varint.encodingLength(obj.children[i])
    }
    if (packedLen) {
      buf[offset++] = 18
      varint.encode(packedLen, buf, offset)
      offset += varint.encode.bytes
    }
    for (let i = 0; i < obj.children.length; i++) {
      if (!obj.children[i]) continue
      encodings.varint.encode(obj.children[i], buf, offset)
      offset += encodings.varint.encode.bytes
    }
  }
  encodeLevel.bytes = offset - oldOffset
  return buf
}

function decodeYoloIndex(buf, offset, end) {
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

function encodingLengthYoloIndex(obj) {
  let length = 0
  if (obj.levels) {
    for (let i = 0; i < obj.levels.length; i++) {
      if (!obj.levels[i]) continue
      const len = encodingLengthLevel(obj.levels[i])
      length += varint.encodingLength(len)
      length += 1 + len
    }
  }
  return length
}

function encodeYoloIndex(obj, buf, offset) {
  if (!offset) offset = 0
  const oldOffset = offset
  if (obj.levels) {
    for (let i = 0; i < obj.levels.length; i++) {
      if (!obj.levels[i]) continue
      buf[offset++] = 10
      const len = encodingLengthLevel(obj.levels[i])
      varint.encode(len, buf, offset)
      offset += varint.encode.bytes
      encodeLevel(obj.levels[i], buf, offset)
      offset += encodeLevel.bytes
    }
  }
  encodeYoloIndex.bytes = offset - oldOffset
  return buf
}

function decodeNode(buf, offset, end) {
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

function encodingLengthNode(obj) {
  let length = 0
  let len = encodings.bytes.encodingLength(obj.index)
  length += 1 + len
  len = encodings.bytes.encodingLength(obj.key)
  length += 1 + len
  if (obj.value) {
    const len = encodings.bytes.encodingLength(obj.value)
    length += 1 + len
  }
  return length
}

function encodeNode(obj, buf, offset) {
  if (!offset) offset = 0
  const oldOffset = offset
  buf[offset++] = 10
  encodings.bytes.encode(obj.index, buf, offset)
  offset += encodings.bytes.encode.bytes
  buf[offset++] = 18
  encodings.bytes.encode(obj.key, buf, offset)
  offset += encodings.bytes.encode.bytes
  if (obj.value) {
    buf[offset++] = 26
    encodings.bytes.encode(obj.value, buf, offset)
    offset += encodings.bytes.encode.bytes
  }
  encodeNode.bytes = offset - oldOffset
  return buf
}
