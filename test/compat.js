const test = require('brittle')
const b4a = require('b4a')
const c = require('compact-encoding')
const { create } = require('./helpers')
const { decodeBlock } = require('../lib/encoding.js')
const { getEncoding } = require('../spec/hyperschema')

const hyperbee1Header = b4a.from('0a086879706572626565', 'hex')

// The remaining legacy fixtures were captured from hyperbee@2.27.3. A fresh
// core after `await bee.del('absent')` holds only the protocol header. The
// metadata variants come from Bees created with
// { contentFeed: <32 x 0xab>, userData: <'user data'> } and `metadata: {}`.
const hyperbee1HeaderWithMetadata = b4a.from(
  '0a086879706572626565122d0a20abababababababababababababababababababababababababababababababab1209757365722064617461',
  'hex'
)
const hyperbee1HeaderWithEmptyMetadata = b4a.from('0a0868797065726265651200', 'hex')

// Malformed header variants: a truncated protocol, a metadata field that
// declares no payload, and unknown fields in the header and its metadata.
const hyperbee1TruncatedHeader = b4a.from('0a0868797065726265', 'hex')
const hyperbee1TruncatedHeaderMetadata = b4a.from('0a0868797065726265651201', 'hex')
const hyperbee1UnknownHeaderField = b4a.from('0a0868797065726265651a00', 'hex')
const hyperbee1UnknownMetadataField = b4a.from('0a08687970657262656512021a00', 'hex')

// Node.encode({ index: YoloIndex with keys: [0], key: 'hello', value: 'world' })
// from hyperbee@2.27.3, appended without a header so the core is one block.
const hyperbee1SingleNode = b4a.from('0a050a030a0100120568656c6c6f1a05776f726c64', 'hex')

// The first block of the 'basic block 0' batch below is self-contained, so it
// is a valid modern one-block core on its own.
const block0SingleBlock = b4a.from('000000000601010000000001022330022330', 'hex')

const hyperbee1Batch = [
  hyperbee1Header,
  b4a.from('0a050a030a0101120568656c6c6f1a05776f726c64', 'hex'),
  b4a.from('0a060a040a020201120368656a1a0676657264656e', 'hex'),
  b4a.from('0a070a050a030201031204686f6c611a056d756e646f', 'hex')
]

test('basic hyperbee1', async function (t) {
  const db = await create(t)

  await db.core.append(hyperbee1Batch)

  const snap = db.checkout({ length: db.core.length })
  const expected = [
    ['hej', 'verden'],
    ['hello', 'world'],
    ['hola', 'mundo']
  ]

  for await (const data of snap.createReadStream()) {
    t.alike([b4a.toString(data.key), b4a.toString(data.value)], expected.shift())
  }

  t.is(expected.length, 0)

  await snap.close()
})

test('import copies a hyperbee1 fixture into a fresh tree', async function (t) {
  const source = await create(t)
  await source.ready()
  await source.core.append(hyperbee1Batch)

  const checkout = source.checkout({ length: source.core.length })
  t.teardown(() => checkout.close())

  const destination = await create(t)
  await destination.import(checkout)

  t.alike(
    await readEntries(destination),
    [
      { key: b4a.from('hej'), value: b4a.from('verden') },
      { key: b4a.from('hello'), value: b4a.from('world') },
      { key: b4a.from('hola'), value: b4a.from('mundo') }
    ],
    'copies the fixture in key order'
  )
})

test('reads a real empty hyperbee1 tree as an empty tree', async function (t) {
  const headers = [
    ['without metadata', hyperbee1Header],
    ['with metadata', hyperbee1HeaderWithMetadata],
    ['with empty metadata', hyperbee1HeaderWithEmptyMetadata]
  ]

  for (const [label, header] of headers) {
    const source = await create(t, { t: 5 })
    await source.ready()
    await source.core.append(header)
    source.update()

    t.is(source.head().length, 1, `${label}: source root is the legacy header block`)
    t.alike(await readEntries(source), [], `${label}: source stream yields no entries`)

    const destination = await create(t)
    await destination.ready()
    const before = { length: destination.core.length, head: destination.head() }

    await destination.import(source)

    t.is(destination.core.length, before.length, `${label}: imports without appending a block`)
    t.alike(destination.head(), before.head, `${label}: destination head unchanged`)
    t.alike(await readEntries(destination), [], `${label}: destination stays empty`)
  }
})

test('rejects malformed hyperbee1 headers instead of reading them as empty', async function (t) {
  const headers = [
    ['truncated protocol', hyperbee1TruncatedHeader],
    ['truncated metadata', hyperbee1TruncatedHeaderMetadata],
    ['unknown header field', hyperbee1UnknownHeaderField],
    ['unknown metadata field', hyperbee1UnknownMetadataField]
  ]

  for (const [label, header] of headers) {
    const source = await create(t, { t: 5 })
    await source.ready()
    await source.core.append(header)
    source.update()

    await t.exception(
      readEntries(source),
      /Decoded message is not valid/,
      `${label}: read path rejects`
    )

    const destination = await create(t)
    await destination.ready()
    const before = { length: destination.core.length, head: destination.head() }

    await t.exception(
      destination.import(source),
      /Decoded message is not valid/,
      `${label}: import rejects`
    )

    t.is(destination.core.length, before.length, `${label}: appends no blocks`)
    t.alike(destination.head(), before.head, `${label}: head unchanged`)
    t.alike(await readEntries(destination), [], `${label}: destination stays empty`)
  }
})

test('rejects legacy header bytes at a node position', async function (t) {
  const source = await create(t, { t: 5 })
  await source.ready()
  await source.core.append(hyperbee1Batch)
  await source.core.append(hyperbee1Header)
  source.update()

  t.is(source.head().length, hyperbee1Batch.length + 1, 'header is the root block')

  await t.exception(readEntries(source), /Decoded message is not valid/, 'read path rejects')

  const destination = await create(t)
  await destination.ready()
  const before = { length: destination.core.length, head: destination.head() }

  await t.exception(destination.import(source), /Decoded message is not valid/, 'import rejects')

  t.is(destination.core.length, before.length, 'appends no blocks')
  t.alike(destination.head(), before.head, 'head unchanged')
  t.alike(await readEntries(destination), [], 'destination stays empty')
})

test('reads and imports a populated one-block hyperbee1 core', async function (t) {
  const source = await create(t, { t: 5 })
  await source.ready()
  await source.core.append(hyperbee1SingleNode)
  source.update()

  const expected = [{ key: b4a.from('hello'), value: b4a.from('world') }]

  t.alike(await readEntries(source), expected, 'reads the single legacy node')

  const destination = await create(t)
  await destination.import(source)

  t.alike(await readEntries(destination), expected, 'imports the single legacy node')
})

test('reads and imports a populated one-block modern block 0 core', async function (t) {
  const source = await create(t)
  await source.ready()
  await source.core.append(block0SingleBlock)
  source.update()

  const expected = [{ key: b4a.from('#0'), value: b4a.from('#0') }]

  t.alike(await readEntries(source), expected, 'reads the single modern block')

  const destination = await create(t)
  await destination.import(source)

  t.alike(await readEntries(destination), expected, 'imports the single modern block')
})

test('bigger hyperbee1', async function (t) {
  const compatBatch = [
    b4a.from('0a086879706572626565', 'hex'),
    b4a.from('0a050a030a0101120223301a022330', 'hex'),
    b4a.from('0a060a040a020102120223311a022331', 'hex'),
    b4a.from('0a070a050a03010203120223321a022332', 'hex'),
    b4a.from('0a080a060a0401020304120223331a022333', 'hex'),
    b4a.from('0a090a070a050102030405120223341a022334', 'hex'),
    b4a.from('0a0a0a080a06010203040506120223351a022335', 'hex'),
    b4a.from('0a0b0a090a0701020304050607120223361a022336', 'hex'),
    b4a.from('0a0c0a0a0a080102030405060708120223371a022337', 'hex'),
    b4a.from('0a1b0a090a01051204090109020a060a04010203040a060a0406070809120223381a022338', 'hex'),
    b4a.from('0a140a090a0105120409010a010a070a05060708090a120223391a022339', 'hex'),
    b4a.from('0a140a090a010512040b010a010a070a0501020b030412032331301a03233130', 'hex'),
    b4a.from('0a150a090a010512040c010a010a080a0601020b0c030412032331311a03233131', 'hex'),
    b4a.from('0a160a090a010512040d010a010a090a0701020b0c0d030412032331321a03233132', 'hex'),
    b4a.from('0a170a090a010512040e010a010a0a0a0801020b0c0d0e030412032331331a03233133', 'hex'),
    b4a.from(
      '0a1e0a0c0a020d0512060f010f020a010a060a0401020b0c0a060a040e0f030412032331341a03233134',
      'hex'
    ),
    b4a.from('0a170a0c0a020d0512060f0110010a010a070a050e0f10030412032331351a03233135', 'hex'),
    b4a.from('0a180a0c0a020d0512060f0111010a010a080a060e0f1011030412032331361a03233136', 'hex'),
    b4a.from('0a190a0c0a020d0512060f0112010a010a090a070e0f101112030412032331371a03233137', 'hex'),
    b4a.from('0a1a0a0c0a020d0512060f0113010a010a0a0a080e0f10111213030412032331381a03233138', 'hex'),
    b4a.from(
      '0a210a0f0a030d120512080f01140114020a010a060a040e0f10110a060a041314030412032331391a03233139',
      'hex'
    )
  ]

  const db = await create(t, { t: 5 })

  await db.core.append(compatBatch)

  const snap = db.checkout({ length: db.core.length })
  const expected = []

  for (let i = 0; i < 20; i++) {
    expected.push(['#' + i, '#' + i])
  }

  expected.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))

  for await (const data of snap.createReadStream()) {
    t.alike([b4a.toString(data.key), b4a.toString(data.value)], expected.shift())
  }

  t.is(expected.length, 0)

  await snap.close()
})

test('encode hyperbee1', async function (t) {
  const db = await create(t, { t: 5 })

  const w = db.write({ compat: true })

  const expected = []

  for (let i = 0; i < 4; i++) {
    w.tryPut(b4a.from('#' + i), b4a.from('#' + i))
    expected.push(['#' + i, '#' + i])
  }

  expected.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))

  await w.flush()

  for await (const data of db.createReadStream()) {
    t.alike([b4a.toString(data.key), b4a.toString(data.value)], expected.shift())
  }

  t.is(expected.length, 0)
})

test('encode bigger hyperbee1', async function (t) {
  const db = await create(t, { t: 5 })

  const w = db.write({ compat: true })

  const expected = []

  for (let i = 0; i < 20; i++) {
    w.tryPut(b4a.from('#' + i), b4a.from('#' + i))
    expected.push(['#' + i, '#' + i])
  }

  expected.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))

  await w.flush()

  db.cache.empty()

  for await (const data of db.createReadStream()) {
    t.alike([b4a.toString(data.key), b4a.toString(data.value)], expected.shift())
  }

  t.is(expected.length, 0)
})

test('basic block 0', async function (t) {
  const block0Batch = [
    b4a.from('000000000601010000000001022330022330', 'hex'),
    b4a.from('0000000007000001020000000001000001022331022331', 'hex'),
    b4a.from('0000000007000101030000000001000002000001022332022332', 'hex'),
    b4a.from('0000000007000201040000000001000002000003000001022333022333', 'hex')
  ]

  const db = await create(t, { t: 5 })

  await db.core.append(block0Batch)

  const snap = db.checkout({ length: db.core.length })
  const expected = []

  for (let i = 0; i < 4; i++) {
    expected.push(['#' + i, '#' + i])
  }

  expected.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))

  for await (const data of snap.createReadStream()) {
    t.alike([b4a.toString(data.key), b4a.toString(data.value)], expected.shift())
  }

  t.is(expected.length, 0)

  await snap.close()
})

test('bigger block 0', async function (t) {
  const block0Batch = [
    b4a.from('000000000601010000000001022330022330', 'hex'),
    b4a.from('0000000007000001020000000001000001022331022331', 'hex'),
    b4a.from('0000000007000101030000000001000002000001022332022332', 'hex'),
    b4a.from('0000000007000201040000000001000002000003000001022333022333', 'hex'),
    b4a.from('0000000007000301050000000001000002000003000004000001022334022334', 'hex'),
    b4a.from('0000000007000401060000000001000002000003000004000005000001022335022335', 'hex'),
    b4a.from('0000000007000501070000000001000002000003000004000005000006000001022336022336', 'hex'),
    b4a.from(
      '0000000007000601080000000001000002000003000004000005000006000007000001022337022337',
      'hex'
    ),
    b4a.from('000000010601040005000006000007000008000001022338022338', 'hex'),
    b4a.from('000001000300070201000400020009010008000400000000010000020000030000', 'hex'),
    b4a.from(
      '00000000070009020100040002000901000a0105000500000600000700000800000a000001022339022339',
      'hex'
    ),
    b4a.from(
      '0000000007000a020100040002000b01000a0105000000000100000b0000020000030000010323313003233130',
      'hex'
    ),
    b4a.from(
      '0000000007000b020100040002000c01000a0106000000000100000b00000c0000020000030000010323313103233131',
      'hex'
    ),
    b4a.from(
      '0000000007000c020100040002000d01000a0107000000000100000b00000c00000d0000020000030000010323313203233132',
      'hex'
    ),
    b4a.from(
      '0000000007000d020100040002000e01000a0108000000000100000b00000c00000d00000e0000020000030000010323313303233133',
      'hex'
    ),
    b4a.from('00000001060104000e00000f0000020000030000010323313403233134', 'hex'),
    b4a.from(
      '0000010007000e020200100000040003001001000f00000a0104000000000100000b00000c0000010323313203233132',
      'hex'
    ),
    b4a.from(
      '00000000070010020200100000040003001001001101000a0105000e00000f0000110000020000030000010323313503233135',
      'hex'
    ),
    b4a.from(
      '00000000070011020200100000040003001001001201000a0106000e00000f0000110000120000020000030000010323313603233136',
      'hex'
    ),
    b4a.from(
      '00000000070012020200100000040003001001001301000a0107000e00000f0000110000120000130000020000030000010323313703233137',
      'hex'
    ),
    b4a.from(
      '00000000070013020200100000040003001001001401000a0108000e00000f0000110000120000130000140000020000030000010323313803233138',
      'hex'
    ),
    b4a.from('0000000106010400140000150000020000030000010323313903233139', 'hex'),
    b4a.from(
      '00000100070014020300100000160000040004001001001601001500000a0104000e00000f0000110000120000010323313703233137',
      'hex'
    )
  ]

  const db = await create(t, { t: 5 })

  await db.core.append(block0Batch)

  const snap = db.checkout({ length: db.core.length })
  const expected = []

  for (let i = 0; i < 20; i++) {
    expected.push(['#' + i, '#' + i])
  }

  expected.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))

  for await (const data of snap.createReadStream()) {
    t.alike([b4a.toString(data.key), b4a.toString(data.value)], expected.shift())
  }

  t.is(expected.length, 0)

  await snap.close()
})

test('encode block0', async function (t) {
  const db = await create(t, { t: 5 })

  const w = db.write({ type: 0 })

  const expected = []

  for (let i = 0; i < 4; i++) {
    w.tryPut(b4a.from('#' + i), b4a.from('#' + i))
    expected.push(['#' + i, '#' + i])
  }

  expected.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))

  await w.flush()

  for await (const data of db.createReadStream()) {
    t.alike([b4a.toString(data.key), b4a.toString(data.value)], expected.shift())
  }

  t.is(expected.length, 0)
})

test('encode bigger block0', async function (t) {
  const db = await create(t, { t: 5 })

  const w = db.write({ type: 0 })

  const expected = []

  for (let i = 0; i < 20; i++) {
    w.tryPut(b4a.from('#' + i), b4a.from('#' + i))
    expected.push(['#' + i, '#' + i])
  }

  expected.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))

  await w.flush()

  db.cache.empty()

  for await (const data of db.createReadStream()) {
    t.alike([b4a.toString(data.key), b4a.toString(data.value)], expected.shift())
  }

  t.is(expected.length, 0)
})

test('compat bubbles through latest-format writes', async function (t) {
  // the 20 key hyperbee1 tree from the 'bigger hyperbee1' test
  const compatBatch = [
    b4a.from('0a086879706572626565', 'hex'),
    b4a.from('0a050a030a0101120223301a022330', 'hex'),
    b4a.from('0a060a040a020102120223311a022331', 'hex'),
    b4a.from('0a070a050a03010203120223321a022332', 'hex'),
    b4a.from('0a080a060a0401020304120223331a022333', 'hex'),
    b4a.from('0a090a070a050102030405120223341a022334', 'hex'),
    b4a.from('0a0a0a080a06010203040506120223351a022335', 'hex'),
    b4a.from('0a0b0a090a0701020304050607120223361a022336', 'hex'),
    b4a.from('0a0c0a0a0a080102030405060708120223371a022337', 'hex'),
    b4a.from('0a1b0a090a01051204090109020a060a04010203040a060a0406070809120223381a022338', 'hex'),
    b4a.from('0a140a090a0105120409010a010a070a05060708090a120223391a022339', 'hex'),
    b4a.from('0a140a090a010512040b010a010a070a0501020b030412032331301a03233130', 'hex'),
    b4a.from('0a150a090a010512040c010a010a080a0601020b0c030412032331311a03233131', 'hex'),
    b4a.from('0a160a090a010512040d010a010a090a0701020b0c0d030412032331321a03233132', 'hex'),
    b4a.from('0a170a090a010512040e010a010a0a0a0801020b0c0d0e030412032331331a03233133', 'hex'),
    b4a.from(
      '0a1e0a0c0a020d0512060f010f020a010a060a0401020b0c0a060a040e0f030412032331341a03233134',
      'hex'
    ),
    b4a.from('0a170a0c0a020d0512060f0110010a010a070a050e0f10030412032331351a03233135', 'hex'),
    b4a.from('0a180a0c0a020d0512060f0111010a010a080a060e0f1011030412032331361a03233136', 'hex'),
    b4a.from('0a190a0c0a020d0512060f0112010a010a090a070e0f101112030412032331371a03233137', 'hex'),
    b4a.from('0a1a0a0c0a020d0512060f0113010a010a0a0a080e0f10111213030412032331381a03233138', 'hex'),
    b4a.from(
      '0a210a0f0a030d120512080f01140114020a010a060a040e0f10110a060a041314030412032331391a03233139',
      'hex'
    )
  ]

  const db = await create(t)

  await db.core.append(compatBatch)
  db.update()

  for (let i = 20; i < 60; i++) {
    const w = db.write()
    w.tryPut(b4a.from('#' + i), b4a.from('#' + i))
    await w.flush()
  }

  const seq = db.core.length - 1
  const head = decodeBlock(await db.core.get(seq), seq)
  t.is(head.t, 5, 'rewritten blocks carry the tree degree')

  // drop all cached nodes so the walk decodes from disk
  db.cache.empty()
  db.update()

  const nodes = await walkNodes(db, await db.bootstrap(db.config), [])

  t.ok(nodes.length > 1, 'sanity: tree has multiple nodes')
  t.ok(
    nodes.every((v) => v.t === 5),
    'every node inflates with the compat degree'
  )
  t.ok(
    nodes.every((v) => v.keys.length <= 9),
    'compat max keys enforced across rewrites'
  )

  let count = 0
  for await (const data of db.createReadStream()) count++ // eslint-disable-line no-unused-vars
  t.is(count, 60)
})

test('latest-format writes stay non-compat', async function (t) {
  const db = await create(t)

  const w = db.write()
  for (let i = 0; i < 30; i++) w.tryPut(b4a.from('#' + i), b4a.from('#' + i))
  await w.flush()

  const seq = db.core.length - 1
  const Block1 = getEncoding('@bee/block-1')
  const head = c.decode(Block1, await db.core.get(seq))
  t.is(head.t, 0, 'latest blocks do not store a degree')

  db.cache.empty()
  db.update()

  const nodes = await walkNodes(db, await db.bootstrap(db.config), [])

  t.ok(
    nodes.every((v) => v.t === 128),
    'no node inflates with a stored degree'
  )
  t.ok(
    nodes.some((v) => v.keys.length > 9),
    'nodes are not limited to the compat degree'
  )
})

async function readEntries(bee) {
  const entries = []

  for await (const entry of bee.createReadStream()) {
    entries.push({
      key: b4a.from(entry.key),
      value: entry.value === null ? null : b4a.from(entry.value)
    })
  }

  return entries
}

async function walkNodes(db, ptr, out) {
  const v = ptr.value || (await db.inflate(ptr, db.config))
  out.push(v)
  for (let i = 0; i < v.children.length; i++) {
    await walkNodes(db, v.children.get(i), out)
  }
  return out
}
