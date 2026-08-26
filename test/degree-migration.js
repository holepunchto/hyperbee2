const test = require('brittle')
const b4a = require('b4a')
const Corestore = require('corestore')
const Bee = require('../')
const { create } = require('./helpers')
const { decodeBlock, peekBlockType, TYPE_COMPAT, TYPE_LATEST } = require('../lib/encoding.js')

const DEGREE_COMPAT = 5
const DEGREE_DEFAULT = 128

test('decodeBlock returns TYPE_LATEST for compat-encoded block, not TYPE_COMPAT', async function (t) {
  const db = await create(t, { t: DEGREE_COMPAT })

  const w = db.write({ compat: true })
  w.tryPut(b4a.from('a'), b4a.from('1'))
  await w.flush()

  // the very first block is the "hyperbee" magic header, real data starts at seq 1
  const buffer = await db.core.get(1)
  t.is(peekBlockType(buffer), TYPE_COMPAT, 'block on disk is labeled as TYPE_COMPAT')

  const block = decodeBlock(buffer, 1)
  t.is(block.type, TYPE_LATEST, 'decoded compat block is relabeled as TYPE_LATEST')
})

test('opening compat block sets tree to historical t=5 degree', async function (t) {
  const dir = await t.tmp()

  {
    const store = new Corestore(dir)
    const writer = new Bee(store, { t: DEGREE_COMPAT })
    await writer.ready()

    for (let i = 0; i < 40; i++) {
      const w = writer.write({ compat: true })
      w.tryPut(b4a.from('k' + String(i).padStart(3, '0')), b4a.from('v' + i))
      await w.flush()
    }

    await writer.close()
  }

  // New process/peer opening this core without knowing blocks are compat
  const store = new Corestore(dir)
  const reader = new Bee(store)
  await reader.ready()

  t.is(reader.context.t, DEGREE_DEFAULT, 'constructed with the library default before touching any data')

  const node = await reader.get(b4a.from('k000'))
  t.alike(node.value, b4a.from('v0'), 'legacy data is still readable')

  t.is(
    reader.context.t,
    DEGREE_COMPAT,
    'reading a compat block should have auto-adapted context.t back to the historical degree'
  )

  await reader.close()
})
