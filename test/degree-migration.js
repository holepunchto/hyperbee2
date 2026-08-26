const test = require('brittle')
const b4a = require('b4a')
const Corestore = require('corestore')
const Bee = require('../')
const { create, createMultiple } = require('./helpers')
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

test('db.compat() compat block as TYPE_LATEST', async function (t) {
  const db = await create(t, { t: DEGREE_COMPAT })

  const w = db.write({ compat: true })
  w.tryPut(b4a.from('a'), b4a.from('1'))
  await w.flush()

  const detected = await db.compat()
  t.is(detected, TYPE_LATEST, 'db.compat() reports legacy data as TYPE_LATEST')
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

test('persist an auto-detected compat degree', async function (t) {
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

  // second reader: default t, reads compat data setting t = 5, then writes
  // degree into a checkpoint.
  let coreLengthBeforeMigration
  {
    const store = new Corestore(dir)
    const migrator = new Bee(store)
    await migrator.ready()

    coreLengthBeforeMigration = migrator.core.length

    await migrator.get(b4a.from('k000'))
    t.is(migrator.context.t, DEGREE_COMPAT, 'auto-adapted in memory')

    const w = migrator.write()
    w.tryPut(b4a.from('k999'), b4a.from('v999'))
    await w.flush()
    t.is(migrator.context.t, DEGREE_COMPAT, 'still set after write')

    // Prove the migration write actually persisted degree=5 to disk (aka wrote
    // after prev length)
    let sawMetadataWithDegree = false
    for (let seq = coreLengthBeforeMigration; seq < migrator.core.length; seq++) {
      const buffer = await migrator.core.get(seq)
      const block = decodeBlock(buffer, seq)
      if (block.metadata && block.metadata.degree === DEGREE_COMPAT) sawMetadataWithDegree = true
    }
    t.ok(sawMetadataWithDegree, 'the migration write persisted degree=5 to a checkpoint on disk')

    await migrator.close()
  }

  // third reader: default t, never derives the degree from compat data
  // it comes from the checkpoint the migrator wrote above.
  const store = new Corestore(dir)
  const third = new Bee(store)
  await third.ready()

  t.is(third.context.t, DEGREE_DEFAULT, 'constructed with the library default before touching any data')

  const node = await third.get(b4a.from('k999'))
  t.alike(node.value, b4a.from('v999'), 'the migrator write is readable')

  const w = third.write()
  w.tryPut(b4a.from('k998'), b4a.from('v998'))
  await w.flush()

  t.is(
    third.context.t,
    DEGREE_COMPAT,
    'a fresh reader should get the correct degree from the persisted checkpoint'
  )

  await third.close()
})

test('an explicitly constructed degree persists across reopen (single core)', async function (t) {
  const dir = await t.tmp()

  {
    const store = new Corestore(dir)
    const db = new Bee(store, { t: 4 })
    await db.ready()

    for (let i = 0; i < 50; i++) {
      const w = db.write()
      w.tryPut(b4a.from('k' + String(i).padStart(3, '0')), b4a.from('v' + i))
      await w.flush()
    }

    t.is(db.context.t, 4)
    await db.close()
  }

  // Reopened with no explicit `t`, the persisted degree should win over the
  // default
  const store = new Corestore(dir)
  const db = new Bee(store)
  await db.ready()

  const w = db.write()
  w.tryPut(b4a.from('zzz'), b4a.from('new'))
  await w.flush()

  t.is(db.context.t, 4, 'degree recorded at construction time should survive reopen')

  await db.close()
})

test('setDegree() persists across reopen (single core)', async function (t) {
  const dir = await t.tmp()

  {
    const store = new Corestore(dir)
    const db = new Bee(store, { t: 4 })
    await db.ready()

    {
      const w = db.write()
      w.tryPut(b4a.from('a'), b4a.from('1'))
      await w.flush()
    }

    db.setDegree(50)

    {
      const w = db.write()
      w.tryPut(b4a.from('b'), b4a.from('2'))
      await w.flush()
    }

    t.is(db.context.t, 50)
    await db.close()
  }

  const store = new Corestore(dir)
  const db = new Bee(store)
  await db.ready()

  const w = db.write()
  w.tryPut(b4a.from('c'), b4a.from('3'))
  await w.flush()

  t.is(db.context.t, 50, 'setDegree() should survive reopen just like the constructor t option')

  await db.close()
})

test('once the persisted degree matches, further writes do not re-emit metadata', async function (t) {
  const dir = await t.tmp()
  const store = new Corestore(dir)
  const db = new Bee(store, { t: 4 })
  await db.ready()

  const countMetadataWithDegree = async () => {
    let count = 0
    for (let seq = 0; seq < db.core.length; seq++) {
      const buffer = await db.core.get(seq)
      const block = decodeBlock(buffer, seq)
      if (block.metadata && block.metadata.degree) count++
    }
    return count
  }

  // first write on a new tree: t (4) doesn't match persistedDegree (null)
  // yet, so it should persists it
  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    await w.flush()
  }

  t.is(await countMetadataWithDegree(), 1, 'first write persists the degree once')

  // several more writes on the same, now-consistent context - none of these
  // should re-emit a metadata block just because a write happened
  for (let i = 0; i < 10; i++) {
    const w = db.write()
    w.tryPut(b4a.from('k' + i), b4a.from('v' + i))
    await w.flush()
  }

  t.is(
    await countMetadataWithDegree(),
    1,
    'subsequent writes on a consistent context should not re-emit metadata'
  )

  await db.close()
})

test('degree persists across peers once a multi-core checkpoint has been written', async function (t) {
  const [a, b] = await createMultiple(t, 2, { t: 7 })
  await a.ready()
  await b.ready()

  // alternate writers targeting each other's core so a genuine cross-core
  // reference gets recorded, which is what flips context.changed = true
  for (let i = 0; i < 30; i++) {
    const k = b4a.from('k' + i)
    if (i % 2 === 0) {
      const w = b.write({ key: a.core.key, length: a.core.length })
      w.tryPut(k, k)
      await w.flush()
    } else {
      const w = a.write({ key: b.core.key, length: b.core.length })
      w.tryPut(k, k)
      await w.flush()
    }
  }

  let sawMetadataWithDegree = false
  for (let seq = 0; seq < a.core.length; seq++) {
    const buffer = await a.core.get(seq)
    const block = decodeBlock(buffer, seq)
    if (block.metadata && block.metadata.degree) sawMetadataWithDegree = true
  }

  t.ok(sawMetadataWithDegree, 'a checkpoint carrying the degree should have been written to core a')
})
