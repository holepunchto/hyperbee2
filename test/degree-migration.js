const test = require('brittle')
const b4a = require('b4a')
const c = require('compact-encoding')
const Corestore = require('corestore')
const Bee = require('../')
const { create, createMultiple, replicate } = require('./helpers')
const { decodeBlock, encodeBlock, TYPE_LATEST } = require('../lib/encoding.js')
const { getEncoding } = require('../spec/hyperschema')

const DEGREE_COMPAT = 5
const DEGREE_DEFAULT = 128

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

  t.is(
    reader.context.t,
    DEGREE_DEFAULT,
    'constructed with the library default before touching any data'
  )

  const node = await reader.get(b4a.from('k000'))
  t.alike(node.value, b4a.from('v0'), 'legacy data is still readable')

  t.is(
    reader.context.t,
    DEGREE_COMPAT,
    'reading a compat block should have auto-adapted context.t back to the historical degree'
  )

  await reader.close()
})

test('opening a compat core will update degree before writing', async function (t) {
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

  // Reopening this core without knowing blocks are compat
  const store = new Corestore(dir)
  const reopendWriter = new Bee(store)
  await reopendWriter.ready()

  t.is(
    reopendWriter.context.t,
    DEGREE_DEFAULT,
    'constructed with the library default before touching any data'
  )

  {
    const w = reopendWriter.write()
    w.tryPut(b4a.from('foo'), b4a.from('bar'))
    await w.flush()
  }

  t.is(
    reopendWriter.context.t,
    DEGREE_COMPAT,
    'auto-adapted context.t back to the historical degree on write'
  )

  await reopendWriter.close()
})

test('persist an auto-detected compat degree', async function (t) {
  const writer = await create(t, { t: DEGREE_COMPAT })

  const countMetadataWithDegree = async (db) => {
    let count = 0
    const localCore = await db.context.getLocalContext().core
    for (let i = 0; i < localCore.length; i++) {
      const buffer = await localCore.get(i)
      const block = decodeBlock(buffer, i)
      if (block.metadata && block.metadata.degree === DEGREE_COMPAT) {
        count++
      }
    }
    return count
  }

  for (let i = 0; i < 40; i++) {
    const w = writer.write({ compat: true })
    w.tryPut(b4a.from('k' + String(i).padStart(3, '0')), b4a.from('v' + i))
    await w.flush()
  }

  // second reader: default t, reads compat data setting t = 5, then writes
  // degree into a checkpoint.
  const migrator = await create(t)
  await migrator.ready()

  replicate(t, writer, migrator)

  {
    const head = writer.head()
    console.log('head', head)
    migrator.move(head)

    await migrator.get(b4a.from('k000'))
    t.is(migrator.context.t, DEGREE_COMPAT, 'auto-adapted in memory')

    const w = migrator.write()
    w.tryPut(b4a.from('k999'), b4a.from('v999'))
    await w.flush()
    t.is(migrator.context.t, DEGREE_COMPAT, 'still set after write')

    t.is(await countMetadataWithDegree(migrator), 1, 'the migration write persisted degree=5 to a checkpoint on disk')
  }

  // third reader: default t, never derives the degree from compat data
  // it comes from the checkpoint the migrator wrote above.
  const third = await create(t)
  await third.ready()

  replicate(t, migrator, third)

  t.is(
    third.context.t,
    DEGREE_DEFAULT,
    'constructed with the library default before touching any data'
  )

  third.move(migrator.head())

  const node = await third.get(b4a.from('k999'))
  t.alike(node.value, b4a.from('v999'), 'the migrator write is readable')

  t.is(
    third.context.t,
    DEGREE_COMPAT,
    'a fresh reader should get the correct degree from the persisted checkpoint'
  )

  const w = third.write()
  w.tryPut(b4a.from('k998'), b4a.from('v998'))
  await w.flush()

  t.is(
    third.context.t,
    DEGREE_COMPAT,
    'a fresh reader should get the correct degree from the persisted checkpoint'
  )
  t.is(await countMetadataWithDegree(third), 1, 'a fresh reader write persisted degree=5 to a checkpoint on disk')
})

// TODO Decide if we should set degree for all contexts via getBlock
test('peer doesnt read block befor writing', async function (t) {
  const writer = await create(t, { t: DEGREE_COMPAT })

  for (let i = 0; i < 40; i++) {
    const w = writer.write({ compat: true })
    w.tryPut(b4a.from('k' + String(i).padStart(3, '0')), b4a.from('v' + i))
    await w.flush()
  }

  const migrator = await create(t)
  await migrator.ready()

  replicate(t, writer, migrator)

  migrator.move(writer.head())

  {
    // await migrator.get(b4a.from('k000')) // Uncomment to cause migrator to have a compat degree
    const w = migrator.write()
    w.tryPut(b4a.from('k999'), b4a.from('v999'))
    await w.flush()

    t.is(migrator.context.getLocalContext().t, DEGREE_COMPAT, 'checkpoint carries configured degree')
  }
})

test('custom t degree persists across reopen (single core)', async function (t) {
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

    t.is(db.context.t, 4, 'context is correct')
    await db.close()
    await store.close()
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

test('write checkpoint w/ degree on context change only', async function (t) {
  const db = await create(t, { t: 4 })

  const countMetadataWithDegree = async (db) => {
    let count = 0
    for (let seq = 0; seq < db.core.length; seq++) {
      const buffer = await db.core.get(seq)
      const block = decodeBlock(buffer, seq)
      if (block.metadata && block.metadata.degree) count++
    }
    return count
  }

  {
    const w = db.write()
    w.tryPut(b4a.from('hello'), b4a.from('world'))
    await w.flush()
  }

  const db2 = await create(t, { t: 4 })
  replicate(t, db, db2)
  {
    const w = db2.write(db.head())
    w.tryPut(b4a.from('hello'), b4a.from('people'))
    await w.flush()
  }

  t.is(await countMetadataWithDegree(db2), 1, 'first write persists the degree once')

  // several more writes on the same, now-consistent context - none of these
  // should re-emit a metadata block just because a write happened
  for (let i = 0; i < 10; i++) {
    const w = db.write()
    w.tryPut(b4a.from('k' + i), b4a.from('v' + i))
    await w.flush()
  }

  t.is(
    await countMetadataWithDegree(db2),
    1,
    'subsequent writes on a consistent context should not re-emit metadata'
  )

  await db.close()
})

test('degree persists across peers once a multi-core checkpoint has been written', async function (t) {
  const [a, b] = await createMultiple(t, 2, { t: 7 })
  await a.ready()
  await b.ready()

  // alternate writers targeting each other's core so a cross-core
  // reference gets recorded, which flips context.changed = true
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

test('braiding cores w/ different degrees keeps their respective degrees', async function (t) {
  const [a, b] = await createMultiple(t, 2, { t: 7 })
  await a.ready()
  await b.ready()

  b.setDegree(128) // Makes B different

  // alternate writers targeting each other's core so a cross-core
  // reference gets recorded, which flips context.changed = true
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

  let countMetadataWDegree = 0
  for (let seq = 0; seq < b.core.length; seq++) {
    const buffer = await b.core.get(seq)
    const block = decodeBlock(buffer, seq)
    if (block.metadata && block.metadata.degree) countMetadataWDegree++
  }

  t.is(
    countMetadataWDegree,
    2,
    'two checkpoints carrying the degree should have been written to core b'
  )
  t.is(a.context.t, 7, 'a still has t = 7')
  t.is(b.context.t, 128, 'updated to 128')
})

test('metadata without optional degree decodes as degree=0', function (t) {
  const metadataEncoding = getEncoding('@bee/metadata')

  // hand encode the old wire format, just the cores array mirrors what the
  // previous schema produced.
  const coreEncoding = getEncoding('@bee/core')

  // encode the array of cores directly
  const buf = c.encode(c.array(coreEncoding), [])

  const decoded = c.decode(metadataEncoding, buf)

  t.alike(decoded.cores, [])
  t.is(decoded.degree, 0, 'missing flags byte should decode as no persisted degree')
})

test('metadata with degree round trips (en/de)codeBlock', function (t) {
  const block = {
    type: TYPE_LATEST,
    checkpoint: 0,
    batch: { start: 0, end: 0 },
    previous: null,
    metadata: { cores: [], degree: 42 },
    tree: [],
    keys: [],
    values: [],
    cohorts: []
  }

  const buffer = encodeBlock(block)
  const decoded = decodeBlock(buffer, 0)

  t.is(decoded.metadata.degree, 42)
})
