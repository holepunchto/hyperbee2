const test = require('brittle')
const b4a = require('b4a')
const c = require('compact-encoding')
const Corestore = require('corestore')
const Bee = require('../')
const { create, createMultiple } = require('./helpers')
const {
  decodeBlock,
  encodeBlock,
  peekBlockType,
  TYPE_COMPAT,
  TYPE_LATEST
} = require('../lib/encoding.js')
const { getEncoding } = require('../spec/hyperschema')

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

  t.is(
    third.context.t,
    DEGREE_DEFAULT,
    'constructed with the library default before touching any data'
  )

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

test('setDegree() after migration is not reverted by re-reading old compat blocks', async function (t) {
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

  const store = new Corestore(dir)
  const migrator = new Bee(store)
  await migrator.ready()

  // auto-detect degree from compat data, then persist it by writing
  // a new key. This only rewrites the path for k999 - most of the tree,
  // e.g. k005, is still backed by untouched, compat-encoded blocks on disk.
  await migrator.get(b4a.from('k000'))
  const w = migrator.write()
  w.tryPut(b4a.from('k999'), b4a.from('v999'))
  await w.flush()
  t.is(migrator.context.t, DEGREE_COMPAT, 't = 5 persisted via checkpoint')

  // the user now explicitly opts into a bigger degree going forward
  migrator.setDegree(128)
  t.is(migrator.context.t, 128, 'setDegree() applies immediately')

  // reading an old key still served from a compat-encoded block should not
  // undo the explicit setDegree() call
  const node = await migrator.get(b4a.from('k005'))
  t.alike(node.value, b4a.from('v5'), 'old compat-backed key is still readable')
  t.is(
    migrator.context.t,
    128,
    'reading old compat data should not revert an explicit setDegree() override'
  )

  const w2 = migrator.write()
  w2.tryPut(b4a.from('zzz'), b4a.from('newval'))
  await w2.flush()
  t.is(migrator.context.t, 128, 'the explicit degree survives the next flush')

  await migrator.close()

  // and it should survive reopen too
  const store2 = new Corestore(dir)
  const reopened = new Bee(store2)
  await reopened.ready()

  const wr = reopened.write()
  wr.tryPut(b4a.from('another'), b4a.from('v'))
  await wr.flush()

  t.is(reopened.context.t, 128, 'the explicit degree survives reopen')

  await reopened.close()
})

test('reading a compat block sets t = 5 but updates the next write', async function (t) {
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

  // migrate to a persisted degree that is neither compat (5) nor the default (128)
  const PERSISTED_DEGREE = 50
  {
    const store = new Corestore(dir)
    const migrator = new Bee(store)
    await migrator.ready()

    await migrator.get(b4a.from('k000'))
    const w = migrator.write()
    w.tryPut(b4a.from('k999'), b4a.from('v999'))
    await w.flush()

    migrator.setDegree(PERSISTED_DEGREE)
    const w2 = migrator.write()
    w2.tryPut(b4a.from('zzz'), b4a.from('newval'))
    await w2.flush()
    t.is(migrator.context.t, PERSISTED_DEGREE, 'checkpoint now carries the migrated degree')

    await migrator.close()
  }

  // fresh reader: a plain get() never calls context.update()/_inflateCheckpoint()
  // If the traversal lands on a compat-encoded leaf (e.g. k005), it sets t=5
  // since thats all it knows.
  const store = new Corestore(dir)
  const reader = new Bee(store)
  await reader.ready()

  t.is(reader.context.persistedDegree, null, 'never synced with a checkpoint yet')

  const node = await reader.get(b4a.from('k005'))
  t.alike(node.value, b4a.from('v5'), 'old compat node value is readable')

  t.is(reader.context.t, DEGREE_COMPAT, 'compat block read sets t=5 in memory')
  t.is(
    reader.context.persistedDegree,
    null,
    'reading doesnt set persistedDegree, so this stays "unconfirmed"'
  )

  // writing calls context.update() before touching the tree, so the checkpoint
  // wins over the in-memory guess
  const w = reader.write()
  w.tryPut(b4a.from('another'), b4a.from('v'))
  await w.flush()

  t.is(reader.context.t, PERSISTED_DEGREE, 'write loads the real persisted degree')
  t.is(
    reader.context.persistedDegree,
    PERSISTED_DEGREE,
    'now persistedDegree set via the checkpoint'
  )

  await reader.close()
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
