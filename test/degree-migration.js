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
    t.is(migrator.context.getLocalContext().t, DEGREE_COMPAT, 'auto-adapted in memory')

    const w = migrator.write()
    w.tryPut(b4a.from('k999'), b4a.from('v999'))
    await w.flush()
    t.is(migrator.context.getLocalContext().t, DEGREE_COMPAT, 'still set after write')

    t.is(
      await countMetadataWithDegree(migrator, DEGREE_COMPAT),
      1,
      'the migration write persisted degree=5 to a checkpoint on disk'
    )
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
  t.is(
    await countMetadataWithDegree(third, DEGREE_COMPAT),
    1,
    'a fresh reader write persisted degree=5 to a checkpoint on disk'
  )
})

test('take degree from tree that you move to', async function (t) {
  const writer = await create(t, { t: DEGREE_COMPAT })

  for (let i = 0; i < 40; i++) {
    const w = writer.write({ compat: true })
    w.tryPut(b4a.from('k' + String(i).padStart(3, '0')), b4a.from('v' + i))
    await w.flush()
  }

  // second peer: default t, writes with t = 128 (no checkpoint), then moves to
  // 1st peer head loads the degree from their tree and writes degree into a
  // checkpoint.
  const migrator = await create(t)
  await migrator.ready()

  replicate(t, writer, migrator)

  {
    const w = migrator.write()
    w.tryPut(b4a.from('k999'), b4a.from('v999'))
    await w.flush()
  }
  t.is(migrator.context.getLocalContext().t, 128, 'starts with default degree')

  const head = writer.head()
  migrator.move(head)

  await migrator.get(b4a.from('k000'))
  t.is(migrator.context.getLocalContext().t, DEGREE_COMPAT, 'auto-adapted in memory')

  t.is(await migrator.get(b4a.from('k999')), null, 'no longer have local writes after move')

  {
    const w = migrator.write()
    w.tryPut(b4a.from('k999'), b4a.from('v999'))
    await w.flush()
    t.is(migrator.context.getLocalContext().t, DEGREE_COMPAT, 'writes now with degree from head')
  }

  t.is(
    await countMetadataWithDegree(migrator, DEGREE_COMPAT),
    1,
    'the migration write persisted degree=128 to a checkpoint on disk'
  )
})

test('peer doesnt read block before writing', async function (t) {
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
    const w = migrator.write()
    w.tryPut(b4a.from('k999'), b4a.from('v999'))
    // Should read a compat block when inflating tree to write, since entire
    // tree is compat, it will always hit one.
    await w.flush()

    t.is(migrator.context.getLocalContext().t, DEGREE_COMPAT, 'context carries configured degree')
    t.is(
      await countMetadataWithDegree(migrator, DEGREE_COMPAT),
      1,
      'checkpoint block carries configured degree'
    )
  }
})

// Skipped because it is caused by improper configuration.
// Aka you should open your hyperbee2 with the same custom t, if the t is
// changed by the remote, that will be detected as part of the checkpoint block.
test.skip('custom t degree persists across reopen (single core)', async function (t) {
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

test('checkout doesnt change live degree', async function (t) {
  const [writer, other] = await createMultiple(t, 2)

  for (let i = 0; i < 40; i++) {
    const w = writer.write()
    w.tryPut(b4a.from('k' + String(i).padStart(3, '0')), b4a.from('v' + i))
    await w.flush()
  }

  other.move(writer.head())

  {
    const w = other.write()
    w.tryPut(b4a.from('other0'), b4a.from('v0'))
    await w.flush()
  }

  t.is(await countMetadataWithDegree(other, 128), 1, 'checkpoint written by other w/ degree')

  // second peer: compat t, writes with t = 5 (no checkpoint), then moves to
  // 1st peer head loads the degree from their tree and writes degree into a
  // checkpoint.
  const migrator = await create(t, { t: DEGREE_COMPAT })
  await migrator.ready()

  replicate(t, other, migrator)

  {
    const w = migrator.write()
    w.tryPut(b4a.from('k999'), b4a.from('v999'))
    await w.flush()
  }
  t.is(migrator.context.getLocalContext().t, DEGREE_COMPAT, 'starts with compat degree')

  const oldHead = migrator.head()

  const head = other.head()
  migrator.move(head)

  await migrator.get(b4a.from('k000'))
  t.is(migrator.context.getLocalContext().t, 128, 'auto-adapted in memory')

  t.is(await migrator.get(b4a.from('k999')), null, 'no longer have local writes after move')

  {
    const w = migrator.write()
    w.tryPut(b4a.from('k999'), b4a.from('v999'))
    await w.flush()
    t.is(migrator.context.getLocalContext().t, 128, 'writes now with degree from head')
  }

  t.is(
    await countMetadataWithDegree(migrator, 128),
    1,
    'the migration write persisted degree=128 to a checkpoint on disk'
  )

  const checkout = migrator.checkout(oldHead)

  t.alike(
    (await checkout.get(b4a.from('k999'))).value,
    b4a.from('v999'),
    'can get value from checkout'
  )
  t.is(migrator.context.getLocalContext().t, DEGREE_COMPAT, 'non-checkout context doesnt change')

  {
    const w = migrator.write()
    w.tryPut(b4a.from('k998'), b4a.from('v998'))
    await w.flush()
    t.is(migrator.context.getLocalContext().t, 128, 'writes now with degree from head')
  }
})

test('write checkpoint w/ degree on context change only', async function (t) {
  const db = await create(t, { t: 4 })

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

  t.is(await countMetadataWithDegree(db2, 4), 1, 'first write persists the degree once')

  // several more writes on the same, now-consistent context - none of these
  // should re-emit a metadata block just because a write happened
  for (let i = 0; i < 10; i++) {
    const w = db.write()
    w.tryPut(b4a.from('k' + i), b4a.from('v' + i))
    await w.flush()
  }

  t.is(
    await countMetadataWithDegree(db2, 4),
    1,
    'subsequent writes on a consistent context should not re-emit metadata'
  )

  await db.close()
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

async function countMetadataWithDegree(db, degree) {
  let count = 0
  const localCore = await db.context.getLocalContext().core
  for (let i = 0; i < localCore.length; i++) {
    const buffer = await localCore.get(i)
    const block = decodeBlock(buffer, i)
    if (block.metadata && block.metadata.degree === degree) {
      count++
    }
  }
  return count
}
