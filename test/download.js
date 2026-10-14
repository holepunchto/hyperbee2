const test = require('brittle')
const b4a = require('b4a')
const c = require('compact-encoding')
const Bee = require('../')
const { getEncoding, getEnum } = require('../spec/hyperschema')
const { create, replicate } = require('./helpers')

test('download fetches the whole tree', async function (t) {
  const { db, db2, entries } = await createRemoteTree(t)

  await db2.download()

  await disconnect(t, db, db2)

  const got = await collect(db2.createReadStream({ wait: false }))
  t.is(got.length, entries.length)

  for (let i = 0; i < entries.length; i++) {
    t.alike(got[i].key, entries[i].key)
    t.alike(got[i].value, entries[i].value)
  }
})

test('download with parallelism 1', async function (t) {
  const { db, db2, entries } = await createRemoteTree(t)

  await db2.download({ parallelism: 1 })

  await disconnect(t, db, db2)

  const got = await collect(db2.createReadStream({ wait: false }))
  t.is(got.length, entries.length)
})

test('download a range', async function (t) {
  const { db, db2, entries } = await createRemoteTree(t)

  const gte = b4a.from('k0100')
  const lt = b4a.from('k0200')

  await db2.download({ gte, lt })

  await disconnect(t, db, db2)

  const got = await collect(db2.createReadStream({ gte, lt, wait: false }))
  const expected = entries.filter((e) => b4a.compare(e.key, gte) >= 0 && b4a.compare(e.key, lt) < 0)

  t.is(got.length, expected.length)
  t.alike(got[0].key, expected[0].key)
  t.alike(got[got.length - 1].key, expected[expected.length - 1].key)

  let has = 0
  for (let i = 0; i < db2.core.length; i++) {
    if (await db2.core.has(i)) has++
  }

  t.ok(has < db2.core.length, 'blocks outside the range were not fetched')
})

test('download a range with exclusive and inclusive bounds', async function (t) {
  const { db, db2 } = await createRemoteTree(t)

  const gt = b4a.from('k0050')
  const lte = b4a.from('k0060')

  await db2.download({ gt, lte })

  await disconnect(t, db, db2)

  const got = await collect(db2.createReadStream({ gt, lte, wait: false }))
  t.is(got.length, 10)
  t.alike(got[0].key, b4a.from('k0051'))
  t.alike(got[9].key, b4a.from('k0060'))
})

test('download of an empty tree', async function (t) {
  const db = await create(t)
  await db.download()
  t.pass('resolved')
})

test('download rejects when blocks are not available', async function (t) {
  const { db, db2 } = await createRemoteTree(t)

  await disconnect(t, db, db2)

  try {
    await db2.download({ wait: false })
    t.fail('should have failed')
  } catch (err) {
    t.is(err.code, 'BLOCK_NOT_AVAILABLE')
  }
})

test('full download marks the root', async function (t) {
  const { db, db2 } = await createRemoteTree(t)

  await db2.download()
  await disconnect(t, db, db2)

  t.ok(await isDownloaded(db2.root))
})

test('ranged download does not mark the root', async function (t) {
  const { db, db2 } = await createRemoteTree(t)

  await db2.download({ gte: b4a.from('k0100'), lt: b4a.from('k0200') })
  await disconnect(t, db, db2)

  t.is(await isDownloaded(db2.root), false)
})

test('download skips subtrees marked by an earlier download', async function (t) {
  const { db, db2, entries } = await createRemoteTree(t)

  await db2.download()
  const length = db2.core.length

  await appendMore(db, entries)
  await db2.core.update({ wait: true })

  const db3 = await reopen(t, db, db2)
  const seqs = traceInflate(db3)

  await db3.download()
  await disconnect(t, db, db2)

  t.ok(seqs.length > 0, 'inflated the new nodes')
  t.ok(
    seqs.every((seq) => seq >= length),
    'did not inflate nodes from the earlier download'
  )
  t.ok(await isDownloaded(db3.root))

  const got = await collect(db3.createReadStream({ wait: false }))
  t.is(got.length, entries.length)
})

test('download walks subtrees a ranged download did not cover', async function (t) {
  const { db, db2, entries } = await createRemoteTree(t)

  await db2.download({ gte: b4a.from('k0100'), lt: b4a.from('k0200') })
  const length = db2.core.length

  await appendMore(db, entries)
  await db2.core.update({ wait: true })

  const db3 = await reopen(t, db, db2)
  const seqs = traceInflate(db3)

  await db3.download()
  await disconnect(t, db, db2)

  t.ok(
    seqs.some((seq) => seq < length),
    'inflated nodes from before the ranged download'
  )
})

test('download of an older checkout skips subtrees downloaded earlier', async function (t) {
  // same tree twice, once without and once with a full download first
  const a = await createRemoteTree(t)
  const b = await createRemoteTree(t)
  const length = a.lengths[a.lengths.length - 3]

  const unmarked = await downloadCheckout(t, a, length)

  await b.db2.download()
  const seqs = await downloadCheckout(t, b, length)

  await disconnect(t, a.db, a.db2)
  await disconnect(t, b.db, b.db2)

  t.ok(seqs.length > 0, 'walked the nodes the newer tree had rewritten')
  t.ok(seqs.length < unmarked.length, 'skipped the subtrees the newer tree shares')
})

test('download of a marked checkout walks nothing', async function (t) {
  const { db, db2, lengths } = await createRemoteTree(t)
  const length = lengths[lengths.length - 3]

  await db2.checkout({ length }).download()

  const db3 = await reopen(t, db, db2)
  const old = db3.checkout({ length })
  const seqs = traceInflate(old)
  await old.download()

  await disconnect(t, db, db2)

  t.is(seqs.length, 0)
})

test('markers from a failed download are kept', async function (t) {
  const { db, db2, entries } = await createRemoteTree(t)

  await db2.download({ gte: b4a.from('k0100'), lt: b4a.from('k0200') })
  await disconnect(t, db, db2)

  try {
    await db2.download({ wait: false })
    t.fail('should have failed')
  } catch (err) {
    t.is(err.code, 'BLOCK_NOT_AVAILABLE')
  }

  const got = await collect(
    db2.createReadStream({ gte: b4a.from('k0100'), lt: b4a.from('k0200'), wait: false })
  )
  t.is(got.length, entries.filter((e) => e.key >= 'k0100' && e.key < 'k0200').length)
})

async function isDownloaded(ptr) {
  const core = ptr.context.getCore(ptr.core)
  await core.ready()

  const key = c.encode(getEncoding('@bee/downloaded-key'), {
    type: getEnum('@bee/local-key-type').DOWNLOADED,
    seq: ptr.seq,
    offset: ptr.offset
  })

  const rx = core.core.state.storage.read()
  const p = rx.getLocal(key)
  rx.tryFlush()
  return (await p) !== null
}

async function appendMore(db, entries) {
  const w = db.write()
  for (let i = 0; i < 25; i++) {
    const key = b4a.from('k' + String(entries.length).padStart(4, '0'))
    const value = b4a.alloc(16, entries.length & 0xff)
    entries.push({ key, value })
    w.tryPut(key, value)
  }
  await w.flush()
}

// download a checkout from a fresh instance and return the node seqs it inflated
async function downloadCheckout(t, { db, db2 }, length) {
  const cold = await reopen(t, db, db2)
  const view = cold.checkout({ length })
  const seqs = traceInflate(view)
  await view.download()
  t.ok(await isDownloaded(view.root))
  return seqs
}

// fresh instance on the same store, so nothing is cached in memory
async function reopen(t, db, db2) {
  const db3 = new Bee(db2.store, { key: db.core.key, t: 4 })
  t.teardown(() => db3.close())
  await db3.ready()
  db3.update()
  return db3
}

function traceInflate(db) {
  const seqs = []
  const inflate = db.inflate.bind(db)
  db.inflate = function (ptr, config) {
    seqs.push(ptr.seq)
    return inflate(ptr, config)
  }
  return seqs
}

async function createRemoteTree(t) {
  const db = await create(t, { t: 4 })
  await db.ready()

  const entries = []

  for (let i = 0; i < 500; i++) {
    const key = b4a.from('k' + String(i).padStart(4, '0'))
    const value = b4a.alloc(i % 5 === 0 ? 2048 : 16, i & 0xff)
    entries.push({ key, value })
  }

  const lengths = []

  for (let i = 0; i < entries.length; i += 25) {
    const w = db.write()
    for (let j = i; j < i + 25; j++) w.tryPut(entries[j].key, entries[j].value)
    await w.flush()
    lengths.push(db.core.length)
  }

  const db2 = await create(t, { key: db.core.key, t: 4 })
  await db2.ready()

  const streams = await replicate(t, db, db2)

  await db2.core.update({ wait: true })
  db2.update()

  db.downloadStreams = streams

  return { db, db2, entries, lengths }
}

async function disconnect(t, db, db2) {
  const { s1, s2 } = db.downloadStreams
  s1.destroy()
  s2.destroy()
  await new Promise((resolve) => setTimeout(resolve, 50))
}

async function collect(stream) {
  const result = []
  for await (const data of stream) result.push(data)
  return result
}
