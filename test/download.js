const test = require('brittle')
const b4a = require('b4a')
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

async function createRemoteTree(t) {
  const db = await create(t, { t: 4 })
  await db.ready()

  const entries = []

  for (let i = 0; i < 500; i++) {
    const key = b4a.from('k' + String(i).padStart(4, '0'))
    const value = b4a.alloc(i % 5 === 0 ? 2048 : 16, i & 0xff)
    entries.push({ key, value })
  }

  for (let i = 0; i < entries.length; i += 25) {
    const w = db.write()
    for (let j = i; j < i + 25; j++) w.tryPut(entries[j].key, entries[j].value)
    await w.flush()
  }

  const db2 = await create(t, { key: db.core.key, t: 4 })
  await db2.ready()

  const streams = await replicate(t, db, db2)

  await db2.core.update({ wait: true })
  db2.update()

  db.downloadStreams = streams

  return { db, db2, entries }
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
