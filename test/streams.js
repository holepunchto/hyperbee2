const test = require('brittle')
const b4a = require('b4a')
const { create } = require('./helpers')

// Builds a reader that knows the writer's tree shape/length (so it can
// bootstrap a root and walk the tree) but can never actually fetch block
// data for it as the replication link is torn down right after the length is
// learned.
async function createUnreachableReader(t) {
  const db = await create(t, { t: 2 })
  await db.ready()

  for (let i = 0; i < 20; i++) {
    const w = db.write()
    w.tryPut(b4a.from('k' + String(i).padStart(4, '0')), b4a.from('v' + i))
    await w.flush()
  }

  const db2 = await create(t, { key: db.core.key, t: 2 })
  await db2.ready()

  const s1 = db.replicate(true)
  const s2 = db2.replicate(false)
  s1.pipe(s2).pipe(s1)
  s1.on('error', () => {})
  s2.on('error', () => {})

  await db2.core.update({ wait: true })
  db2.update()

  s1.destroy()
  s2.destroy()

  return db2
}

test('close open streams when Hyperbee instance closed', async function (t) {
  const db = await createUnreachableReader(t)
  const db2 = await createUnreachableReader(t)

  const s1 = db.createReadStream()
  const s2 = db.createDiffStream(db2)
  const s3 = db.createDiffStream(db2)
  const s4 = db.createChangesStream()

  t.is(db.streams.length, 4)

  t.is(s1.destroyed, false)
  t.is(s2.destroyed, false)
  t.is(s3.destroyed, false)
  t.is(s4.destroyed, false)

  // destroy one manually (it should be removed from Hyperbee instance)
  s2.destroy()
  await new Promise((resolve) => s2.once('close', resolve))
  t.is(db.streams.length, 3)

  t.is(s1.destroyed, false)
  t.is(s2.destroyed, true)
  t.is(s3.destroyed, false)
  t.is(s4.destroyed, false)

  // Ignore SESSION_CLOSED error on DiffStream.
  // TODO: should this be squashed by the stream once destory() has been called on it?
  s3.on('error', (err) => t.is(err.code, 'SESSION_CLOSED'))

  // Closing db should close remaining streams
  await db.close()
  t.is(db.streams.length, 0)

  t.is(s1.destroyed, true)
  t.is(s2.destroyed, true)
  t.is(s3.destroyed, true)
  t.is(s4.destroyed, true)

  await db2.close()
})
