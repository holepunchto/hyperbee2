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

async function waitFor(cond, tries = 200) {
  for (let i = 0; i < tries; i++) {
    if (cond()) return true
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  return false
}

test('destroying one RangeStream cancels an unrelated RangeStream that shared its inflated root', async function (t) {
  const db2 = await createUnreachableReader(t)

  // Two independent readers without a shared activeRequests array.
  const a = db2.createReadStream()
  const b = db2.createReadStream()

  const aErrors = []
  const bErrors = []
  a.on('error', (err) => aErrors.push(err))
  b.on('error', (err) => bErrors.push(err))

  a.resume()
  b.resume()

  // Wait until A has actually issued its (permanently hanging) request for
  // the root block.
  const requested = await waitFor(() => a.iterator.config.activeRequests.length > 0)
  t.ok(requested, 'sanity: A issued request for root')

  t.is(b.iterator.config.activeRequests.length, 0, 'sanity: B made no request of its own')
  t.is(a.iterator.root, b.iterator.root, 'A & B share the same root pointer')
  t.ok(a.iterator.root.inflating, 'the shared root is mid-inflate')

  // TODO adjust when fixed as B shouldn't close if it doesn't get the error
  // Register both close waiters before destroying: B's cancellation cascades
  // off the same rejected promise as A's
  const aClosed = new Promise((resolve) => a.once('close', resolve))
  const bClosed = new Promise((resolve) => b.once('close', resolve))

  // The consumer of `A` decides it no longer wants it - Shouldn't affect stream `B`.
  a.destroy()

  await aClosed
  await bClosed

  t.is(aErrors.length, 0, 'destroying A is a clean cancel for A itself')

  // B was never destroyed and never asked to stop, yet
  // destroying a cancels the shared inflate() promise b was also awaiting,
  // and that REQUEST_CANCELLED surfaces as B's own stream error via b's
  // _read callback.
  t.is(bErrors.length, 0, 'B was never asked to stop and should not error')
})
