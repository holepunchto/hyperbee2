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

test('destroying one RangeStream does not cancel an unrelated RangeStream reading the same tree node', async function (t) {
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

  // Wait until both A and B have issued their own (permanently hanging)
  // request for the root block.
  const aRequested = await waitFor(() => a.iterator.config.activeRequests.length > 0)
  const bRequested = await waitFor(() => b.iterator.config.activeRequests.length > 0)
  t.ok(aRequested, 'sanity: A issued its own request for root')
  t.ok(bRequested, 'sanity: B issued its own request for root')

  // They still read through the very same cached tree node - inflate() just
  // no longer makes B's fetch depend on A's in-flight promise to get there.
  t.is(a.iterator.root, b.iterator.root, 'A & B share the same root pointer')

  const aClosed = new Promise((resolve) => a.once('close', resolve))

  // The consumer of `A` decides it no longer wants it - shouldn't affect
  // stream `B` in any way.
  a.destroy()

  await aClosed

  t.is(aErrors.length, 0, 'destroying A is a clean cancel for A itself')

  // Give B plenty of time to have reacted, if it were going to.
  await new Promise((resolve) => setTimeout(resolve, 50))

  // B was never destroyed and never asked to stop, and destroying A no
  // longer has any effect on it: B is still alive, still waiting on its own
  // independent (and still hanging) request.
  t.is(bErrors.length, 0, 'B was never asked to stop and does not error')
  t.absent(b.destroyed, 'B is still alive')
  t.ok(b.iterator.config.activeRequests.length > 0, 'B is still waiting on its own request')

  b.destroy()
  await new Promise((resolve) => b.once('close', resolve))
})
