const test = require('brittle')
const b4a = require('b4a')
const { create, replicate } = require('./helpers')
const { RangeIterator } = require('../lib/ranges.js')

test('range prefetch adapts to trees with small nodes', async function (t) {
  const db2 = await createRemoteSkinnyTree(t)

  const it = new RangeIterator(db2, { limit: 20 })
  await it.open()

  const first = await it.next()
  t.ok(first, 'got first entry')

  const inflated = await waitForSiblings(it)
  t.ok(inflated, 'sibling leaves were prefetched')
})

test('unbounded range streams prefetch too', async function (t) {
  const db2 = await createRemoteSkinnyTree(t)

  const it = new RangeIterator(db2, {})
  await it.open()

  const first = await it.next()
  t.ok(first, 'got first entry')

  const inflated = await waitForSiblings(it)
  t.ok(inflated, 'sibling leaves were prefetched')
})

test('reverse range prefetch fetches the preceding siblings', async function (t) {
  const db2 = await createRemoteSkinnyTree(t)

  const it = new RangeIterator(db2, { lte: b4a.from('k0056'), reverse: true, limit: 20 })
  await it.open()

  const first = await it.next()
  t.ok(first, 'got first entry')

  const leaf = it.stack[it.stack.length - 1]
  const parent = it.stack[it.stack.length - 2]
  const children = parent.node.value.children

  let index = -1
  for (let j = 0; j < children.length; j++) {
    if (children.get(j) === leaf.node) index = j
  }

  t.ok(index > 0, 'sanity: leaf has preceding siblings')
  t.ok(index < children.length - 1, 'sanity: leaf has siblings past the range')

  let inflated = false
  for (let i = 0; i < 100 && !inflated; i++) {
    inflated = true
    for (let j = 0; j < index; j++) {
      if (!children.get(j).value) inflated = false
    }
    if (!inflated) await new Promise((resolve) => setTimeout(resolve, 10))
  }

  t.ok(inflated, 'preceding siblings were prefetched')

  for (let j = index + 1; j < children.length; j++) {
    t.absent(children.get(j).value, 'sibling past the range was not fetched')
  }
})

test('no prefetch when the current leaf covers the limit', async function (t) {
  const db2 = await createRemoteSkinnyTree(t)

  const it = new RangeIterator(db2, { limit: 1 })
  await it.open()

  const first = await it.next()
  t.ok(first, 'got first entry')

  t.is(it.prefetching, null, 'prefetch was skipped')
})

// a legacy-shaped tree: small degree, one write per key so the nodes spread
// over many blocks, read by a second peer at the default degree
async function createRemoteSkinnyTree(t) {
  const db = await create(t, { t: 2 })
  await db.ready()

  for (let i = 0; i < 60; i++) {
    const w = db.write()
    w.tryPut(b4a.from('k' + String(i).padStart(4, '0')), b4a.from('v' + i))
    await w.flush()
  }

  const db2 = await create(t, { key: db.core.key })
  await db2.ready()

  replicate(t, db, db2)

  await db2.core.update({ wait: true })
  db2.update()

  return db2
}

// after emitting the first key, the remaining children of the leaf's parent
// should inflate in the background without the iterator visiting them
async function waitForSiblings(it) {
  const parent = it.stack[it.stack.length - 2]
  const children = parent.node.value.children

  for (let i = 0; i < 100; i++) {
    let inflated = 0
    for (let j = 0; j < children.length; j++) {
      if (children.get(j).value) inflated++
    }
    if (inflated === children.length) return true
    await new Promise((resolve) => setTimeout(resolve, 10))
  }

  return false
}
