const test = require('brittle')
const b4a = require('b4a')
const { create } = require('./helpers')

test('setDegree updates t/minKeys/maxKeys', async function (t) {
  const db = await create(t, { t: 4 })
  await db.ready()

  t.is(db.context.t, 4)
  t.is(db.context.minKeys, 3)
  t.is(db.context.maxKeys, 7)

  db.setDegree(16)

  t.is(db.context.t, 16)
  t.is(db.context.minKeys, 15)
  t.is(db.context.maxKeys, 31)
})

test('setDegree validates input', async function (t) {
  const db = await create(t, { t: 4 })
  await db.ready()

  t.exception(() => db.setDegree(1), /t must be an integer/)
  t.exception(() => db.setDegree(1.5), /t must be an integer/)
  t.exception(() => db.setDegree('4'), /t must be an integer/)

  t.is(db.context.t, 4, 'unchanged after rejected updates')
})

test('setDegree throws while a write is in progress', async function (t) {
  const db = await create(t, { t: 4 })
  await db.ready()

  const w = db.write()
  w.tryPut(b4a.from('a'), b4a.from('1'))
  const flushed = w.flush()

  t.exception(() => db.setDegree(16), /Cannot change degree while a write is in progress/)

  await flushed

  db.setDegree(16)
  t.is(db.context.t, 16)
})

// Skipping because its more of an example than a behaviour we want to enforce
test.skip("setDegree via a snapshot changes the live tree's degree stats for that context", async function (t) {
  const db = await create(t, { t: 8 }) // maxKeys = 15
  await db.ready()

  const total = 300
  for (let i = 0; i < total; i++) {
    const w = db.write()
    w.tryPut(b4a.from('k' + String(i).padStart(4, '0')), b4a.from('v' + i))
    await w.flush()
  }

  // Caller only holds a read-only snapshot, it cannot reorganize the nodes
  // Not required but shows that no writes happen.
  const snap = db.snapshot()
  t.absent(snap.writable, 'snapshot is read-only')
  t.is(snap.context, db.context, 'snapshot shares the live context object')

  snap.setDegree(2)
  t.is(db.context.maxKeys, 3, 'new degree on snapshot recalculates maxKey on db')
})

test('setDegree, old and new degree nodes coexist correctly', async function (t) {
  const db = await create(t, { t: 2 })
  await db.ready()

  const total = 200

  for (let i = 0; i < total; i++) {
    const w = db.write()
    w.tryPut(b4a.from('k' + i), b4a.from('v' + i))
    await w.flush()
  }

  db.setDegree(32)

  for (let i = total; i < total * 2; i++) {
    const w = db.write()
    w.tryPut(b4a.from('k' + i), b4a.from('v' + i))
    await w.flush()
  }

  for (let i = 0; i < total * 2; i++) {
    const node = await db.get(b4a.from('k' + i))
    t.ok(node, 'key ' + i + ' exists')
    t.alike(node.value, b4a.from('v' + i))
  }

  // delete a range that spans keys written under both the old and new degree,
  // forcing rebalances against the new minKeys on nodes built under the old one
  for (let i = 50; i < total + 50; i++) {
    const w = db.write()
    w.tryDelete(b4a.from('k' + i))
    await w.flush()
  }

  for (let i = 0; i < total * 2; i++) {
    const node = await db.get(b4a.from('k' + i))
    const shouldExist = i < 50 || i >= total + 50

    if (shouldExist) {
      t.ok(node, 'key ' + i + ' still exists')
      t.alike(node.value, b4a.from('v' + i))
    } else {
      t.absent(node, 'key ' + i + ' was deleted')
    }
  }
})

test('setDegree is picked up by a fresh read stream', async function (t) {
  const db = await create(t, { t: 2 })
  await db.ready()

  for (let i = 0; i < 50; i++) {
    const w = db.write()
    w.tryPut(b4a.from('k' + i), b4a.from('v' + i))
    await w.flush()
  }

  db.setDegree(8)

  const entries = []
  for await (const entry of db.createReadStream()) entries.push(entry)

  t.is(entries.length, 50)
})
