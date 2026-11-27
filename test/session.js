const test = require('brittle')
const b4a = require('b4a')
const { create, replicate } = require('./helpers')

async function put(db, key, value) {
  const w = db.write()
  w.tryPut(b4a.from(key), b4a.from(value))
  await w.flush()
}

async function get(db, key) {
  const node = await db.get(b4a.from(key))
  return node && b4a.toString(node.value)
}

test('session starts at the same root and inherits writable', async function (t) {
  const db = await create(t)
  await put(db, 'a', '1')

  const s = db.session()

  t.is(s.writable, true)
  t.alike(s.head(), db.head())
  t.is(await get(s, 'a'), '1')

  const r = db.session({ writable: false })
  t.is(r.writable, false)
  t.exception(() => r.write())

  await s.close()
  await r.close()
})

test('session move does not affect the parent', async function (t) {
  const db = await create(t)
  await put(db, 'a', '1')
  await put(db, 'a', '2')

  const s = db.session()
  s.move({ length: db.head().length - 1 })

  t.is(await get(s, 'a'), '1')
  t.is(await get(db, 'a'), '2')

  db.move({ length: 0 })
  t.is(await get(s, 'a'), '1')

  await s.close()
})

test('session write only updates the session', async function (t) {
  const db = await create(t)
  await put(db, 'a', '1')

  const s = db.session()
  await put(s, 'b', '2')

  t.is(await get(s, 'b'), '2')
  t.is(await get(db, 'b'), null)

  await s.close()
})

test('closing a session leaves the parent open', async function (t) {
  const db = await create(t)
  await put(db, 'a', '1')

  const s = db.session()
  await s.close()

  t.is(db.closed, false)
  t.is(await get(db, 'a'), '1')
  await put(db, 'b', '2')
  t.is(await get(db, 'b'), '2')
})

test('session of a reader follows appends until closed', async function (t) {
  const db = await create(t)
  await put(db, 'a', '1')

  const reader = await create(t, { key: db.core.key, writable: false })
  replicate(t, db, reader)
  await reader.ready()
  await reader.core.update({ wait: true })

  const before = reader.core.listenerCount('append')

  const s = reader.session()
  t.is(s.writable, false)
  t.is(reader.core.listenerCount('append'), before + 1)

  const updated = new Promise((resolve) => s.once('update', resolve))
  await put(db, 'b', '2')
  await updated

  t.is(await get(s, 'b'), '2')

  await s.close()
  t.is(reader.core.listenerCount('append'), before)
})

test('session of a snapshot stays pinned', async function (t) {
  const db = await create(t)
  await put(db, 'a', '1')

  const snap = db.snapshot()
  const s = snap.session()
  await put(db, 'a', '2')

  t.is(s.writable, false)
  t.is(await get(s, 'a'), '1')

  await s.close()
  await snap.close()
})
