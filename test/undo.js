const test = require('brittle')
const b4a = require('b4a')
const Corestore = require('corestore')
const Bee = require('../')
const { create } = require('./helpers')

test('undo', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('1'), b4a.from('1'))
    await w.flush()
  }

  {
    const w = db.write()
    w.tryPut(b4a.from('2'), b4a.from('2'))
    w.tryPut(b4a.from('1'), b4a.from('2'))
    await w.flush()
  }

  {
    const w = db.write()
    w.tryPut(b4a.from('4'), b4a.from('4'))
    w.tryPut(b4a.from('5'), b4a.from('5'))
    w.tryPut(b4a.from('1'), b4a.from('3'))
    await w.flush()
  }

  const u = db.undo(1)

  t.alike((await u.get(b4a.from('1'))).value, b4a.from('2'))
  t.alike((await u.get(b4a.from('2'))).value, b4a.from('2'))

  {
    const u1 = u.undo(1)

    t.alike((await u1.get(b4a.from('1'))).value, b4a.from('1'))
    t.is(await u1.get(b4a.from('2')), null)
  }

  {
    const u1 = db.undo(2)

    t.alike((await u1.get(b4a.from('1'))).value, b4a.from('1'))
    t.is(await u1.get(b4a.from('2')), null)
  }

  {
    const u1 = db.undo(3)
    t.alike(await u1.get(b4a.from('1')), null)
    t.alike(await u1.get(b4a.from('2')), null)
  }

  {
    const u1 = db.undo(100)
    t.alike(await u1.get(b4a.from('1')), null)
    t.alike(await u1.get(b4a.from('2')), null)
  }
})

test('undo and write', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('1'), b4a.from('1'))
    await w.flush()
  }

  {
    const w = db.write()
    w.tryPut(b4a.from('2'), b4a.from('2'))
    w.tryPut(b4a.from('1'), b4a.from('2'))
    await w.flush()
  }

  {
    const w = db.write()
    w.tryPut(b4a.from('4'), b4a.from('4'))
    w.tryPut(b4a.from('5'), b4a.from('5'))
    w.tryPut(b4a.from('1'), b4a.from('3'))
    await w.flush()
  }

  const u = db.undo(1)

  {
    const all = []

    for await (const { key, value } of u.createReadStream()) {
      all.push({ key, value })
    }

    t.alike(all, [
      { key: b4a.from('1'), value: b4a.from('2') },
      { key: b4a.from('2'), value: b4a.from('2') }
    ])
  }

  {
    const w = u.write()
    w.tryPut(b4a.from('0'), b4a.from('0'))
    w.tryPut(b4a.from('2'), b4a.from('2*'))
    await w.flush()
  }

  {
    const all = []

    for await (const { key, value } of u.createReadStream()) {
      all.push({ key, value })
    }

    t.alike(all, [
      { key: b4a.from('0'), value: b4a.from('0') },
      { key: b4a.from('1'), value: b4a.from('2') },
      { key: b4a.from('2'), value: b4a.from('2*') }
    ])
  }
})

test('move', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('1'), b4a.from('1'))
    await w.flush()
  }

  {
    const w = db.write()
    w.tryPut(b4a.from('2'), b4a.from('1'))
    await w.flush()
  }

  const h = db.head()

  {
    const w = db.write()
    w.tryPut(b4a.from('3'), b4a.from('1'))
    await w.flush()
  }

  db.move(h)

  {
    const w = db.write()
    w.tryPut(b4a.from('3'), b4a.from('1'))
    await w.flush()
  }

  {
    const all = []

    for await (const { key, value } of db.createReadStream()) {
      all.push({ key, value })
    }

    t.alike(all, [
      { key: b4a.from('1'), value: b4a.from('1') },
      { key: b4a.from('2'), value: b4a.from('1') },
      { key: b4a.from('3'), value: b4a.from('1') }
    ])
  }
})

test('undo after writing on top of another core', async function (t) {
  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close())

  const a = new Bee(store.namespace('a'))
  await a.ready()
  for (const k of ['a', 'b']) {
    const w = a.write()
    w.tryPut(b4a.from(k), b4a.from(k))
    await w.flush()
  }

  const b = new Bee(store.namespace('b'))
  await b.ready()
  b.move({ key: a.core.key, length: a.core.length })
  for (const k of ['c', 'd']) {
    const w = b.write()
    w.tryPut(b4a.from(k), b4a.from(k))
    await w.flush()
  }

  t.alike(b.core.key, b.context.local.key, 'the tree views the core it wrote to')

  const head = b.checkout()
  t.alike((await head.get(b4a.from('d'))).value, b4a.from('d'), 'a checkout sees the writes')
  await head.close()

  const undone = b.undo(1)
  t.alike((await undone.get(b4a.from('c'))).value, b4a.from('c'), 'undo keeps the earlier write')
  t.is(await undone.get(b4a.from('d')), null, 'and drops the last one')
  await undone.close()

  await a.close()
  await b.close()
})
