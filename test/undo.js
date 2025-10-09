const test = require('brittle')
const b4a = require('b4a')
const { create, replicate } = require('./helpers')

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
