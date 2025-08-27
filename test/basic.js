const test = require('brittle')
const b4a = require('b4a')
const { create, replicate } = require('./helpers')

test('basic', async function (t) {
  const db = await create(t)
  const w = db.write()

  w.tryPut(b4a.from('hello'), b4a.from('world'))
  w.tryPut(b4a.from('hej'), b4a.from('verden'))
  w.tryPut(b4a.from('hi'), b4a.from('ho'))

  await w.flush()

  t.alike((await db.get(b4a.from('hi'))).value, b4a.from('ho'))
  t.alike((await db.get(b4a.from('hej'))).value, b4a.from('verden'))
  t.alike((await db.get(b4a.from('hello'))).value, b4a.from('world'))
})

test('100 keys', async function (t) {
  const db = await create(t)

  const expected = []
  for (let i = 0; i < 100; i++) {
    const w = db.write()
    const k = b4a.from('' + i)
    expected.push(k)
    w.tryPut(k, k)
    await w.flush()
  }

  expected.sort(b4a.compare)
  const actual = []

  for await (const data of db.createReadStream()) {
    actual.push(data.key)
  }

  t.alike(actual, expected)
})

test('100 keys reversed', async function (t) {
  const db = await create(t)

  const expected = []
  for (let i = 0; i < 100; i++) {
    const w = db.write()
    const k = b4a.from('' + i)
    expected.push(k)
    w.tryPut(k, k)
    await w.flush()
  }

  expected.sort((a, b) => -b4a.compare(a, b))
  const actual = []

  for await (const data of db.createReadStream({ reverse: true })) {
    actual.push(data.key)
  }

  t.alike(actual, expected)
})

test('1000 keys in 10 batches', async function (t) {
  const db = await create(t)

  const expected = []

  let n = 0

  for (let i = 0; i < 10; i++) {
    const w = db.write()
    for (let j = 0; j < 100; j++) {
      const k = b4a.from('' + (n++))
      expected.push(k)
      w.tryPut(k, k)
    }
    await w.flush()
  }

  expected.sort(b4a.compare)
  const actual = []

  for await (const data of db.createReadStream()) {
    actual.push(data.key)
  }

  t.alike(actual, expected)
})

test('basic cross link', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('hello'), b4a.from('world'))
    w.tryPut(b4a.from('hej'), b4a.from('verden'))
    await w.flush()
  }

  const db2 = await create(t)

  replicate(t, db, db2)

  {
    const w = db2.write(db.head())
    w.tryPut(b4a.from('hej'), b4a.from('verden*'))
    await w.flush()
  }

  t.alike((await db2.get(b4a.from('hej')))?.value, b4a.from('verden*'))
  t.alike((await db2.get(b4a.from('hello')))?.value, b4a.from('world'))
})

test('basic fuzz (2k rounds)', async function (t) {
  const db = await create(t)

  const expected = new Map()

  for (let i = 0; i < 2000; i++) {
    const n = (Math.random() * 10) | 0
    const w = db.write()
    for (let j = 0; j < n; j++) {
      const put = Math.random() < 0.8
      const k = b4a.from('' + ((Math.random() * 10_000) | 0))
      if (put) {
        expected.set(k.toString(), k)
        w.tryPut(k, k)
      } else {
        expected.delete(k.toString())
        w.tryDelete(k)
      }
    }
    await w.flush()
  }

  const sorted = [...expected.values()].sort(b4a.compare)
  const actual = []

  for await (const data of db.createReadStream()) {
    actual.push(data.key)
  }

  t.alike(actual, sorted)
})
