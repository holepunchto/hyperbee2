const test = require('brittle')
const { create } = require('./helpers')

test('basic', async function (t) {
  const db = await create(t)
  const w = db.write()

  w.tryPut(Buffer.from('hello'), Buffer.from('world'))
  w.tryPut(Buffer.from('hej'), Buffer.from('verden'))
  w.tryPut(Buffer.from('hi'), Buffer.from('ho'))

  await w.flush()

  t.alike((await db.get(Buffer.from('hi'))).value, Buffer.from('ho'))
  t.alike((await db.get(Buffer.from('hej'))).value, Buffer.from('verden'))
  t.alike((await db.get(Buffer.from('hello'))).value, Buffer.from('world'))
})

test('100 keys', async function (t) {
  const db = await create(t)

  const expected = []
  for (let i = 0; i < 100; i++) {
    const w = db.write()
    const k = Buffer.from('' + i)
    expected.push(k)
    w.tryPut(k, k)
    await w.flush()
  }

  expected.sort(Buffer.compare)
  const actual = []

  for await (const data of db.createReadStream()) {
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
      const k = Buffer.from('' + (n++))
      expected.push(k)
      w.tryPut(k, k)
    }
    await w.flush()
  }

  expected.sort(Buffer.compare)
  const actual = []

  for await (const data of db.createReadStream()) {
    actual.push(data.key)
  }

  t.alike(actual, expected)
})

test('basic fuzz (2k rounds)', async function (t) {
  const db = await create(t)

  const expected = new Map()

  for (let i = 0; i < 2000; i++) {
    const n = (Math.random() * 10) | 0
    const w = db.write()
    for (let j = 0; j < n; j++) {
      const put = Math.random() < 0.8
      const k = Buffer.from('' + ((Math.random() * 10_000) | 0))
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

  const sorted = [...expected.values()].sort(Buffer.compare)
  const actual = []

  for await (const data of db.createReadStream()) {
    actual.push(data.key)
  }

  t.alike(actual, sorted)
})
