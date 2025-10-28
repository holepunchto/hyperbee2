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

test('basic overwrite', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    await w.flush()
  }

  t.alike((await db.get(b4a.from('a'))).value, b4a.from('1'))

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('2'))
    await w.flush()
  }

  t.alike((await db.get(b4a.from('a'))).value, b4a.from('2'))

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('3'))
    await w.flush()
  }

  t.alike((await db.get(b4a.from('a'))).value, b4a.from('3'))

  const head = db.head()

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('3'))
    await w.flush()
  }

  t.alike(db.head(), head)
})

test('basic encrypted', async function (t) {
  const db = await create(t, { encryption: { key: b4a.alloc(32, 'enc') } })

  {
    const w = db.write()
    w.tryPut(b4a.from('PLAINTEXT'), b4a.from('PLAINTEXT'))
    await w.flush()
  }

  t.is(db.core.length, 1)
  const blk = await db.core.get(0, { raw: true })
  t.ok(b4a.toString(blk).indexOf('PLAINTEXT') === -1)
})

test('big overwrite', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    for (let i = 0; i < 3000; i++) {
      w.tryPut(b4a.from('#' + i), b4a.from('1'))
    }
    await w.flush()
  }

  {
    const w = db.write()
    for (let i = 0; i < 3000; i++) {
      w.tryPut(b4a.from('#' + i), b4a.from('2'))
    }
    await w.flush()
  }

  const actual = []
  const expected = []

  for await (const data of db.createReadStream()) {
    actual.push(data.value)
    expected.push(b4a.from('2'))
  }

  t.alike(actual, expected)
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
      const k = b4a.from('' + n++)
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

test('basic cross link (encrypted)', async function (t) {
  const db = await create(t, { encryption: { key: b4a.alloc(32) } })

  {
    const w = db.write()
    w.tryPut(b4a.from('hello'), b4a.from('world'))
    w.tryPut(b4a.from('hej'), b4a.from('verden'))
    await w.flush()
  }

  const db2 = await create(t, { encryption: { key: b4a.alloc(32) } })

  replicate(t, db, db2)

  {
    const w = db2.write(db.head())
    w.tryPut(b4a.from('hej'), b4a.from('verden*'))
    await w.flush()
  }

  t.alike((await db2.get(b4a.from('hej')))?.value, b4a.from('verden*'))
  t.alike((await db2.get(b4a.from('hello')))?.value, b4a.from('world'))
})

test('changes', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('1'), b4a.from('1'))
    await w.flush()
  }

  {
    const w = db.write()
    w.tryPut(b4a.from('2'), b4a.from('2'))
    w.tryPut(b4a.from('3'), b4a.from('3'))
    await w.flush()
  }

  {
    const w = db.write()
    w.tryPut(b4a.from('4'), b4a.from('4'))
    w.tryPut(b4a.from('5'), b4a.from('5'))
    w.tryPut(b4a.from('6'), b4a.from('6'))
    await w.flush()
  }

  {
    const w = db.write()
    w.tryPut(b4a.from('4'), b4a.from('4'))
    w.tryPut(b4a.from('5'), b4a.from('5'))
    w.tryPut(b4a.from('6'), b4a.from('6'))
    w.tryPut(b4a.from('7'), b4a.from('7'))
    await w.flush()
  }

  {
    const w = db.write()
    w.tryPut(b4a.from('4'), b4a.from('4'))
    w.tryPut(b4a.from('5'), b4a.from('5'))
    w.tryPut(b4a.from('6'), b4a.from('6'))
    w.tryPut(b4a.from('7'), b4a.from('7'))
    w.tryPut(b4a.from('8'), b4a.from('8'))
    w.tryPut(b4a.from('9'), b4a.from('9'))
    w.tryPut(b4a.from('10'), b4a.from('10'))
    w.tryPut(b4a.from('11'), b4a.from('11'))
    w.tryPut(b4a.from('12'), b4a.from('12'))
    w.tryPut(b4a.from('13'), b4a.from('13'))
    w.tryPut(b4a.from('14'), b4a.from('14'))
    await w.flush()
  }

  const changes = []
  const head = db.head()

  for await (const data of db.createChangesStream()) {
    changes.push(data)
  }

  let length = head.length
  t.is(changes.length, 5)
  t.alike(changes[0].head, head)
  length -= changes[0].batch.length
  t.alike(changes[1].head, { ...head, length })
  length -= changes[1].batch.length
  t.alike(changes[2].head, { ...head, length })
  length -= changes[2].batch.length
  t.alike(changes[3].head, { ...head, length })
  length -= changes[4].batch.length
  t.alike(changes[4].head, { ...head, length })
})
