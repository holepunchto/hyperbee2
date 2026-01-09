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

test('basic (empty cache)', async function (t) {
  const db = await create(t)
  const w = db.write()

  w.tryPut(b4a.from('hello'), b4a.from('world'))
  w.tryPut(b4a.from('hej'), b4a.from('verden'))
  w.tryPut(b4a.from('hi'), b4a.from('ho'))

  await w.flush()

  db.cache.empty()

  t.alike((await db.get(b4a.from('hi'))).value, b4a.from('ho'))
  t.alike((await db.get(b4a.from('hej'))).value, b4a.from('verden'))
  t.alike((await db.get(b4a.from('hello'))).value, b4a.from('world'))
})

test('basic, bigger (empty cache)', async function (t) {
  const db = await create(t)
  const w = db.write()

  for (let i = 0; i < 20; i++) {
    w.tryPut(b4a.from('#' + i), b4a.from('#' + i))
  }

  await w.flush()

  db.cache.empty()

  for (let i = 0; i < 20; i++) {
    t.alike((await db.get(b4a.from('#' + i))).value, b4a.from('#' + i))
  }
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

test('empty noop batch', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryDelete(b4a.from('a'))
    await w.flush()
  }
})

test('basic delete', async function (t) {
  const db = await create(t)

  {
    const length = db.core.length
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    w.tryPut(b4a.from('c'), b4a.from('3'))
    await w.flush()
    t.ok(length < db.core.length)
  }

  {
    const length = db.core.length
    const w = db.write()
    w.tryDelete(b4a.from('b'))
    await w.flush({ debug: true })
    t.ok(length < db.core.length)
  }

  const expected = ['a', 'c']

  for await (const { key } of db.createReadStream()) {
    t.alike(b4a.toString(key), expected.shift())
  }

  t.is(expected.length, 0)
})

test('basic encrypted', async function (t) {
  const db = await create(t, { encryption: { key: b4a.alloc(32, 'enc') } })

  {
    const w = db.write()
    w.tryPut(b4a.from('PLAINTEXT'), b4a.from('PLAINTEXT'))
    await w.flush()
  }

  for (let i = 0; i < db.core.length; i++) {
    const blk = await db.core.get(i, { raw: true })
    t.ok(b4a.toString(blk).indexOf('PLAINTEXT') === -1)
  }
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

test('parallel batch', async function (t) {
  const db = await create(t)
  const w1 = db.write()
  const w2 = db.write()

  w1.tryPut(b4a.from('hello'), b4a.from('world'))
  w1.tryPut(b4a.from('hej'), b4a.from('verden'))
  w1.tryPut(b4a.from('hi'), b4a.from('ho'))

  w2.tryPut(b4a.from('hello'), b4a.from('world*'))

  await Promise.all([w1.flush(), w2.flush()])

  t.alike((await db.get(b4a.from('hi'))).value, b4a.from('ho'))
  t.alike((await db.get(b4a.from('hej'))).value, b4a.from('verden'))
  t.alike((await db.get(b4a.from('hello'))).value, b4a.from('world*'))
})

test('basic seq, offset and core', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('hello'))
    await w.flush()
  }
  {
    const w = db.write()
    w.tryPut(b4a.from('b'), b4a.from('world'))
    await w.flush()
  }
  {
    const w = db.write()
    w.tryPut(b4a.from('c'), b4a.from('!'))
    await w.flush()
  }

  const a = await db.get(b4a.from('a'))
  const b = await db.get(b4a.from('b'))
  const c = await db.get(b4a.from('c'))

  t.ok(a.seq !== null && a.seq !== undefined)
  t.ok(a.offset !== null && a.offset !== undefined)
  t.ok(a.core !== null && typeof a.core === 'object')

  t.ok(b.seq !== null && b.seq !== undefined)
  t.ok(b.offset !== null && b.offset !== undefined)
  t.ok(b.core !== null && typeof b.core === 'object')

  t.ok(c.seq !== null && c.seq !== undefined)
  t.ok(c.offset !== null && c.offset !== undefined)
  t.ok(c.core !== null && typeof c.core === 'object')

  t.not(a.seq, b.seq)
  t.not(a.seq, c.seq)
  t.not(b.seq, c.seq)
})
