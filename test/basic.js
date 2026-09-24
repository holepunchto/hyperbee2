const test = require('brittle')
const b4a = require('b4a')
const Corestore = require('corestore')
const Bee = require('../')
const { TYPE_LATEST, TYPE_COMPAT, decodeBlock } = require('../lib/encoding.js')
const { create, createMultiple, replicate } = require('./helpers')

const INFLIGHT_RANGE = [256, 512]

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

test('basic, two batches', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('hello'), b4a.from('world'))
    w.tryPut(b4a.from('hej'), b4a.from('verden'))
    w.tryPut(b4a.from('hi'), b4a.from('ho'))
    await w.flush()
  }

  {
    const w = db.write()
    w.tryPut(b4a.from('hola'), b4a.from('mundo'))
    await w.flush()
  }

  t.alike((await db.get(b4a.from('hi'))).value, b4a.from('ho'))
  t.alike((await db.get(b4a.from('hej'))).value, b4a.from('verden'))
  t.alike((await db.get(b4a.from('hello'))).value, b4a.from('world'))
  t.alike((await db.get(b4a.from('hola'))).value, b4a.from('mundo'))
})

test('migrating by copy from t = 5 to t = 128', async function (t) {
  const storage = await t.tmp()

  const migrationEntries = new Map()

  {
    const store = new Corestore(storage)
    const db = new Bee(store, { t: 5 })

    {
      const w = db.write()
      for (let i = 0; i < 256; i++) {
        const key = b4a.from('k' + String(i).padStart(3, 0))
        const value = b4a.from('v' + String(i).padStart(3, 0))
        w.tryPut(key, value)
        migrationEntries.set(key, value)
      }
      await w.flush()
    }

    t.alike((await db.get(b4a.from('k001'))).value, b4a.from('v001'))
    t.is(db.root.value.t, 5, 'root has value 5')

    await db.close()
  }

  t.comment('migrate')
  {
    const store = new Corestore(storage)
    const db = new Bee(store)
    await db.ready()

    t.is(db.t, 128, 'bee is now default value t')

    db.move({ key: db.core.key, length: 0 })
    {
      const w = db.write()
      for (const [key, value] of migrationEntries.entries()) {
        w.tryPut(key, value)
      }
      await w.flush()
    }

    t.is(db.root.value.t, 128, 'bee still default value t')

    t.alike((await db.get(b4a.from('k001'))).value, b4a.from('v001'))
    t.is(
      decodeBlock(await db.context.core.get(db.context.core.length - 1)).t,
      128,
      'got t = 128 for root block'
    )
    t.is(decodeBlock(await db.context.core.get(0)).t, 5, 'old blocks still exist')
    await db.close()
  }
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

test('isGenesis', async function (t) {
  const db = await create(t)

  t.is(db.isGenesis(), false, 'false before ready')

  await db.ready()

  t.is(db.isGenesis(), true)
  t.is(db.head().length, 0)

  const w = db.write()
  w.tryPut(b4a.from('a'), b4a.from('1'))
  await w.flush()

  t.is(db.isGenesis(), false)

  const empty = db.checkout({ length: 0 })
  await empty.ready()
  t.is(empty.isGenesis(), true)
  await empty.close()

  const latest = db.checkout({ length: db.head().length })
  await latest.ready()
  t.is(latest.isGenesis(), false)
  await latest.close()
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

test('basic get encrypted', async function (t) {
  const db = await create(t, {
    getEncryptionProvider: () => ({
      key: b4a.alloc(32, 'enc')
    })
  })

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

test('basic auto-update', async function (t) {
  const db = await create(t)
  await db.ready()

  const db2 = await create(t, { key: db.core.key, autoUpdate: true })
  await db2.ready()

  const db3 = await create(t, { key: db.core.key, autoUpdate: false })
  await db3.ready()

  replicate(t, db, db2)
  replicate(t, db, db3)

  {
    const w = db.write()
    w.tryPut(b4a.from('1'), b4a.from('1'))
    await w.flush()
  }

  {
    const w = db.write()
    w.tryPut(b4a.from('2'), b4a.from('2'))
    await w.flush()
  }

  // event flush
  await new Promise((resolve) => setTimeout(resolve, 100))

  t.alike((await db2.get(b4a.from('1'))).value, b4a.from('1'))
  t.alike((await db2.get(b4a.from('2'))).value, b4a.from('2'))

  t.alike(await db3.get(b4a.from('1')), null)
  t.alike(await db3.get(b4a.from('2')), null)

  {
    const w = db.write()
    w.tryPut(b4a.from('3'), b4a.from('3'))
    w.tryPut(b4a.from('4'), b4a.from('4'))
    await w.flush()
  }

  {
    const w = db.write()
    w.tryPut(b4a.from('1'), b4a.from('1-updated'))
    await w.flush()
  }

  // event flush
  await new Promise((resolve) => setTimeout(resolve, 100))

  t.alike((await db2.get(b4a.from('3'))).value, b4a.from('3'))
  t.alike((await db2.get(b4a.from('4'))).value, b4a.from('4'))
  t.alike((await db2.get(b4a.from('1'))).value, b4a.from('1-updated'))

  t.alike(await db3.get(b4a.from('3')), null)
  t.alike(await db3.get(b4a.from('4')), null)
  t.alike(await db3.get(b4a.from('1')), null)
})

test('autoUpdate defaults are correct', async function (t) {
  const db = await create(t, { writable: true })
  await db.ready()
  t.is(db.autoUpdate, false)

  const db2 = await create(t, { key: db.core.key, writable: true })
  t.is(db2.autoUpdate, false)

  const db3 = await create(t, { key: db.core.key, writable: false })
  t.is(db3.autoUpdate, true)

  // views
  const snap = db3.snapshot()
  t.is(snap.autoUpdate, false)

  await snap.close()
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

test('basic cross link (encryption+getEncryption)', async function (t) {
  const db = await create(t, { encryption: { key: b4a.alloc(32) } })

  {
    const w = db.write()
    w.tryPut(b4a.from('hello'), b4a.from('world'))
    w.tryPut(b4a.from('hej'), b4a.from('verden'))
    await w.flush()
  }

  const db2 = await create(t, { getEncryptionProvider: () => ({ key: b4a.alloc(32) }) })

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

test('reindex replays remote changes into the local core', async function (t) {
  const [a, b] = await createMultiple(t, 2)
  await a.ready()
  await b.ready()

  const heads = []

  for (let i = 0; i < 3; i++) {
    const w = b.write()
    w.tryPut(b4a.from('key' + i), b4a.from('val' + i))
    if (i === 2) w.tryDelete(b4a.from('key0'))
    await w.flush()
    heads.push(b.head())
  }

  let appends = 0
  a.core.on('append', () => appends++)

  a.move({ key: b.core.key, length: b.head().length })
  t.alike(a.head().key, b.core.key)

  t.is(await a.reindex(() => false), 3)
  t.is(appends, 1, 'all changes land in a single append')
  t.alike(a.head().key, a.core.key)
  t.is(a.head().length, a.core.length)

  const changes = []
  for await (const data of a.createChangesStream()) changes.push(data)

  t.is(changes.length, 3)
  for (const c of changes) t.alike(c.head.key, a.core.key)
  t.is(changes[2].tail, null)

  for (let i = 0; i < 3; i++) {
    const local = await entries(a.checkout(changes[2 - i].head))
    const remote = await entries(b.checkout(heads[i]))
    t.alike(local, remote, 'version ' + i + ' matches')
  }

  const indexed = b.head()

  for (let i = 3; i < 5; i++) {
    const w = b.write()
    w.tryPut(b4a.from('key' + i), b4a.from('val' + i))
    w.tryPut(b4a.from('key1'), b4a.from('updated' + i))
    await w.flush()
  }

  a.move({ key: b.core.key, length: b.head().length })

  const n = await a.reindex(
    (c) => b4a.equals(c.head.key, indexed.key) && c.head.length <= indexed.length
  )

  t.is(n, 2)
  t.alike(a.head().key, a.core.key)

  const all = []
  for await (const data of a.createChangesStream()) all.push(data)
  t.is(all.length, 5)
  for (const c of all.slice(0, 2)) t.alike(c.head.key, a.core.key)
  t.alike(all[1].tail, indexed)
  for (const c of all.slice(2)) t.alike(c.head.key, b.core.key)

  t.alike(await entries(a), await entries(b))

  // nothing new to index
  const localHead = a.head()
  t.is(await a.reindex((c) => c.head.length <= localHead.length), 0)
  t.alike(a.head(), localHead)

  async function entries(db) {
    const list = []
    for await (const e of db.createReadStream()) {
      list.push(b4a.toString(e.key) + '=' + b4a.toString(e.value))
    }
    if (db.view) await db.close()
    return list
  }
})

test('reindex preserves batches across splits, value blocks and cores', async function (t) {
  const [a, b, c] = await createMultiple(t, 3, { t: 3 })
  await a.ready()
  await b.ready()
  await c.ready()

  // c seeds a tree, b builds on top of c's head so b's blocks point into c's core
  {
    const w = c.write()
    for (let i = 0; i < 40; i++) w.tryPut(b4a.from('c' + pad(i)), b4a.from('c' + i))
    await w.flush()
  }

  const genesis = c.head()

  b.move(genesis)

  for (let round = 0; round < 4; round++) await writeRound(round)

  const remote = []
  for await (const data of b.createChangesStream()) remote.push(data)
  t.is(remote.length, 5)

  const source = b.head()
  t.absent(b4a.equals(source.key, b.core.key), 'b is viewing c, its writes live in its local core')

  // stop at c's change, so the copies keep pointing into c's core for untouched nodes
  a.move(source)
  t.is(await a.reindex((change) => b4a.equals(change.head.key, genesis.key)), 4)

  t.alike(a.head().key, a.core.key)
  t.is(a.head().length, a.core.length)
  t.is((await a.cores()).length, 2, 'local table references c but not b')

  const copied = []
  for await (const data of a.createChangesStream()) copied.push(data)

  t.is(copied.length, 5)
  t.alike(copied[3].tail, genesis, 'oldest copy links to the remote tail')
  t.alike(copied[4].head, genesis)

  for (let i = 0; i < 4; i++) {
    const r = remote[i]
    const l = copied[i]

    t.alike(l.head.key, a.core.key)
    t.alike(r.head.key, source.key)
    t.is(l.batch.length, r.batch.length, 'change ' + i + ' has the same number of blocks')

    for (let j = 0; j < r.batch.length; j++) {
      t.alike(shape(l.batch[j]), shape(r.batch[j]), 'change ' + i + ' block ' + j + ' matches')
    }
  }

  a.cache.empty()
  t.alike(await entries(a), await entries(b))

  for (let i = 0; i < 4; i++) {
    const l = await entries(a.checkout(copied[i].head))
    const r = await entries(b.checkout(remote[i].head))
    t.alike(l, r, 'version ' + i + ' matches')
  }

  // b writes more, a only copies the new change and chains it onto its own tip
  await writeRound(4)

  a.move(b.head())
  t.is(
    await a.reindex((c) => b4a.equals(c.head.key, source.key) && c.head.length <= source.length),
    1
  )

  const more = []
  for await (const data of a.createChangesStream()) more.push(data)

  t.is(more.length, 6)
  t.alike(more[0].head, a.head())
  t.alike(more[0].tail, source, 'new copy links to the change until stopped at')
  t.alike(more[1].head, source)
  t.is((await a.cores()).length, 3, 'now also references b')

  a.cache.empty()
  t.alike(await entries(a), await entries(b))

  // a's own write continues the chain and reads through copied blocks
  {
    const w = a.write()
    w.tryPut(b4a.from('a0'), b4a.from('a'))
    w.tryDelete(b4a.from('b' + pad(7)))
    await w.flush()
  }

  a.cache.empty()
  t.alike((await a.get(b4a.from('a0'))).value, b4a.from('a'))
  t.is(await a.get(b4a.from('b' + pad(7))), null)
  t.alike((await a.get(b4a.from('b' + pad(14)))).value, b4a.alloc(3000, 14))
  t.alike((await a.get(b4a.from('c' + pad(39)))).value, b4a.from('c39'))

  async function writeRound(round) {
    const w = b.write()
    for (let i = 0; i < 15; i++) {
      const n = round * 15 + i
      const value = n % 7 === 0 ? b4a.alloc(3000, n) : b4a.from('b' + n)
      w.tryPut(b4a.from('b' + pad(n)), value)
    }
    for (let i = 0; i < 3; i++) w.tryDelete(b4a.from('c' + pad(round * 8 + i)))
    await w.flush()
  }

  function pad(n) {
    return String(n).padStart(3, '0')
  }

  function shape(blk) {
    return {
      type: blk.type,
      batch: blk.batch,
      t: blk.t,
      keys: blk.keys && blk.keys.map((k) => ({ key: k.key, value: k.value })),
      values: blk.values,
      tree:
        blk.tree &&
        blk.tree.map((n) => ({
          keys: n.keys.map((d) => [d.type, d.index, d.pointer && d.pointer.offset]),
          children: n.children.map((d) => [d.type, d.index, d.pointer && d.pointer.offset])
        })),
      cohorts:
        blk.cohorts &&
        blk.cohorts.map((co) => co.map((d) => [d.type, d.index, d.pointer && d.pointer.offset]))
    }
  }

  async function entries(db) {
    const list = []
    for await (const e of db.createReadStream()) {
      list.push(b4a.toString(e.key) + '=' + b4a.toString(e.value))
    }
    if (db.view) await db.close()
    return list
  }
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

test('throws hypercore error if block not available', async function (t) {
  t.plan(1)

  const db = await create(t)
  await db.ready()

  const db2 = await create(t, { key: db.core.key, autoUpdate: true })
  await db2.ready()

  replicate(t, db, db2)

  const w = db.write()
  w.tryPut(b4a.from('a'), b4a.alloc(32))
  await w.flush()

  await new Promise((resolve) => setTimeout(resolve, 100))

  try {
    await db2.get(b4a.from('b'), { wait: false })
    t.fail('should have failed')
  } catch (error) {
    t.is(error.code, 'BLOCK_NOT_AVAILABLE')
  }
})

test('lock to avoid building concurrent batches', async function (t) {
  const name = b4a.from('name')

  const db = await create(t)
  const w = db.write()
  w.tryPut(name, b4a.from('world'))
  await w.flush()

  const w1 = db.write()
  const p1 = (async () => {
    await w1.lock()
    const entry = await db.get(name)
    w1.tryPut(name, b4a.from(entry.value.toString() + '!'))
    await w1.flush()
  })()

  const w2 = db.write()
  const p2 = (async () => {
    await w2.lock()
    const entry = await db.get(name)
    w2.tryPut(name, b4a.from(entry.value.toString().toUpperCase()))
    await w2.flush()
  })()

  await p1
  await p2

  t.alike((await db.get(name)).value, b4a.from('WORLD!'))
})

test('emit update event after remote append to empty tree and autoUpdate = true', async function (t) {
  let counter = 0
  const db = await create(t, { autoUpdate: true })
  db.on('update', () => counter++)

  await db.ready()

  // Manually append to underlying core
  await db.core.append([
    // w.tryPut(b4a.from('hello'), b4a.from('world'))
    b4a.from('010000000c01011100010568656c6c6f0105776f726c64', 'hex')
  ])

  const { promise, resolve } = Promise.withResolvers()
  setTimeout(resolve, 0)

  await promise
  t.alike(counter, 1)
})

test('emit update event after remote append to non-empty tree and autoUpdate = true', async function (t) {
  let counter = 0
  const db = await create(t, { autoUpdate: true })
  db.on('update', () => counter++)

  await db.ready()

  const w = db.write()
  w.tryPut(b4a.from('hello'), b4a.from('world'))
  await w.flush()

  // Manually append to underlying core
  await db.core.append([
    // w.tryPut(b4a.from('hi'), b4a.from('ho'))
    b4a.from('010000000d000001021159010100010268690102686f', 'hex')
  ])

  const { promise, resolve } = Promise.withResolvers()
  setTimeout(resolve, 0)

  await promise
  t.alike(counter, 2)
})

test('do not emit multiple update events when autoUpdate = true', async function (t) {
  let counter = 0
  const db = await create(t, { autoUpdate: true })
  db.on('update', () => counter++)

  await db.ready()

  const w = db.write()
  w.tryPut(b4a.from('hello'), b4a.from('world'))
  await w.flush()

  t.alike(counter, 1)
})

test('ensure head is set correctly immediately after flush', async function (t) {
  const db = await create(t, { autoUpdate: true })

  await db.ready()
  const key = db.core.key

  t.alike(db.head(), { length: 0, key })

  const w = db.write()
  w.tryPut(b4a.from('hello'), b4a.from('world'))
  await w.flush()

  t.alike(db.head(), { length: 1, key })
})

test('emit update event for move() when autoUpdate is false', async function (t) {
  let counter = 0
  const db = await create(t, { autoUpdate: false })
  db.on('update', () => counter++)

  await db.ready()

  {
    const w = db.write()
    w.tryPut(b4a.from('hello'), b4a.from('world'))
    await w.flush()
  }

  {
    const w = db.write()
    w.tryPut(b4a.from('hi'), b4a.from('ho'))
    await w.flush()
  }

  t.alike(counter, 2)

  db.move({ length: 1 })
  t.alike(counter, 3)
})

test('emit update event after rollback', async function (t) {
  const db = await create(t, { autoUpdate: false })
  await db.ready()

  {
    const w = db.write()
    w.tryPut(b4a.from('hello'), b4a.from('world'))
    await w.flush()
  }

  {
    const w = db.write()
    w.tryPut(b4a.from('hi'), b4a.from('ho'))
    await w.flush()
  }

  let counter = 0
  const db2 = db.undo(1)
  db2.on('update', () => counter++)
  await db2.ready()

  t.alike(counter, 0)

  // Force bootstrap
  await db2.get(b4a.from('hello'))

  t.alike(counter, 1)
})

test('emit update when move changes key but not length', async function (t) {
  const db = await create(t)
  await db.ready()

  {
    const w = db.write()
    w.tryPut(b4a.from('hello'), b4a.from('world'))
    await w.flush()
  }

  let counter = 0
  const db2 = await create(t)
  db2.on('update', () => counter++)

  replicate(t, db, db2)

  await db2.ready()
  t.alike(counter, 0)

  {
    const w = db2.write()
    w.tryPut(b4a.from('hello'), b4a.from('world!!!'))
    await w.flush()
  }

  t.alike((await db2.get(b4a.from('hello')))?.value, b4a.from('world!!!'))
  t.alike(counter, 1)

  // Both db and db2 head now have length=1 (but different keys).
  db2.move(db.head())

  t.alike((await db2.get(b4a.from('hello')))?.value, b4a.from('world'))
  t.alike(counter, 2)
})

test('move to current head() does not emit update event', async function (t) {
  let counter = 0
  const db = await create(t)
  db.on('update', () => counter++)

  await db.ready()
  t.alike(counter, 0)

  {
    const w = db.write()
    w.tryPut(b4a.from('hello'), b4a.from('world'))
    await w.flush()
  }
  t.alike(counter, 1)

  db.move(db.head())
  t.alike(counter, 1)
})

test("move - write w/ key after move defaults to head's key", async function (t) {
  const db = await create(t)
  await db.ready()

  {
    const w = db.write()
    w.tryPut(b4a.from('hello'), b4a.from('world'))
    await w.flush()
  }

  const db2 = await create(t)

  replicate(t, db, db2)

  await db2.ready()

  // Both db and db2 head now have length=1 (but different keys).
  const dbHead = db.head()
  db2.move(dbHead)

  t.alike((await db2.get(b4a.from('hello')))?.value, b4a.from('world'))

  {
    const w = db2.write()
    t.alike(w.key, dbHead.key, 'new write is using db head')
    w.tryPut(b4a.from('hello'), b4a.from('world!!!'))
    await w.flush()
  }

  t.alike((await db2.get(b4a.from('hello')))?.value, b4a.from('world!!!'))
})

test('RangeIterator.prefetchNext with upper bound', async function (t) {
  const db = await create(t)

  function encodeUint32(n) {
    const buf = new ArrayBuffer(4)
    const view = new DataView(buf)
    view.setUint32(0, n, false)
    return b4a.from(buf)
  }

  const ENTRIES = 256

  const w = db.write()
  for (let i = 0; i < ENTRIES; i++) {
    w.tryPut(encodeUint32(i), encodeUint32(i))
  }
  await w.flush()

  const opt = {
    prefetch: true,
    lt: encodeUint32(ENTRIES),
    // Needs a limit > minKeys to avoid early exit in prefetchNext
    // (separate bug that can hide this one)
    limit: ENTRIES * 2
  }
  let count = 0
  for await (const _ of db.createReadStream(opt)) {
    count++
  }
  t.alike(count, ENTRIES)
})

test('trace', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('hello'), b4a.from('world'))
    await w.flush()
  }

  {
    const w = db.write()
    w.tryPut(b4a.from('hej'), b4a.from('verden'))
    await w.flush()
  }

  db.cache.empty()
  const seqs = new Set()

  const stream = db.createReadStream({
    trace(core, seq) {
      seqs.add(seq)
    }
  })

  for await (const _ of stream) {
    // do nothing
  }

  t.alike(seqs, new Set([0, 1]))
})

test('repeated put with empty buffer value after reload of storage', async function (t) {
  const storage = await t.tmp()
  const hello = b4a.from('hello')

  {
    const store = new Corestore(storage)
    const db = new Bee(store)

    {
      const w = db.write()
      await w.lock()

      w.tryPut(hello, b4a.alloc(0))
      await w.flush()
    }
    t.alike((await db.get(hello)).value, b4a.alloc(0), 'got empty buffer value')

    await db.close()
    await store.close()
  }

  {
    const store = new Corestore(storage)
    const db = new Bee(store)

    // Previous behaviour was a crash
    {
      const w = db.write()
      await w.lock()

      w.tryPut(hello, b4a.alloc(0))
      await w.flush()
    }

    t.alike((await db.get(hello)).value, b4a.alloc(0), 'got empty buffer value after reloading')

    await db.close()
    await store.close()
  }
})

test('autoUpdate doesnt lose data', async function (t) {
  const db = await create(t, { autoUpdate: true })
  await db.ready()

  {
    const w = db.write()
    w.tryPut(b4a.from('1'), b4a.from('1'))
    w.tryPut(b4a.from('2'), b4a.from('2'))
    await w.flush()
  }

  t.alike((await db.get(b4a.from('1'))).value, b4a.from('1'))
  t.alike((await db.get(b4a.from('2'))).value, b4a.from('2'))
})

test('lots of overwrites with odd batches', async function (t) {
  const db = await create(t)

  let j = 0
  for (let i = 0; i < 1000; i++) {
    const w = db.write()

    j = (j + 1) % 80

    w.tryPut(b4a.from('#a' + j), b4a.from('#' + i + '.' + j))
    w.tryPut(b4a.from('#b' + j), b4a.from('#' + i + '.' + j))
    w.tryPut(b4a.from('#c' + j), b4a.from('#' + i + '.' + j))

    await w.flush()
  }

  t.ok(db.root.value.keys.delta.length < 20, 'sanity check')
})

test('db.compat()', async function (t) {
  const db = await create(t, { t: 5 })

  t.is(await db.compat(), TYPE_LATEST, 'returns TYPE_LATEST w/o blocks (aka default)')

  {
    const w = db.write({ compat: true })
    w.tryPut(b4a.from('beep'), b4a.from('boop'))
    await w.flush()
  }

  t.is(await db.compat(), TYPE_COMPAT, 'returns TYPE_COMPAT w/ compat block')

  const db2 = await create(t)

  t.is(await db2.compat(), TYPE_LATEST, 't = 128 returns TYPE_LATEST w/o blocks (aka default)')

  {
    const w = db2.write()
    w.tryPut(b4a.from('beep'), b4a.from('boop'))
    await w.flush()
  }

  t.is(await db2.compat(), TYPE_LATEST, 't = 128 returns TYPE_COMPAT w/ block')
})

test('inflight range - default root core (no key given)', async function (t) {
  const db = await create(t)
  await db.ready()

  t.alike(db.core.replicator.inflightRange, INFLIGHT_RANGE)
})

test('inflight range - secondary core opened via context.getCore()', async function (t) {
  const db1 = await create(t)
  {
    const w = db1.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    await w.flush()
  }

  const db2 = await create(t)
  replicate(t, db1, db2)

  {
    const w = db2.write(db1.head())
    w.tryPut(b4a.from('b'), b4a.from('2'))
    await w.flush()
  }

  // 'a' lives on db1's core, so resolving it forces db2's local context
  // to open db1's core as a secondary core (context.cores[0]) via getCore()
  const value = await db2.get(b4a.from('a'))
  t.alike(value.value, b4a.from('1'))

  const hc = db2.context.getCore(1)
  t.alike(hc.replicator.inflightRange, INFLIGHT_RANGE)
})

test('inflight range - secondary core opened via context.getContextByKey()', async function (t) {
  const db1 = await create(t)
  {
    const w = db1.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    await w.flush()
  }

  const db2 = await create(t)
  replicate(t, db1, db2)

  // writing on top of a foreign head resolves that key's context via
  // getContextByKey() during flush()
  const w = db2.write(db1.head())
  w.tryPut(b4a.from('b'), b4a.from('2'))
  await w.flush()

  const ctx = db2.context.getContextByKey(db1.core.key)
  t.alike(ctx.core.replicator.inflightRange, INFLIGHT_RANGE)
})

test('cores() lists the own core and referenced cores', async function (t) {
  const db1 = await create(t)
  {
    const w = db1.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    await w.flush()
  }

  const db2 = await create(t)
  replicate(t, db1, db2)

  // only itself before referencing anything
  t.alike(await db2.cores(), [db2.core.key])
  t.alike(await db2.cores({ local: false }), [])

  // writing on top of a foreign head references db1's core
  const w = db2.write(db1.head())
  w.tryPut(b4a.from('b'), b4a.from('2'))
  await w.flush()

  t.alike(await db1.cores(), [db1.core.key])
  t.alike(await db2.cores(), [db2.core.key, db1.core.key])
  t.alike(await db2.cores({ local: false }), [db1.core.key])

  // a fresh reader has to inflate the checkpoint to discover the cores
  const reader = await create(t, { key: db2.core.key, writable: false })
  replicate(t, db2, reader)
  await reader.core.update({ wait: true })

  t.alike(await reader.cores(), [db2.core.key, db1.core.key])
  t.alike(await reader.cores({ local: false }), [db1.core.key])
})

test('checkout with timeout and wait options', async function (t) {
  const db = await create(t)

  const w = db.write()
  w.tryPut(b4a.from('a'), b4a.from('1'))
  await w.flush()

  // reader that never replicates, so every read of a missing block hangs
  const reader = await create(t, { key: db.core.key })
  await reader.ready()

  const inherited = reader.checkout({ length: db.head().length })
  t.is(inherited.config.timeout, reader.config.timeout, 'inherits timeout by default')
  t.is(inherited.config.wait, reader.config.wait, 'inherits wait by default')
  await inherited.close()

  const timedOut = reader.checkout({ length: db.head().length, timeout: 100 })
  t.is(timedOut.config.timeout, 100)
  await t.exception(timedOut.get(b4a.from('a')), /REQUEST_TIMEOUT/)
  await timedOut.close()

  const noWait = reader.checkout({ length: db.head().length, wait: false })
  t.is(noWait.config.wait, false)
  await t.exception(noWait.get(b4a.from('a')), /BLOCK_NOT_AVAILABLE/)
  await noWait.close()
})

test('snapshot with timeout and wait options', async function (t) {
  const db = await create(t)

  const w = db.write()
  w.tryPut(b4a.from('a'), b4a.from('1'))
  await w.flush()

  const reader = await create(t, { key: db.core.key })
  await reader.ready()
  reader.move({ length: db.head().length })

  const inherited = reader.snapshot()
  t.is(inherited.config.timeout, reader.config.timeout, 'inherits timeout by default')
  t.is(inherited.config.wait, reader.config.wait, 'inherits wait by default')
  await inherited.close()

  const timedOut = reader.snapshot({ timeout: 100 })
  t.is(timedOut.config.timeout, 100)
  await t.exception(timedOut.get(b4a.from('a')), /REQUEST_TIMEOUT/)
  await timedOut.close()

  const noWait = reader.snapshot({ wait: false })
  t.is(noWait.config.wait, false)
  await t.exception(noWait.get(b4a.from('a')), /BLOCK_NOT_AVAILABLE/)
  await noWait.close()
})

test('undo with timeout and wait options', async function (t) {
  const db = await create(t)

  for (let i = 0; i < 2; i++) {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('' + i))
    await w.flush()
  }

  const reader = await create(t, { key: db.core.key })
  await reader.ready()
  reader.move({ length: db.head().length })

  const inherited = reader.undo(1)
  t.is(inherited.config.timeout, reader.config.timeout, 'inherits timeout by default')
  t.is(inherited.config.wait, reader.config.wait, 'inherits wait by default')
  await inherited.close()

  const timedOut = reader.undo(1, { timeout: 100 })
  t.is(timedOut.config.timeout, 100)
  await t.exception(timedOut.get(b4a.from('a')), /REQUEST_TIMEOUT/)
  await timedOut.close()

  const noWait = reader.undo(1, { wait: false })
  t.is(noWait.config.wait, false)
  await t.exception(noWait.get(b4a.from('a')), /BLOCK_NOT_AVAILABLE/)
  await noWait.close()
})

test('reindex maps values as they are copied', async function (t) {
  const [a, b] = await createMultiple(t, 2)
  await a.ready()
  await b.ready()

  const big = b4a.alloc(4096).fill('x')
  const huge = b4a.alloc(3 * 4096).fill('y') // spans several value blocks
  const heads = []

  for (let i = 0; i < 3; i++) {
    const w = b.write()
    w.tryPut(b4a.from('inline' + i), b4a.from('val' + i))
    w.tryPut(b4a.from('big' + i), big)
    w.tryPut(b4a.from('huge' + i), huge)
    w.tryPut(b4a.from('keep' + i), b4a.from('keep' + i))
    await w.flush()
    heads.push(b.head())
  }

  a.move(b.head())

  const seen = []

  const n = await a.reindex(() => false, {
    map: (key, value, change) => {
      seen.push([b4a.toString(key), value.byteLength, change.head.length])
      const name = b4a.toString(key)
      if (name.startsWith('inline')) return b4a.from('mapped' + name.slice(6))
      if (name.startsWith('big')) return b4a.concat([b4a.from('mapped'), value])
      if (name.startsWith('huge')) return b4a.concat([b4a.from('mapped'), value])
      return null
    }
  })

  t.is(n, 3)
  t.is(seen.length, 12, 'every copied key is offered to map')
  t.ok(
    seen.some(([name, size]) => name === 'big0' && size === big.byteLength),
    'pointed values are inflated for map'
  )

  a.cache.empty()

  for (let i = 0; i < 3; i++) {
    t.alike((await a.get(b4a.from('inline' + i))).value, b4a.from('mapped' + i))
    t.alike((await a.get(b4a.from('big' + i))).value, b4a.concat([b4a.from('mapped'), big]))
    t.alike((await a.get(b4a.from('huge' + i))).value, b4a.concat([b4a.from('mapped'), huge]))
    t.alike((await a.get(b4a.from('keep' + i))).value, b4a.from('keep' + i))
  }

  const copied = []
  for await (const { head } of a.createChangesStream()) copied.unshift(head)
  t.is(copied.length, 3)

  for (let i = 0; i < 3; i++) {
    const c = a.checkout(copied[i])
    t.alike((await c.get(b4a.from('inline' + i))).value, b4a.from('mapped' + i), 'version ' + i)
    t.is(await c.get(b4a.from('inline' + (i + 1))), null)
    await c.close()
  }
})
