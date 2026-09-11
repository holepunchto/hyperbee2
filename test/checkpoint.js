const test = require('brittle')
const b4a = require('b4a')
const Corestore = require('corestore')
const Bee = require('../')
const { create, replicate } = require('./helpers')

test('offline reader can move to cached checkpoint after learning a newer length', async function (t) {
  // db1 owns the first core, db2 writes on top of it so db2's root block
  // references db1's core through the checkpoint core table
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

  const storage = await t.tmp()
  let checkpoint = null

  {
    const store = new Corestore(storage)
    const reader = new Bee(store, { key: db2.core.key, writable: false })
    await reader.ready()

    const appended = new Promise((resolve) => reader.core.once('append', resolve))
    const stop = replicateManual(reader, db2)
    replicateManual(reader, db1)
    await appended

    t.alike((await reader.get(b4a.from('a'))).value, b4a.from('1'))
    t.alike((await reader.get(b4a.from('b'))).value, b4a.from('2'))

    checkpoint = reader.head()
    t.is(checkpoint.length, 1)

    // remote appends, reader only learns the new length, no reads
    const next = new Promise((resolve) => reader.core.once('append', resolve))
    {
      const w = db2.write()
      w.tryPut(b4a.from('c'), b4a.from('3'))
      await w.flush()
    }
    await next
    t.is(reader.core.length, 2)

    await stop()
    await reader.close()
  }

  {
    const store = new Corestore(storage)
    const reader = new Bee(store, { key: db2.core.key, writable: false })
    t.teardown(() => reader.close())
    await reader.ready()

    t.is(reader.core.length, 2, 'still knows the newer length')

    reader.move({ length: checkpoint.length })

    // both keys resolve without a peer, 'a' lives on db1's core so the
    // core table must come from the cached checkpoint, not the head block
    t.alike((await reader.get(b4a.from('a'), { wait: false })).value, b4a.from('1'))
    t.alike((await reader.get(b4a.from('b'), { wait: false })).value, b4a.from('2'))
  }
})

test('core table resolves from the referring block, not the tip of the core', async function (t) {
  // db2 block 0 references db1, block 1 introduces a new checkpoint that also
  // references db3, block 2 is a plain append. A reader that only has block 1
  // locally must be able to resolve db3 from block 1's checkpoint without
  // touching block 2 (the tip), which it does not have.
  const db1 = await create(t)
  {
    const w = db1.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    await w.flush()
  }

  const db3 = await create(t)
  {
    const w = db3.write()
    w.tryPut(b4a.from('x'), b4a.from('9'))
    await w.flush()
  }

  const db2 = await create(t)
  replicate(t, db1, db2)
  replicate(t, db3, db2)
  {
    const w = db2.write(db1.head())
    w.tryPut(b4a.from('b'), b4a.from('2'))
    await w.flush()
  }
  {
    const w = db2.write(db3.head())
    w.tryPut(b4a.from('c'), b4a.from('3'))
    await w.flush()
  }
  {
    const w = db2.write()
    w.tryPut(b4a.from('d'), b4a.from('4'))
    await w.flush()
  }
  t.is(db2.core.length, 3)

  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close())

  const reader = new Bee(store, { key: db2.core.key, writable: false })
  t.teardown(() => reader.close())
  await reader.ready()

  const appended = new Promise((resolve) => reader.core.once('append', resolve))
  const stop = replicateManual(reader, db2)
  await appended
  t.is(reader.core.length, 3)

  // only fetch the block we are going to read from, never the tip
  await reader.core.get(1)
  await stop()

  t.is(await reader.core.has(1), true)
  t.is(await reader.core.has(2), false, 'tip is not local')

  // db3 is reachable, db2 is not
  const c3 = store.get({ key: db3.core.key })
  await c3.ready()
  replicate(t, reader, db3)
  await c3.get(0)

  reader.move({ length: 2 })

  t.alike((await reader.get(b4a.from('x'), { wait: false })).value, b4a.from('9'))
  t.alike((await reader.get(b4a.from('c'), { wait: false })).value, b4a.from('3'))
})

function replicateManual(a, b) {
  const s1 = a.replicate(true)
  const s2 = b.replicate(false)

  s1.pipe(s2).pipe(s1)

  s1.on('error', () => {})
  s2.on('error', () => {})

  const closed1 = new Promise((resolve) => s1.once('close', resolve))
  const closed2 = new Promise((resolve) => s2.once('close', resolve))

  return async function stop() {
    s1.destroy()
    s2.destroy()
    await closed1
    await closed2
  }
}
