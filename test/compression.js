const test = require('brittle')
const b4a = require('b4a')
const Bee = require('../')
const Corestore = require('corestore')

test('multi writer relative pointers', async function (t) {
  const path = await t.tmp()
  let store = new Corestore(path)
  let a = new Bee(store.namespace('a'))
  let b = new Bee(store.namespace('b'))

  t.teardown(() => a.close())
  t.teardown(() => b.close())
  t.teardown(() => store.close())

  await a.ready()
  await b.ready()

  async function reopen() {
    await a.close()
    await b.close()
    await store.close()
    store = new Corestore(path)
    a = new Bee(store.namespace('a'))
    b = new Bee(store.namespace('b'))
    await a.ready()
    await b.ready()
  }

  // Relative pointer for first entry is equivalent to absolute pointer
  {
    const k = b4a.from('1')
    const w = a.write()

    w.tryPut(k, k)
    await w.flush()
  }

  // Introduce relative pointers that differ to the absolute pointers
  {
    const k = b4a.from('3')
    const w = a.write()

    w.tryPut(k, k)
    await w.flush()
  }
  {
    const k = b4a.from('4')
    const w = a.write()

    w.tryPut(k, k)
    await w.flush()
  }

  // close and re-open dbs
  await reopen()

  // Build on top of remote hyperbee that has used a relative pointer
  {
    const k = b4a.from('2')
    const w = b.write({ key: a.core.key, length: a.core.length })

    w.tryPut(k, k)
    await w.flush()
  }

  // close and re-open dbs
  await reopen()

  async function getKeys(hb) {
    const keys = []
    for await (const data of hb.createReadStream()) {
      keys.push(data.key)
    }
    return keys
  }

  t.alike(await getKeys(a), [b4a.from('1'), b4a.from('3'), b4a.from('4')])

  t.alike(await getKeys(b), [b4a.from('1'), b4a.from('2'), b4a.from('3'), b4a.from('4')])

  t.pass('finished')
})
