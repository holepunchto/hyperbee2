const b4a = require('b4a')
const test = require('brittle')
const Corestore = require('corestore')
const createTestnet = require('hyperdht/testnet')
const Hyperswarm = require('hyperswarm')

const Bee = require('..')

test.solo('basic network', async (t) => {
  const { store, swarm, store2, swarm2 } = await setup(t, 2)

  const db = new Bee(store)
  t.teardown(() => db.close())
  await db.ready()
  swarm.join(db.context.core.discoveryKey)

  const w = db.write()
  w.tryPut(b4a.from('hello'), b4a.from('world'))
  await w.flush()

  const db2 = new Bee(store2, { key: db.context.core.key })
  t.teardown(() => db2.close())
  await db2.ready()
  swarm2.join(db2.context.core.discoveryKey)

  await db2.download()

  t.alike((await db.get(b4a.from('hello')))?.value, b4a.from('world'))
  t.alike((await db2.get(b4a.from('hello')))?.value, b4a.from('world'))
})

async function setup(t, n = 1, network) {
  const res = network ?? (await setupTestnet(t))
  const { bootstrap } = res

  for (let step = 1; step <= n; step++) {
    const storage = await t.tmp()
    const store = new Corestore(storage)
    t.teardown(() => store.close(), { order: 4000 })
    const swarm = new Hyperswarm({ bootstrap })
    t.teardown(() => swarm.destroy(), { order: 3000 })

    swarm.on('connection', (conn) => store.replicate(conn))

    const nstring = step > 1 ? step : ''
    res[`storage${nstring}`] = storage
    res[`store${nstring}`] = store
    res[`swarm${nstring}`] = swarm
  }

  return res
}

async function setupTestnet(t) {
  const testnet = await createTestnet()
  t.teardown(() => testnet.destroy(), { order: 5000 })
  const bootstrap = testnet.bootstrap
  return { testnet, bootstrap }
}
