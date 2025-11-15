const Bee = require('../../')
const Corestore = require('corestore')

exports.create = create
exports.createMultiple = createMultiple
exports.replicate = replicate

async function replicate(t, a, b) {
  const s1 = a.replicate(true)
  const s2 = b.replicate(false)

  s1.pipe(s2).pipe(s1)

  const closed1 = new Promise((resolve) => s1.once('close', resolve))
  const closed2 = new Promise((resolve) => s2.once('close', resolve))

  s1.on('error', () => {})
  s2.on('error', () => {})

  t.teardown(async () => {
    s1.destroy()
    s2.destroy()
    await closed1
    await closed2
  })
}

async function create(t, opts) {
  const store = new Corestore(await t.tmp())
  const db = new Bee(store, opts)
  t.teardown(() => db.close())
  return db
}

async function createMultiple(t, n, opts) {
  const store = new Corestore(await t.tmp())
  const dbs = []

  for (let i = 0; i < n; i++) {
    const db = new Bee(store.namespace('#' + i), opts)
    t.teardown(() => db.close())
    dbs.push(db)
  }

  return dbs
}
