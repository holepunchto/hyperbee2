const Bee = require('../../')
const Corestore = require('corestore')

exports.create = create

async function create (t, opts) {
  const store = new Corestore(await t.tmp())
  const db = new Bee(store, opts)
  t.teardown(() => db.close())
  return db
}
