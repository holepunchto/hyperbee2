const Bee = require('../../')
const Corestore = require('corestore')

exports.create = create

async function create (t, opts) {
  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close()) // TODO: obvs should be bee close
  return new Bee(store, opts)
}
