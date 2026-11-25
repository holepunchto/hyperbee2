const b4a = require('b4a')
const BulkLoader = require('./bulk-load.js')

module.exports = async function importEntries(destination, source) {
  if (destination === source) throw new Error('Cannot import a bee into itself')

  // Capture an already-ready source before the first await. The caller may move
  // or advance the live source as soon as import() returns its promise.
  let pinned = source.root === null ? null : pinSource(source)

  let stream = null
  let loader = null

  try {
    if (pinned === null) {
      await source.ready()
      pinned = pinSource(source)
    }

    await destination.ready()

    // undo() defers its rollback until bootstrap, so materialise the selected
    // head in the owned view before the root is used as the pinned version.
    await pinned.bootstrap(pinned.config)
    await validateDestination(destination, pinned.head())

    stream = pinned.createReadStream()

    for await (const entry of stream) {
      if (loader === null) loader = await BulkLoader.open(destination)
      await loader.append(entry)
    }

    if (loader !== null) {
      const root = await loader.finish()
      if (root !== null) destination._setRoot(root, true)
    }
  } finally {
    if (stream !== null) stream.destroy()
    if (loader !== null) await loader.close()
    if (pinned !== null) await pinned.close()
  }
}

function pinSource(source) {
  return source._makeView(
    source.context,
    source.root,
    false,
    source.unbatch,
    source.config.timeout,
    source.config.wait
  )
}

async function validateDestination(destination, sourceHead) {
  if (!destination.writable) throw new Error('Import destination must be writable')

  const head = destination.head()
  if (head === null) throw new Error('Import destination is not ready')
  if (head.length !== 0) throw new Error('Import destination must be a genesis bee')

  const local = destination.context.getLocalContext()
  await local.core.ready()

  if (!b4a.equals(head.key, local.core.key)) {
    throw new Error('Import destination must be rooted in its local core')
  }

  if (!local.core.writable) throw new Error('Import destination must be writable')
  if (local.core.length !== 0) {
    throw new Error('Import destination must be a fresh bee with no blocks')
  }

  if (sourceHead !== null && b4a.equals(local.core.key, sourceHead.key)) {
    throw new Error('Import destination must use a core distinct from the source')
  }
}
