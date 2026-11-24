const b4a = require('b4a')

const MAX_ENTRIES = 1024
const MAX_BYTES = 1024 * 1024

module.exports = async function importEntries(destination, source) {
  if (destination === source) throw new Error('Cannot import a bee into itself')

  await destination.ready()
  await source.ready()

  let pinned = null
  let stream = null
  let batch = null
  let entries = 0
  let bytes = 0

  try {
    pinned = await pinSource(source)
    await validateDestination(destination, pinned.head())

    stream = pinned.createReadStream()

    for await (const entry of stream) {
      const value = entry.value
      const size = entry.key.byteLength + (value === null ? 0 : value.byteLength)

      if (batch !== null && (entries >= MAX_ENTRIES || bytes + size > MAX_BYTES)) {
        await flush()
      }

      if (batch === null) batch = destination.write()
      batch.tryPut(entry.key, value)
      entries++
      bytes += size
    }

    if (batch !== null) await flush()
  } finally {
    if (stream !== null) stream.destroy()
    if (batch !== null && !batch.closed) await batch.close()
    if (pinned !== null) await pinned.close()
  }

  async function flush() {
    const current = batch
    entries = 0
    bytes = 0
    await current.flush()
    if (batch === current) batch = null
  }
}

async function pinSource(source) {
  const pinned = source._makeView(
    source.context,
    source.root,
    false,
    source.unbatch,
    source.config.timeout,
    source.config.wait
  )

  try {
    // undo() defers its rollback until bootstrap, so materialise the selected
    // head in the owned view before the root is used as the pinned version.
    await pinned.bootstrap(pinned.config)
  } catch (err) {
    await pinned.close()
    throw err
  }

  return pinned
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
