const test = require('brittle')
const b4a = require('b4a')
const Corestore = require('corestore')
const Bee = require('../')
const { decodeBlock } = require('../lib/encoding.js')

function binaryKey(i) {
  return b4a.from(i.toString(16).padStart(8, '0'), 'hex')
}

function binaryValue(i) {
  return b4a.from([i & 0xff, (i >>> 8) & 0xff, 0, 0xff])
}

async function readEntries(bee) {
  const entries = []

  for await (const entry of bee.createReadStream()) {
    entries.push({
      key: b4a.from(entry.key),
      value: entry.value === null ? null : b4a.from(entry.value)
    })
  }

  return entries
}

async function createImportBee(t, opts) {
  const store = new Corestore(await t.tmp())
  const bee = new Bee(store, opts)
  t.teardown(() => bee.close())
  await bee.ready()
  return bee
}

async function seedImportSource(t, count = 1500) {
  const source = await createImportBee(t)

  {
    const w = source.write()
    for (let i = 0; i < count; i++) w.tryPut(binaryKey(i), binaryValue(i))
    await w.flush()
  }

  return { source, entries: await readEntries(source), state: importState(source) }
}

async function seedOldVersion(source) {
  {
    const w = source.write()
    w.tryPut(b4a.from('old'), b4a.from('old-value'))
    await w.flush()
  }

  return readEntries(source)
}

async function seedTwoBatchStore(t) {
  const storage = await t.tmp()
  const store = new Corestore(storage)
  const source = new Bee(store)
  await source.ready()

  let firstBatch
  {
    const w = source.write()
    for (let i = 0; i < 20; i++) w.tryPut(binaryKey(i), b4a.from('first-' + i))
    await w.flush()
    firstBatch = await readEntries(source)
  }

  {
    const w = source.write()
    for (let i = 20; i < 40; i++) w.tryPut(binaryKey(i), b4a.from('second-' + i))
    await w.flush()
  }

  const all = await readEntries(source)
  await source.close()

  return { storage, firstBatch, all }
}

function importState(bee) {
  return { length: bee.core.length, head: bee.head() }
}

function alikeImportState(t, bee, state, message) {
  t.is(bee.core.length, state.length, message + ': core length unchanged')
  t.alike(bee.head(), state.head, message + ': head unchanged')
}

function trackImportResources(source, destination, onAppend = null) {
  const tracked = {
    view: null,
    stream: null,
    viewClosed: false,
    streamClosed: false,
    appends: [],
    batches: [],
    flushes: []
  }

  const makeView = source._makeView

  source._makeView = function (...args) {
    const view = makeView.apply(this, args)
    tracked.view = view

    const close = view.close
    view.close = function (...closeArgs) {
      tracked.viewClosed = true
      return close.apply(this, closeArgs)
    }

    const createReadStream = view.createReadStream
    view.createReadStream = function (...streamArgs) {
      tracked.stream = createReadStream.apply(this, streamArgs)
      tracked.stream.once('close', () => {
        tracked.streamClosed = true
      })
      return tracked.stream
    }

    return view
  }

  const write = destination.write

  destination.write = function (...writeArgs) {
    const batch = write.apply(this, writeArgs)
    tracked.batches.push(batch)

    const flush = batch.flush
    batch.flush = function (...flushArgs) {
      let bytes = 0

      for (const op of batch.ops) {
        bytes += op.key.byteLength
        if (op.value !== null) bytes += op.value.byteLength
      }

      tracked.flushes.push({
        entries: batch.ops.length,
        bytes,
        keys: batch.ops.map((op) => op.key.toString('hex'))
      })
      return flush.apply(this, flushArgs)
    }

    return batch
  }

  // Import writes directly through the local core. Tracking that boundary works
  // for the chunked writer and lets each committed batch be inspected.
  const core = destination.context.getLocalContext().core
  const append = core.append

  core.append = function (buffers, ...args) {
    const list = Array.isArray(buffers) ? buffers : [buffers]
    let bytes = 0

    for (const buffer of list) bytes += buffer.byteLength

    tracked.appends.push({ blocks: list.length, bytes })
    const appending = append.call(this, buffers, ...args)
    if (onAppend !== null) onAppend(tracked.appends, appending)
    return appending
  }

  tracked.restore = function () {
    source._makeView = makeView
    destination.write = write
    core.append = append
  }

  return tracked
}

async function importFailure(destination, source) {
  try {
    await destination.import(source)
    return null
  } catch (err) {
    return err
  }
}

async function assertImportStreamReleased(t, tracked, message) {
  const { view, stream } = tracked

  t.ok(stream !== null, message + ': opens a read stream')
  if (stream === null) return

  t.is(stream.tree, view, message + ': reads through the pinned view')

  const destroyed = stream.destroyed
  t.ok(destroyed, message + ': destroys its read stream')

  if (destroyed && !tracked.streamClosed) {
    await new Promise((resolve) => stream.once('close', resolve))
  }

  t.is(view.streams.length, 0, message + ': unregisters its stream from the pinned view')
}

function assertImportLeavesBothOpen(t, source, destination) {
  t.is(source.closed, false, 'source bee remains open')
  t.is(destination.closed, false, 'destination bee remains open')
}

async function assertSourceIntactAndRetry(t, source, sourceState, expected) {
  t.alike(source.head(), sourceState.head, 'source head unchanged')
  t.is(source.core.length, sourceState.length, 'source core length unchanged')
  t.alike(await readEntries(source), expected, 'source still reads after the fault is removed')

  const retry = await createImportBee(t)
  await retry.import(source)
  t.alike(await readEntries(retry), expected, 'retries into a different fresh destination')
}

function injectSourceReadFailure(source, fault) {
  const core = source.core
  const get = core.get
  let failing = false

  core.get = function (...args) {
    if (failing) return Promise.reject(fault)
    return get.apply(this, args)
  }

  return {
    fail() {
      failing = true
      source.cache.empty()
    },
    restore() {
      core.get = get
    }
  }
}

function instrumentAppend(bee, failOn = -1, fault = null) {
  const core = bee.core
  const append = core.append
  let calls = 0

  core.append = function (...args) {
    calls++
    if (calls === failOn) return Promise.reject(fault)
    return append.apply(this, args)
  }

  return {
    get calls() {
      return calls
    },
    restore() {
      core.append = append
    }
  }
}

async function readBlocks(bee) {
  const blocks = []
  const length = bee.core.length

  for (let seq = 0; seq < length; seq++) {
    blocks.push({ seq, block: decodeBlock(await bee.core.get(seq), seq) })
  }

  return blocks
}

async function treeShape(bee) {
  const root = await bee.bootstrap(bee.config)
  if (root === null) return { levels: 0, nodes: [], leaves: [] }

  const nodes = []
  const leaves = []
  let levels = 0
  let frontier = [{ ptr: root, depth: 0 }]

  while (frontier.length) {
    const next = []
    levels++

    for (const { ptr, depth } of frontier) {
      const node = ptr.value ? bee.bump(ptr) : await bee.inflate(ptr, bee.config)
      const entry = { depth, keys: node.keys.length, children: node.children.length }

      nodes.push(entry)

      if (node.children.length === 0) {
        leaves.push(entry)
      } else {
        for (let i = 0; i < node.children.length; i++) {
          next.push({ ptr: node.children.get(i), depth: depth + 1 })
        }
      }
    }

    frontier = next
  }

  return { levels, nodes, leaves }
}

function assertLegalTree(t, shape, degree, message) {
  const maxKeys = 2 * degree - 1
  const minKeys = degree - 1

  t.ok(shape.levels > 0, message + ': has a tree')
  t.ok(shape.leaves.length > 0, message + ': has leaves')
  t.is(new Set(shape.leaves.map((leaf) => leaf.depth)).size, 1, message + ': leaves share a depth')

  for (const node of shape.nodes) {
    t.ok(node.keys <= maxKeys, message + ': node keys within the degree')

    if (node.children > 0) {
      t.is(node.children, node.keys + 1, message + ': internal child count matches its keys')
    }

    if (node.depth > 0) {
      t.ok(node.keys >= minKeys, message + ': non-root node is at least half full')
    }
  }
}

test('import into a fresh destination copies nothing from an empty source', async function (t) {
  const source = await createImportBee(t)
  const destination = await createImportBee(t)
  const before = importState(destination)

  await destination.import(source)

  t.is(destination.core.length, before.length, 'appends no blocks')
  t.alike(destination.head(), before.head, 'head unchanged')
  t.alike(await readEntries(destination), [], 'destination stays empty')
})

test('import copies 2,051 binary entries in bounded chunks', async function (t) {
  const source = await createImportBee(t)
  const destination = await createImportBee(t)

  const big = b4a.alloc(2 * 1024 * 1024 + 1).fill(0xab)
  const subLimit = b4a.alloc(700 * 1024).fill(0xcd)
  const overwritten = b4a.from('overwritten-value')

  {
    const w = source.write()
    for (let i = 0; i <= 2052; i++) w.tryPut(binaryKey(i), i === 2 ? null : binaryValue(i))
    await w.flush()
  }

  {
    const w = source.write()
    w.tryPut(binaryKey(1), b4a.alloc(0))
    w.tryPut(binaryKey(7), overwritten)
    w.tryPut(binaryKey(2050), big)
    w.tryPut(binaryKey(2051), subLimit)
    w.tryPut(binaryKey(2052), subLimit)
    w.tryDelete(binaryKey(100))
    w.tryDelete(binaryKey(2000))
    await w.flush()
  }

  const expected = await readEntries(source)
  t.is(expected.length, 2051, 'source has 2,051 live entries')

  const tracked = trackImportResources(source, destination)

  try {
    await destination.import(source)
  } finally {
    tracked.restore()
  }

  t.alike(await readEntries(destination), expected, 'copies every ordered key/value pair')
  t.alike((await destination.get(binaryKey(7))).value, overwritten, 'copies overwritten value')
  t.is(await destination.get(binaryKey(100)), null, 'does not copy a deleted key')
  t.is((await destination.get(binaryKey(2))).value, null, 'preserves a null value')
  t.alike((await destination.get(binaryKey(1))).value, b4a.alloc(0), 'preserves an empty value')
  t.alike((await destination.get(binaryKey(2050))).value, big, 'copies an oversized value')
  t.is(destination.cache.dirty.length, 0, 'unmarks every written node')

  t.ok(tracked.appends.length > 1, 'copied in multiple appends')
  t.ok(
    tracked.appends.every((append) => append.blocks > 0),
    'never appended an empty batch'
  )
  t.ok(
    tracked.appends.every((append) => append.bytes > 0),
    'never appended empty buffers'
  )

  const maxKeys = 2 * destination.t - 1
  const valueBlocks = []
  const keyBlocks = []

  for (const { block } of await readBlocks(destination)) {
    if (block.keys !== null) {
      keyBlocks.push(block)
      // One block can hold deltas for more than one node; each node's keys are
      // still bounded by the degree at the writer boundary.
      const nodes = Math.max(1, block.tree.length)
      t.ok(block.keys.length <= maxKeys * nodes, 'kept every node block within the degree')
    }

    if (block.values !== null) {
      valueBlocks.push(block)
      const bytes = block.values.reduce((total, value) => total + value.byteLength, 0)
      t.ok(bytes <= 4096 || block.values.length === 1, 'bounded every value block')
    }
  }

  t.ok(keyBlocks.length > 1, 'wrote more than one node block')
  t.ok(valueBlocks.length > 0, 'stored external values outside the node blocks')

  const bigBlock = valueBlocks.find((block) => block.values.some((value) => b4a.equals(value, big)))
  t.ok(bigBlock !== undefined, 'stores the oversized value in a value block')
  t.is(bigBlock === undefined ? -1 : bigBlock.values.length, 1, 'stores the oversized value alone')

  const holder = keyBlocks.find((block) =>
    block.keys.some((key) => b4a.equals(key.key, binaryKey(2050)))
  )
  t.ok(holder !== undefined, 'writes a node block holding the oversized key')
  t.ok(
    holder !== undefined &&
      holder.keys.find((key) => b4a.equals(key.key, binaryKey(2050))).valuePointer !== null,
    'externalizes the oversized value pointer'
  )

  const pair = [2051, 2052].map((i) => binaryKey(i).toString('hex'))
  const oversized = tracked.flushes.filter((flush) => flush.bytes > 1024 * 1024)

  t.ok(tracked.flushes.length > 1, 'copied in multiple chunks')
  t.ok(
    tracked.flushes.some((flush) => flush.entries === 1024),
    'filled a chunk up to the entry-count cap'
  )
  t.ok(oversized.length > 0, 'exceeded the byte cap')
  t.alike(
    tracked.flushes.filter((flush) => flush.entries > 1 && flush.bytes > 1024 * 1024),
    [],
    'kept every multi-entry chunk within the byte cap'
  )
  t.ok(
    oversized.every((flush) => flush.entries === 1),
    'flushed every oversized chunk as a singleton'
  )
  t.is(
    tracked.flushes.filter((flush) => pair.every((key) => flush.keys.includes(key))).length,
    0,
    'split sub-limit values that exceed the byte cap together'
  )
  t.is(
    tracked.flushes.filter((flush) => flush.entries === 0).length,
    0,
    'never flushed an empty chunk'
  )
  t.ok(tracked.flushes[tracked.flushes.length - 1].entries < 1024, 'flushed a short tail chunk')
  t.ok(
    tracked.batches.every((batch) => batch.closed),
    'closes every committed batch after the final flush'
  )
})

test('import keeps the pinned source view when the live source advances', async function (t) {
  const source = await createImportBee(t)
  const destination = await createImportBee(t)

  {
    // More than one packed write batch, so the import is paused after it has
    // already appended blocks.
    const w = source.write()
    for (let i = 0; i < 9000; i++) w.tryPut(binaryKey(i), binaryValue(i))
    await w.flush()
  }

  const expected = await readEntries(source)
  const lateKey = binaryKey(9999)
  const lateValue = b4a.from('late-entry')

  let appendedFirst
  const firstAppend = new Promise((resolve) => {
    appendedFirst = resolve
  })
  let releaseImport
  const released = new Promise((resolve) => {
    releaseImport = resolve
  })

  const tracked = trackImportResources(source, destination)
  const core = destination.context.getLocalContext().core
  const append = core.append
  let paused = false

  core.append = async function (...args) {
    const result = await append.apply(this, args)

    if (!paused) {
      paused = true
      appendedFirst()
      await released
    }

    return result
  }

  try {
    const importing = destination.import(source)

    await firstAppend
    {
      const w = source.write()
      w.tryPut(lateKey, lateValue)
      await w.flush()
    }
    releaseImport()

    await importing
  } finally {
    core.append = append
    tracked.restore()
  }

  t.alike(await readEntries(destination), expected, 'imports only the pinned source view')
  t.is(await destination.get(lateKey), null, 'ignores the entry appended after the first append')
})

test('import pins the source version before the caller moves it', async function (t) {
  const source = await createImportBee(t)
  const destination = await createImportBee(t)
  const expected = await seedOldVersion(source)

  const done = destination.import(source)
  source.move({ length: 0 })

  await done

  t.alike(await readEntries(destination), expected, 'copies the version selected at the call')
  t.is(source.head().length, 0, 'source stays moved to length zero')
})

test('import pins the source before waiting on a delayed destination', async function (t) {
  const source = await createImportBee(t)
  const destination = await createImportBee(t)
  const expected = await seedOldVersion(source)

  const ready = destination.ready
  let releaseReady = null
  const gate = new Promise((resolve) => {
    releaseReady = resolve
  })

  destination.ready = async function () {
    await gate
    return ready.call(this)
  }

  try {
    const done = destination.import(source)
    source.move({ length: 0 })
    releaseReady()

    await done
  } finally {
    destination.ready = ready
  }

  t.alike(await readEntries(destination), expected, 'copies the version selected at the call')
  t.is(source.head().length, 0, 'source stays moved to length zero')
})

test('import pins an unopened source once it is ready', async function (t) {
  const storage = await t.tmp()

  const seedStore = new Corestore(storage)
  const seed = new Bee(seedStore)
  await seed.ready()
  const expected = await seedOldVersion(seed)
  await seed.close()

  const destination = await createImportBee(t)

  const store = new Corestore(storage)
  const source = new Bee(store)
  t.teardown(() => source.close())

  t.is(source.head(), null, 'source has not selected a version yet')

  await destination.import(source)

  t.alike(await readEntries(destination), expected, 'copies the source version after it opens')
})

test('import materializes a deferred undo source before copying', async function (t) {
  const source = await createImportBee(t)
  const destination = await createImportBee(t)

  let firstBatch
  {
    const w = source.write()
    for (let i = 0; i < 20; i++) w.tryPut(binaryKey(i), b4a.from('first-' + i))
    await w.flush()
    firstBatch = await readEntries(source)
  }

  {
    const w = source.write()
    for (let i = 20; i < 40; i++) w.tryPut(binaryKey(i), b4a.from('second-' + i))
    await w.flush()
  }

  const undo = source.undo(1)
  t.teardown(() => undo.close())

  await destination.import(undo)

  t.alike(await readEntries(destination), firstBatch, 'imports the materialized undo view')
})

test('import preserves an unopened undo source selection', async function (t) {
  const destination = await createImportBee(t)
  const { storage, firstBatch, all } = await seedTwoBatchStore(t)

  const source = new Bee(new Corestore(storage))
  t.teardown(() => source.close())

  const undo = source.undo(1)
  t.teardown(() => undo.close())

  t.is(source.head(), null, 'source has not selected a version yet')
  t.is(undo.unbatch, 1, 'undo view keeps its pending rollback before it opens')

  await destination.import(undo)

  t.alike(await readEntries(destination), firstBatch, 'copies the selected prior version')
  t.alike(await readEntries(source), all, 'leaves the source on its latest version')
})

test('import preserves an unopened undo selection while destination is delayed', async function (t) {
  const destination = await createImportBee(t)
  const { storage, firstBatch, all } = await seedTwoBatchStore(t)

  const source = new Bee(new Corestore(storage))
  t.teardown(() => source.close())

  const ready = destination.ready
  let releaseReady = null
  const gate = new Promise((resolve) => {
    releaseReady = resolve
  })

  destination.ready = async function () {
    await gate
    return ready.call(this)
  }

  const undo = source.undo(1)
  t.teardown(() => undo.close())

  try {
    const importing = destination.import(undo)

    // Let the view finish opening, which clears its own pending undo, before
    // the destination readiness gate is released.
    await undo.ready()
    releaseReady()

    await importing
  } finally {
    destination.ready = ready
  }

  t.alike(await readEntries(destination), firstBatch, 'copies the version selected at the call')
  t.alike(await readEntries(source), all, 'leaves the source on its latest version')
})

test('import rejects the original source read error after progress', async function (t) {
  const { source, entries: expected, state: sourceState } = await seedImportSource(t, 9000)
  const destination = await createImportBee(t)
  const fault = new Error('source read failed')
  const read = injectSourceReadFailure(source, fault)
  const append = instrumentAppend(destination)
  const tracked = trackImportResources(source, destination, (appends, appending) => {
    if (appends.length === 1) {
      appending.then(
        () => read.fail(),
        () => {}
      )
    }
  })

  let rejected
  try {
    rejected = await importFailure(destination, source)
  } finally {
    read.restore()
    append.restore()
    tracked.restore()
  }

  t.is(rejected, fault, 'rejects with the original source read error')
  t.is(tracked.flushes.length, 1, 'flushes only the chunk completed before the failure')
  t.is(append.calls, 1, 'appends no blocks after the failed read')
  t.is(tracked.appends.length, 1, 'stops at the append completed before the failure')
  t.ok(destination.core.length > 0, 'keeps the progress written before the failure')
  t.is(destination.cache.dirty.length, 0, 'unmarks its written nodes after the failure')
  t.ok(tracked.viewClosed, 'closes its owned source view')
  t.is(tracked.batches.length, 2, 'keeps the flushed batch and the unflushed one')
  t.ok(
    tracked.batches.every((batch) => batch.closed),
    'closes both the flushed and the unflushed batch'
  )
  await assertImportStreamReleased(t, tracked, 'source read failure')
  assertImportLeavesBothOpen(t, source, destination)
  await assertSourceIntactAndRetry(t, source, sourceState, expected)
})

test('import rejects the original destination append error after progress', async function (t) {
  const { source, entries: expected, state: sourceState } = await seedImportSource(t, 9000)
  const destination = await createImportBee(t)
  const fault = new Error('destination append failed')
  const append = instrumentAppend(destination, 2, fault)
  const tracked = trackImportResources(source, destination)

  let rejected
  try {
    rejected = await importFailure(destination, source)
  } finally {
    append.restore()
    tracked.restore()
  }

  t.is(rejected, fault, 'rejects with the original destination append error')
  t.is(tracked.flushes.length, 2, 'attempts no flush after the failed append')
  t.is(append.calls, 2, 'appends no blocks after the failed append')
  t.is(tracked.appends.length, 2, 'records the progress and the failed attempt')
  t.ok(destination.core.length > 0, 'keeps the progress written before the failure')
  t.is(destination.cache.dirty.length, 0, 'unmarks its written nodes after the failure')
  t.ok(tracked.viewClosed, 'closes its owned source view')
  t.is(tracked.batches.length, 2, 'keeps the flushed batch and the failed one')
  t.ok(
    tracked.batches.every((batch) => batch.closed),
    'closes both the flushed and the failed batch'
  )
  await assertImportStreamReleased(t, tracked, 'destination append failure')
  assertImportLeavesBothOpen(t, source, destination)
  await assertSourceIntactAndRetry(t, source, sourceState, expected)
})

test('import closes its own resources and leaves both bees open', async function (t) {
  const { source, entries: expected } = await seedImportSource(t, 20)
  const destination = await createImportBee(t)
  const tracked = trackImportResources(source, destination)

  try {
    await destination.import(source)
  } finally {
    tracked.restore()
  }

  t.ok(tracked.viewClosed, 'closes its owned source view')
  await assertImportStreamReleased(t, tracked, 'successful import')
  t.is(tracked.appends.length, 1, 'writes a short source in one append')
  t.is(tracked.batches.length, 1, 'uses one batch for a short source')
  t.ok(
    tracked.batches.every((batch) => batch.closed),
    'closes the batch after the final flush'
  )
  t.is(destination.cache.dirty.length, 0, 'unmarks every written node')
  assertImportLeavesBothOpen(t, source, destination)
  t.alike(await readEntries(destination), expected, 'copies every entry')
})

test('import rejects the same bee without writing', async function (t) {
  const bee = await createImportBee(t)

  {
    const w = bee.write()
    w.tryPut(binaryKey(1), binaryValue(1))
    await w.flush()
  }

  const before = importState(bee)

  await t.exception(bee.import(bee), /itself/)

  alikeImportState(t, bee, before, 'same bee')
})

test('import rejects a destination handle on the source core', async function (t) {
  const store = new Corestore(await t.tmp())
  const source = new Bee(store)
  const destination = new Bee(store)

  t.teardown(async () => {
    await source.close()
    await destination.close()
  })

  await source.ready()
  await destination.ready()

  const before = importState(destination)

  await t.exception(destination.import(source), /core/)

  alikeImportState(t, destination, before, 'same core key')
})

test('import rejects a read-only destination without writing', async function (t) {
  const source = await createImportBee(t)
  const destination = await createImportBee(t)

  {
    const w = source.write()
    w.tryPut(binaryKey(1), binaryValue(1))
    await w.flush()
  }

  const readOnly = destination.snapshot()
  t.teardown(() => readOnly.close())

  const before = importState(destination)

  await t.exception(readOnly.import(source), /writable/)

  alikeImportState(t, destination, before, 'read-only destination')
})

test('import rejects a destination not rooted in its local core', async function (t) {
  const source = await createImportBee(t)
  const other = await createImportBee(t)
  const destination = await createImportBee(t)

  {
    const w = source.write()
    w.tryPut(binaryKey(1), binaryValue(1))
    await w.flush()
  }

  destination.move({ key: other.core.key, length: 0, writable: true })

  const before = importState(destination)
  t.is(before.length, 0, 'destination has no blocks')
  t.is(before.head.length, 0, 'destination head is empty')
  t.ok(b4a.equals(before.head.key, other.core.key), 'destination head points at another core')
  t.ok(
    !b4a.equals(before.head.key, destination.context.local.key),
    'destination head is not the local append core'
  )

  await t.exception(destination.import(source), /local core/)

  alikeImportState(t, destination, before, 'non-local destination')
})

test('import rejects a populated destination without writing', async function (t) {
  const source = await createImportBee(t)
  const destination = await createImportBee(t)

  {
    const w = source.write()
    w.tryPut(binaryKey(1), binaryValue(1))
    await w.flush()
  }

  {
    const w = destination.write()
    w.tryPut(binaryKey(2), binaryValue(2))
    await w.flush()
  }

  const before = importState(destination)
  const tracked = trackImportResources(source, destination)

  try {
    await t.exception(destination.import(source), /fresh|empty|genesis/)
  } finally {
    tracked.restore()
  }

  t.ok(tracked.viewClosed, 'closes its owned source view')
  t.is(tracked.appends.length, 0, 'appends no blocks')
  alikeImportState(t, destination, before, 'populated destination')
})

test('import rejects a destination moved to length zero over populated blocks', async function (t) {
  const source = await createImportBee(t)
  const destination = await createImportBee(t)

  {
    const w = source.write()
    w.tryPut(binaryKey(1), binaryValue(1))
    await w.flush()
  }

  {
    const w = destination.write()
    w.tryPut(binaryKey(2), binaryValue(2))
    await w.flush()
  }

  destination.move({ length: 0 })

  const before = importState(destination)
  t.ok(before.length > 0, 'destination still has physical blocks')
  t.is(before.head.length, 0, 'destination appears as a genesis head')

  await t.exception(destination.import(source), /fresh|empty|blocks/)

  alikeImportState(t, destination, before, 'moved destination')
})

test('import builds an independent degree-128 tree in separate storage', async function (t) {
  const sourceStorage = await t.tmp()
  const destinationStorage = await t.tmp()

  const sourceStore = new Corestore(sourceStorage)
  const source = new Bee(sourceStore, { t: 5 })
  t.teardown(() => source.close())

  await source.ready()

  {
    const w = source.write()
    for (let i = 0; i < 300; i++) w.tryPut(binaryKey(i), binaryValue(i))
    await w.flush()
  }

  {
    const w = source.write()
    w.tryPut(binaryKey(3), b4a.from('updated'))
    w.tryDelete(binaryKey(7))
    await w.flush()
  }

  const expected = await readEntries(source)
  const sourceHead = source.head()
  const sourceLength = source.core.length

  t.ok(expected.length > 128, 'source has more live entries than one destination node')

  const destinationStore = new Corestore(destinationStorage)
  const destination = new Bee(destinationStore)
  t.teardown(() => destination.close())

  await destination.ready()
  t.is(destination.t, 128, 'destination uses the default degree')
  t.ok(
    !b4a.equals(destination.head().key, sourceHead.key),
    'destination is rooted in a distinct core'
  )

  await destination.import(source)

  t.alike(source.head(), sourceHead, 'source head unchanged after import')
  t.is(source.core.length, sourceLength, 'source core length unchanged after import')
  t.alike(await readEntries(destination), expected, 'copies every live entry')

  const destinationHead = destination.head()

  await source.close()
  await destination.close()

  const reopenedStore = new Corestore(destinationStorage)
  const reopened = new Bee(reopenedStore)
  t.teardown(() => reopened.close())
  await reopened.ready()

  t.alike(reopened.head(), destinationHead, 'reopened destination keeps the imported head')
  t.alike(await readEntries(reopened), expected, 'entries survive without the source storage')

  const seq = reopened.core.length - 1
  t.is(decodeBlock(await reopened.core.get(seq), seq).t, 128, 'persisted root block stores t = 128')
})

test('import packs sorted entries into the target two-level tree', async function (t) {
  const count = 40000
  const sourceDir = await t.tmp()
  const destinationDir = await t.tmp()

  const sourceStore = new Corestore(sourceDir)
  const source = new Bee(sourceStore)
  t.teardown(() => source.close())
  await source.ready()

  {
    const w = source.write()
    for (let i = 0; i < count; i++) w.tryPut(binaryKey(i), binaryValue(i))
    await w.flush()
  }

  const expected = await readEntries(source)
  const sourceHead = source.head()

  const destinationStore = new Corestore(destinationDir)
  const destination = new Bee(destinationStore)
  t.teardown(() => destination.close())
  await destination.ready()

  await destination.import(source)

  t.alike(source.head(), sourceHead, 'source head unchanged')
  t.alike(await readEntries(destination), expected, 'copies every entry')

  const shape = await treeShape(destination)
  t.is(shape.levels, 2, 'targets two levels instead of three')
  t.is(shape.leaves.length, 157, 'packs 157 leaves at the default degree')
  assertLegalTree(t, shape, 128, 'packed 40,000 entries')

  const head = destination.head()

  await source.close()
  await destination.close()

  const reopenedStore = new Corestore(destinationDir)
  const reopened = new Bee(reopenedStore)
  t.teardown(() => reopened.close())
  await reopened.ready()

  t.alike(reopened.head(), head, 'reopened destination keeps the imported head')
  t.alike(
    await readEntries(reopened),
    expected,
    'every key and value byte survives without the source storage'
  )

  const seq = reopened.core.length - 1
  t.is(decodeBlock(await reopened.core.get(seq), seq).t, 128, 'persisted root block stores t = 128')

  const checkout = reopened.checkout(reopened.head())
  t.teardown(() => checkout.close())

  const first = await checkout.get(binaryKey(0))
  const last = await checkout.get(binaryKey(count - 1))

  t.alike(first.value, binaryValue(0), 'checkout reads the first imported entry')
  t.alike(last.value, binaryValue(count - 1), 'checkout reads the last imported entry')
})

test('import packs node-capacity boundaries', async function (t) {
  for (const count of [1, 254, 255, 256]) {
    const source = await createImportBee(t)
    const destination = await createImportBee(t)

    {
      const w = source.write()
      for (let i = 0; i < count; i++) w.tryPut(binaryKey(i), binaryValue(i))
      await w.flush()
    }

    const expected = await readEntries(source)

    await destination.import(source)

    t.alike(await readEntries(destination), expected, count + ': round-trips every entry')

    const shape = await treeShape(destination)
    assertLegalTree(t, shape, 128, count + ' entries')

    if (count <= 255) {
      t.is(shape.levels, 1, count + ': single leaf for one node worth of entries')
      t.is(shape.leaves.length, 1, count + ': one leaf')
    } else {
      t.is(shape.levels, 2, count + ': split into two leaves and a root')
      t.alike(
        shape.leaves.map((leaf) => leaf.keys).sort((a, b) => a - b),
        [127, 128],
        count + ': rebalances the final siblings'
      )
    }
  }
})

test('import commits bounded batches at the two-level boundary', async function (t) {
  // Two-level capacity at t=128 is 255 + 256 * 255 entries. The writer commits
  // bounded batches, so the rightmost leaf is split at chunk boundaries and the
  // exact two-level maximum needs one extra leaf. Both cases must still be
  // legal and round-trip.
  for (const [count, levels, leaves] of [
    [65535, 3, 257],
    [65536, 3, 257]
  ]) {
    const source = await createImportBee(t)
    const destination = await createImportBee(t)

    {
      const w = source.write()
      for (let i = 0; i < count; i++) w.tryPut(binaryKey(i), binaryValue(i))
      await w.flush()
    }

    const expected = await readEntries(source)

    await destination.import(source)

    t.alike(await readEntries(destination), expected, count + ': round-trips every entry')

    const shape = await treeShape(destination)
    assertLegalTree(t, shape, 128, count + ' entries')

    t.is(shape.levels, levels, count + ': level count')
    t.is(shape.leaves.length, leaves, count + ': leaf count')
    t.ok(
      shape.nodes.every((node) => node.children === 0 || node.children === node.keys + 1),
      count + ': every internal node keeps one more child than keys'
    )
  }
})

test('import packs a custom destination degree', async function (t) {
  const source = await createImportBee(t)
  const destination = await createImportBee(t, { t: 5 })

  {
    const w = source.write()
    for (let i = 0; i < 300; i++) w.tryPut(binaryKey(i), binaryValue(i))
    await w.flush()
  }

  const expected = await readEntries(source)

  await destination.import(source)

  t.alike(await readEntries(destination), expected, 'copies every entry into the custom degree')
  t.is(destination.t, 5, 'destination keeps its custom degree')

  const shape = await treeShape(destination)
  assertLegalTree(t, shape, 5, 'custom degree 5')

  const seq = destination.core.length - 1
  t.is(decodeBlock(await destination.core.get(seq), seq).t, 5, 'persisted root block stores t = 5')
})

test('import preserves null, empty and oversized values', async function (t) {
  const source = await createImportBee(t)
  const destination = await createImportBee(t)

  const big = b4a.alloc(2 * 1024 * 1024 + 1).fill(0xab)
  const external = b4a.alloc(700 * 1024).fill(0xcd)

  {
    const w = source.write()
    w.tryPut(binaryKey(0), big)
    w.tryPut(binaryKey(1), null)
    w.tryPut(binaryKey(2), b4a.alloc(0))
    w.tryPut(binaryKey(3), external)
    for (let i = 4; i < 300; i++) w.tryPut(binaryKey(i), binaryValue(i))
    await w.flush()
  }

  const expected = await readEntries(source)

  await destination.import(source)

  t.alike(await readEntries(destination), expected, 'copies every value byte-for-byte')
  t.alike((await destination.get(binaryKey(0))).value, big, 'copies an oversized value')
  t.is((await destination.get(binaryKey(1))).value, null, 'preserves a null value')
  t.alike((await destination.get(binaryKey(2))).value, b4a.alloc(0), 'preserves an empty value')
  t.alike((await destination.get(binaryKey(3))).value, external, 'copies an external value')

  const shape = await treeShape(destination)
  t.is(shape.levels, 2, 'packs the entries into two levels')
  assertLegalTree(t, shape, 128, 'null, empty and oversized values')

  const blocks = await readBlocks(destination)
  const bigBlock = blocks.find(
    ({ block }) => block.values !== null && block.values.some((value) => b4a.equals(value, big))
  )
  t.ok(bigBlock !== undefined, 'stores the oversized value outside the node block')

  const holder = blocks.find(
    ({ block }) =>
      block.keys !== null && block.keys.some((key) => b4a.equals(key.key, binaryKey(0)))
  )
  t.ok(
    holder !== undefined &&
      holder.block.keys.find((key) => b4a.equals(key.key, binaryKey(0))).valuePointer !== null,
    'points the key at its external value block'
  )
})

test('import bounds wide node blocks', async function (t) {
  const source = await createImportBee(t)
  const destination = await createImportBee(t)
  const count = 400
  const wide = 64 * 1024

  for (let start = 0; start < count; start += 50) {
    const w = source.write()
    const end = Math.min(count, start + 50)

    for (let i = start; i < end; i++) {
      const key = b4a.alloc(wide)
      key.writeUInt32BE(i, 0)
      w.tryPut(key, null)
    }

    await w.flush()
  }

  const expected = await readEntries(source)

  await destination.import(source)

  t.alike(await readEntries(destination), expected, count + ': round-trips every wide key')

  const shape = await treeShape(destination)
  assertLegalTree(t, shape, 128, 'wide keys')
  t.ok(shape.levels >= 1, 'wide keys build a tree')

  let largest = 0
  for (let seq = 0; seq < destination.core.length; seq++) {
    const buffer = await destination.core.get(seq)
    if (buffer.byteLength > largest) largest = buffer.byteLength
  }

  t.ok(largest <= 15 * 1024 * 1024, 'keeps every wide-key block under the hypercore block limit')
})

test('import bounds near-limit values', async function (t) {
  const source = await createImportBee(t)
  const destination = await createImportBee(t)
  const external = b4a.alloc(2 * 1024).fill(0xcd)
  const nearLimit = b4a.alloc(15 * 1024 * 1024 - 1024).fill(0xab)

  // One source batch: the writer must not group the small external value with
  // the near-limit value into a block above the hypercore limit.
  {
    const w = source.write()
    w.tryPut(binaryKey(0), external)
    w.tryPut(binaryKey(1), nearLimit)
    await w.flush()
  }

  await destination.import(source)

  t.alike((await destination.get(binaryKey(0))).value, external, 'copies the external value')
  t.alike((await destination.get(binaryKey(1))).value, nearLimit, 'copies the near-limit value')

  let largest = 0
  let nearLimitBlock = null

  for (let seq = 0; seq < destination.core.length; seq++) {
    const buffer = await destination.core.get(seq)
    if (buffer.byteLength > largest) largest = buffer.byteLength

    const block = decodeBlock(buffer, seq)
    if (block.values !== null && block.values.some((value) => b4a.equals(value, nearLimit))) {
      nearLimitBlock = block
    }
  }

  t.ok(
    largest <= 15 * 1024 * 1024,
    'keeps every near-limit value block under the hypercore block limit'
  )
  t.ok(nearLimitBlock !== null, 'stores the near-limit value in its own block')
  t.is(
    nearLimitBlock === null ? -1 : nearLimitBlock.values.length,
    1,
    'does not group a smaller value with the near-limit value'
  )
})

test('import respects a small node cache', async function (t) {
  const source = await createImportBee(t)
  const destination = await createImportBee(t, { maxCacheSize: 8 })
  const count = 5000

  {
    const w = source.write()
    for (let i = 0; i < count; i++) w.tryPut(binaryKey(i), binaryValue(i))
    await w.flush()
  }

  const expected = await readEntries(source)

  await destination.import(source)

  t.alike(await readEntries(destination), expected, 'copies every entry with a tiny node cache')

  const cache = destination.context.getLocalContext().cache
  t.is(cache.maxSize, 8, 'keeps the configured cache bound')
  t.ok(cache.size <= cache.maxSize, 'evicts nodes instead of retaining the whole imported tree')
})

test('import commits bounded change batches', async function (t) {
  const { source, entries: expected } = await seedImportSource(t, 6000)
  const destination = await createImportBee(t)

  await destination.import(source)

  t.alike(await readEntries(destination), expected, 'copies every entry')

  const changes = []

  for await (const change of destination.createChangesStream()) {
    changes.push(change)
  }

  t.ok(changes.length > 1, 'splits the import into multiple changes')
  t.is(changes[changes.length - 1].tail, null, 'the oldest change has no predecessor')
  t.is(changes[0].head.length, destination.core.length, 'the newest change ends at the import head')
  t.ok(
    changes[0].batch.length < destination.core.length,
    'the newest batch does not span every block written by the import'
  )

  let prefix = 0
  let previous = null

  for (let i = 0; i < changes.length; i++) {
    const change = changes[i]

    if (previous !== null) {
      t.alike(change.head, previous.tail, i + ': change head matches the next tail')
    }
    previous = change

    t.ok(change.batch.length > 0, i + ': batch has blocks')

    for (let j = 0; j < change.batch.length; j++) {
      const block = change.batch[j]
      t.is(block.batch.start, j, i + ': batch start spans only its change')
      t.is(
        block.batch.end,
        change.batch.length - 1 - j,
        i + ': batch end records the block position'
      )
    }
  }

  // Changes stream newest first; walk them oldest first to check that every
  // committed version is a readable prefix of the import.
  for (let i = changes.length - 1; i >= 0; i--) {
    const change = changes[i]
    const view = destination.checkout(change.head)

    try {
      const entries = await readEntries(view)
      t.ok(entries.length > prefix, i + ': change reads a longer prefix')
      t.alike(entries, expected.slice(0, entries.length), i + ': change head reads a prefix')
      prefix = entries.length
    } finally {
      await view.close()
    }
  }

  t.is(prefix, expected.length, 'the changes cover every imported entry')

  const head = destination.head()
  const extraKey = binaryKey(100000)
  const extraValue = b4a.from('after import')

  {
    const w = destination.write()
    w.tryPut(extraKey, extraValue)
    await w.flush()
  }

  t.alike(
    await readEntries(destination),
    expected.concat([{ key: extraKey, value: extraValue }]),
    'a normal write appends to the imported tree'
  )
  t.ok(destination.head().length > head.length, 'the new write advances the head')
})
