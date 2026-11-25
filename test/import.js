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

function importState(bee) {
  return { length: bee.core.length, head: bee.head() }
}

function alikeImportState(t, bee, state, message) {
  t.is(bee.core.length, state.length, message + ': core length unchanged')
  t.alike(bee.head(), state.head, message + ': head unchanged')
}

function trackImportResources(source, destination, onFlush = null) {
  const tracked = {
    view: null,
    stream: null,
    viewClosed: false,
    streamClosed: false,
    batches: [],
    flushes: 0
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
  destination.write = function (opts) {
    const batch = write.call(this, opts)
    tracked.batches.push(batch)

    const flush = batch.flush
    batch.flush = function (...flushArgs) {
      const flushed = flush.apply(this, flushArgs)
      tracked.flushes++
      if (onFlush !== null) onFlush(tracked.flushes, flushed)
      return flushed
    }

    return batch
  }

  tracked.restore = function () {
    source._makeView = makeView
    destination.write = write
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

  const flushes = []
  const write = destination.write

  destination.write = function (opts) {
    const batch = write.call(this, opts)
    const flush = batch.flush.bind(batch)

    batch.flush = function () {
      let bytes = 0

      for (const op of batch.ops) {
        bytes += op.key.byteLength
        if (op.value !== null) bytes += op.value.byteLength
      }

      flushes.push({
        entries: batch.ops.length,
        bytes,
        keys: batch.ops.map((op) => op.key.toString('hex'))
      })
      return flush()
    }

    return batch
  }

  try {
    await destination.import(source)
  } finally {
    destination.write = write
  }

  t.alike(await readEntries(destination), expected, 'copies every ordered key/value pair')
  t.alike((await destination.get(binaryKey(7))).value, overwritten, 'copies overwritten value')
  t.is(await destination.get(binaryKey(100)), null, 'does not copy a deleted key')
  t.is((await destination.get(binaryKey(2))).value, null, 'preserves a null value')
  t.alike((await destination.get(binaryKey(1))).value, b4a.alloc(0), 'preserves an empty value')
  t.alike((await destination.get(binaryKey(2050))).value, big, 'copies an oversized value')

  const pair = [2051, 2052].map((i) => binaryKey(i).toString('hex'))
  const oversized = flushes.filter((f) => f.bytes > 1024 * 1024)

  t.ok(flushes.length > 1, 'copied in multiple chunks')
  t.ok(
    flushes.some((f) => f.entries === 1024),
    'filled a chunk up to the entry-count target'
  )
  t.ok(oversized.length > 0, 'exceeded the byte target')
  t.alike(
    flushes.filter((f) => f.entries > 1 && f.bytes > 1024 * 1024),
    [],
    'kept every multi-entry chunk within the byte target'
  )
  t.ok(
    oversized.every((f) => f.entries === 1),
    'flushed every oversized chunk as a singleton'
  )
  t.is(
    flushes.filter((f) => pair.every((key) => f.keys.includes(key))).length,
    0,
    'split sub-limit values that exceed the byte target together'
  )
  t.is(flushes.filter((f) => f.entries === 0).length, 0, 'never flushed an empty chunk')
  t.ok(flushes[flushes.length - 1].entries < 1024, 'flushed a short tail chunk')
})

test('import keeps the pinned source view when the live source advances', async function (t) {
  const source = await createImportBee(t)
  const destination = await createImportBee(t)

  {
    const w = source.write()
    for (let i = 0; i < 1500; i++) w.tryPut(binaryKey(i), binaryValue(i))
    await w.flush()
  }

  const expected = await readEntries(source)
  const lateKey = binaryKey(9999)
  const lateValue = b4a.from('late-entry')

  let flushedFirst
  const firstFlush = new Promise((resolve) => {
    flushedFirst = resolve
  })
  let releaseImport
  const released = new Promise((resolve) => {
    releaseImport = resolve
  })

  let paused = false
  const write = destination.write

  destination.write = function (opts) {
    const batch = write.call(this, opts)
    const flush = batch.flush.bind(batch)

    batch.flush = async function () {
      const result = await flush()

      if (!paused) {
        paused = true
        flushedFirst()
        await released
      }

      return result
    }

    return batch
  }

  try {
    const importing = destination.import(source)

    await firstFlush
    {
      const w = source.write()
      w.tryPut(lateKey, lateValue)
      await w.flush()
    }
    releaseImport()

    await importing
  } finally {
    destination.write = write
  }

  t.alike(await readEntries(destination), expected, 'imports only the pinned source view')
  t.is(await destination.get(lateKey), null, 'ignores the entry appended after the first flush')
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

test('import rejects the original source read error after a flush', async function (t) {
  const { source, entries: expected, state: sourceState } = await seedImportSource(t)
  const destination = await createImportBee(t)
  const fault = new Error('source read failed')
  const read = injectSourceReadFailure(source, fault)
  const append = instrumentAppend(destination)
  const tracked = trackImportResources(source, destination, (flushes, flushed) => {
    if (flushes === 1) flushed.then(() => read.fail())
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
  t.is(tracked.flushes, 1, 'flushes only the chunk completed before the failure')
  t.is(append.calls, 1, 'appends no batch after the failed read')
  t.ok(tracked.viewClosed, 'closes its owned source view')
  await assertImportStreamReleased(t, tracked, 'source read failure')
  t.is(tracked.batches.length, 2, 'keeps the flushed batch and the unflushed one')
  t.ok(
    tracked.batches.every((batch) => batch.closed),
    'closes both the flushed and the unflushed batch'
  )
  assertImportLeavesBothOpen(t, source, destination)
  await assertSourceIntactAndRetry(t, source, sourceState, expected)
})

test('import rejects the original destination append error after a flush', async function (t) {
  const { source, entries: expected, state: sourceState } = await seedImportSource(t)
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
  t.is(tracked.flushes, 2, 'attempts no flush after the failed append')
  t.is(append.calls, 2, 'appends no batch after the failed append')
  t.ok(tracked.viewClosed, 'closes its owned source view')
  await assertImportStreamReleased(t, tracked, 'destination append failure')
  t.is(tracked.batches.length, 2, 'keeps the flushed batch and the failed one')
  t.ok(
    tracked.batches.every((batch) => batch.closed),
    'closes both the flushed and the failed batch'
  )
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
  t.is(tracked.batches.length, 1, 'uses one batch for a short source')
  t.ok(
    tracked.batches.every((batch) => batch.closed),
    'closes the batch after the final flush'
  )
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
