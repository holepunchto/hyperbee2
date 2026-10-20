const test = require('brittle')
const b4a = require('b4a')
const Corestore = require('corestore')
const Hypercore = require('hypercore')
const Bee = require('../')

const hex = (k) => b4a.toString(k, 'hex')
const key = (i) => b4a.from('key/' + String(i).padStart(4, '0'))
const value = (i) => b4a.from('value-' + i)

async function source(store, n) {
  const a = new Bee(store.namespace('a'))
  await a.ready()
  const w = a.write()
  for (let i = 0; i < n; i++) w.tryPut(key(i), value(i))
  await w.flush()
  return a
}

// a core holding the same blocks, the target of a mapping
async function copyCore(store, src, name) {
  const dst = store.get({ name })
  await dst.ready()
  for (let i = 0; i < src.length; i++) await dst.append(await src.get(i))
  return dst
}

async function readAll(bee, n, t, msg) {
  for (let i = 0; i < n; i++) {
    const node = await bee.get(key(i))
    if (!node || !b4a.equals(node.value, value(i))) return t.fail(msg + ': key ' + i)
  }
  t.pass(msg)
}

test('mappings - the viewed core is read through its mapping', async function (t) {
  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close())

  const a = await source(store, 50)
  const copy = await copyCore(store, a.core, 'copy')
  await a.core.clear(0, a.core.length)

  const mappings = [{ from: a.core.key, to: copy.key }]
  const b = new Bee(store.namespace('b'), { key: a.core.key, mappings, writable: false })
  await b.ready()

  t.alike(b.core.key, copy.key, 'the view sits on the mapped core')
  await readAll(b, 50, t, 'every entry reads through the mapping')
  t.alike((await b.cores()).map(hex), [hex(copy.key)], 'cores() lists the mapped core only')

  await b.close()
})

test('mappings - the references of a writer resolve to the mapped core', async function (t) {
  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close())

  const a = await source(store, 50)
  const copy = await copyCore(store, a.core, 'copy')
  const mappings = [{ from: a.core.key, to: copy.key }]

  const w = new Bee(store.namespace('w'), { mappings })
  await w.ready()
  w.move({ key: a.core.key, length: a.core.length })
  t.alike(w.core.key, copy.key, 'moving onto the original lands on the mapping')

  const batch = w.write()
  for (let i = 50; i < 60; i++) batch.tryPut(key(i), value(i))
  await batch.flush()

  const referenced = (await w.cores({ local: false })).map(hex)
  t.absent(referenced.includes(hex(a.core.key)), 'the original is not among the cores read')
  await w.close()

  await a.core.clear(0, a.core.length)

  const again = new Bee(store.namespace('w'))
  await again.ready()
  t.alike(
    (await again.cores({ local: false })).map(hex),
    referenced,
    'the persisted mappings resolve the table'
  )
  await readAll(again, 60, t, 'a reader without the mappings reads everything via the checkpoint')
  await again.close()
})

test('mappings - an existing table is remapped on load and persisted on the next flush', async function (t) {
  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close())

  const a = await source(store, 50)

  // a writer that referenced the original before any mapping existed
  const w = new Bee(store.namespace('w'))
  await w.ready()
  w.move({ key: a.core.key, length: a.core.length })
  {
    const batch = w.write()
    for (let i = 50; i < 60; i++) batch.tryPut(key(i), value(i))
    await batch.flush()
  }
  t.ok(
    (await w.cores({ local: false })).some((k) => b4a.equals(k, a.core.key)),
    'table names the original'
  )
  await w.close()

  const copy = await copyCore(store, a.core, 'copy')
  const mappings = [{ from: a.core.key, to: copy.key }]

  const mapped = new Bee(store.namespace('w'), { mappings })
  await mapped.ready()
  const cores = (await mapped.cores({ local: false })).map(hex)
  t.ok(cores.includes(hex(copy.key)), 'the loaded table is remapped')
  t.absent(cores.includes(hex(a.core.key)), 'the original is gone from the loaded table')
  await readAll(mapped, 60, t, 'reads go through the remapped table')

  {
    const batch = mapped.write()
    batch.tryPut(key(60), value(60))
    await batch.flush()
  }
  await mapped.close()

  await a.core.clear(0, a.core.length)

  const plain = new Bee(store.namespace('w'))
  await plain.ready()
  const persisted = (await plain.cores({ local: false })).map(hex)
  t.ok(persisted.includes(hex(copy.key)), 'the mappings were persisted by the flush')
  t.absent(persisted.includes(hex(a.core.key)), 'without them given, the original is still gone')
  await readAll(plain, 61, t, 'a reader without the mappings reads through the persisted ones')
  await plain.close()
})

test('mappings - the persisted mapping survives an undo below it', async function (t) {
  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close())

  const a = await source(store, 50)

  const w = new Bee(store.namespace('w'))
  await w.ready()
  w.move({ key: a.core.key, length: a.core.length })
  for (let i = 50; i < 55; i++) {
    const batch = w.write()
    batch.tryPut(key(i), value(i))
    await batch.flush()
  }
  await w.close()

  const copy = await copyCore(store, a.core, 'copy')
  const mappings = [{ from: a.core.key, to: copy.key }]

  const mapped = new Bee(store.namespace('w'), { mappings })
  await mapped.ready()
  {
    const batch = mapped.write()
    batch.tryPut(key(55), value(55))
    await batch.flush()
  }
  await mapped.close()

  await a.core.clear(0, a.core.length)

  // a reader without the mappings, viewing a root written before they were
  // recorded, still resolves the table from its latest checkpoint
  const plain = new Bee(store.namespace('w'))
  await plain.ready()
  const old = plain.undo(3)
  await readAll(old, 53, t, 'an undone root reads through the persisted mapping')
  t.is(await old.get(key(53)), null, 'the undone writes are gone')
  t.alike(
    (await old.cores({ local: false })).map(hex),
    [hex(copy.key)],
    'the undone view resolves the original to the copy'
  )
  await old.close()
  await plain.close()
})

test('mappings - a mapping met in another writer is recorded locally', async function (t) {
  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close())

  const a = await source(store, 50)
  const copy = await copyCore(store, a.core, 'copy')
  const mappings = [{ from: a.core.key, to: copy.key }]

  // b writes on top of a with the mapping, recording it in its checkpoint
  const b = new Bee(store.namespace('b'), { mappings })
  await b.ready()
  b.move({ key: a.core.key, length: a.core.length })
  {
    const batch = b.write()
    batch.tryPut(key(50), value(50))
    await batch.flush()
  }

  await a.core.clear(0, a.core.length)

  // c is never given the mapping and only ever sees b's head
  const c = new Bee(store.namespace('c'))
  await c.ready()
  c.move({ key: b.context.local.key, length: b.context.local.length })
  {
    const batch = c.write()
    batch.tryPut(key(51), value(51))
    await batch.flush()
  }
  await readAll(c, 52, t, 'c reads a through the mapping b recorded')
  await c.close()
  await b.close()

  const again = new Bee(store.namespace('c'))
  await again.ready()
  await readAll(again, 52, t, "a reopen of c reads a through c's own checkpoint")
  t.absent(
    (await again.cores()).some((k) => b4a.equals(k, a.core.key)),
    'the original is not opened anywhere'
  )
  await again.close()
})

test('mappings - a remote reads the whole tree after the original data is gone', async function (t) {
  const origin = new Corestore(await t.tmp())
  t.teardown(() => origin.close())

  const a = await source(origin, 200)

  const w = new Bee(origin.namespace('w'))
  await w.ready()
  w.move({ key: a.core.key, length: a.core.length })
  for (let i = 200; i < 220; i++) {
    const batch = w.write()
    batch.tryPut(key(i), value(i))
    await batch.flush()
  }
  const wKey = w.context.local.key
  await w.close()

  const copy = await copyCore(origin, a.core, 'copy')
  const mappings = [{ from: a.core.key, to: copy.key }]

  const mapped = new Bee(origin.namespace('w'), { mappings })
  await mapped.ready()
  {
    const batch = mapped.write()
    batch.tryPut(key(220), value(220))
    await batch.flush()
  }
  await mapped.close()

  await a.core.clear(0, a.core.length)
  t.is(await a.core.has(0), false, 'the original holds no data any more')
  t.is(copy.contiguousLength, copy.length, 'the copy still holds every block')

  // a remote that only ever sees the origin
  const remote = new Corestore(await t.tmp())
  t.teardown(() => remote.close())
  const stop = replicateStores(origin, remote)

  const reader = new Bee(remote.session(), { key: wKey, mappings, writable: false })
  await reader.ready()
  await reader.core.update({ wait: true })
  t.is(reader.core.length, 21, 'the remote learnt the head')

  let count = 0
  for await (const { key: k, value: v } of reader.createReadStream()) {
    const expected = b4a.equals(k, key(count)) && b4a.equals(v, value(count))
    if (!expected) return t.fail('bad entry ' + count)
    count++
  }
  t.is(count, 221, 'the whole tree was read through the mapping')
  await readAll(reader, 221, t, 'every point read resolves too')
  t.alike(
    (await reader.cores()).map(hex).sort(),
    [hex(wKey), hex(copy.key)].sort(),
    'only the writer and the copy were opened'
  )
  t.absent(
    await remote.storage.hasCore(Hypercore.discoveryKey(a.core.key)),
    'the remote never asked for the original'
  )
  await reader.close()

  await stop()

  // a second remote never given the mappings: a head written before they were
  // recorded runs into the cleared original, the head that carries them does not
  const other = new Corestore(await t.tmp())
  t.teardown(() => other.close())
  const stopOther = replicateStores(origin, other)

  {
    const core = other.get({ key: wKey })
    await core.update({ wait: true })
    await core.download({ start: 0, end: core.length }).done()
    await core.close()
  }

  const before = new Bee(other.session(), { key: wKey, writable: false })
  await before.ready()
  before.move({ length: 20 })
  await t.exception(
    before.get(key(0), { wait: false }),
    /BLOCK_NOT_AVAILABLE/,
    'before the record, a reader without the mappings hits missing data'
  )
  await before.close()

  const after = new Bee(other.session(), { key: wKey, writable: false })
  await after.ready()
  await readAll(after, 221, t, 'at the recorded head, the same reader resolves everything')
  await after.close()

  await stopOther()
})

test('mappings - move, checkout and write onto an original resolve synchronously', async function (t) {
  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close())

  const a = await source(store, 50)
  const copy = await copyCore(store, a.core, 'copy')
  await a.core.clear(0, a.core.length)

  const mappings = [{ from: a.core.key, to: copy.key }]
  const w = new Bee(store.namespace('w'), { mappings })
  await w.ready()

  w.move({ key: a.core.key, length: a.core.length })
  t.alike(w.core.key, copy.key, 'the bee sits on the mapped core right after the move')
  t.alike(w.head().key, copy.key, 'the head is on the mapped core')
  await readAll(w, 50, t, 'reads go through the mapping')

  const view = w.checkout({ key: a.core.key, length: a.core.length })
  t.alike(view.head().key, copy.key, 'a checkout onto the original sits on the mapped core')
  await readAll(view, 50, t, 'and reads through it')
  await view.close()

  const batch = w.write({ key: a.core.key, length: a.core.length })
  batch.tryPut(key(50), value(50))
  await batch.flush()
  await readAll(w, 51, t, 'a batch keyed by the original built on the mapped core')
  t.absent(
    (await w.cores()).some((k) => b4a.equals(k, a.core.key)),
    'the original is not among the cores read'
  )
  await w.close()
})

test('mappings - added after open, a read in flight survives its core being remapped', async function (t) {
  const origin = new Corestore(await t.tmp())
  t.teardown(() => origin.close())

  const a = await source(origin, 50)
  const copy = await copyCore(origin, a.core, 'copy')

  const w = new Bee(origin.namespace('w'))
  await w.ready()
  w.move({ key: a.core.key, length: a.core.length })
  {
    const batch = w.write()
    batch.tryPut(key(50), value(50))
    await batch.flush()
  }
  const wKey = w.context.local.key
  await w.close()

  // the remote holds the writer but nothing of the core it references
  const remote = new Corestore(await t.tmp())
  t.teardown(() => remote.close())
  {
    const stop = replicateStores(origin, remote)
    const core = remote.get({ key: wKey })
    await core.update({ wait: true })
    await core.download({ start: 0, end: core.length }).done()
    await core.close()
    await stop()
  }

  const reader = new Bee(remote.session(), { key: wKey, writable: false })
  await reader.ready()

  // a read that has to wait for a peer to serve the referenced core
  const read = reader.get(key(0))
  for (let i = 0; i < 100 && reader.context.opened[0] === null; i++) await immediate()
  const stale = reader.context.opened[0]
  t.alike(stale.key, a.core.key, 'the read opened the core as referenced')

  reader.addMappings([{ from: a.core.key, to: copy.key }])
  const hc = reader.context.getCore(1)
  t.alike(hc.key, copy.key, 'the next open resolves to the mapped core')
  t.absent(stale.closing, 'the superseded session is left to the reads holding it')

  const stop = replicateStores(origin, remote)
  const node = await read
  t.alike(node.value, value(0), 'the read issued before the remap still resolved')

  await readAll(reader, 51, t, 'later reads go through the mapping')
  t.alike(
    (await reader.cores()).map(hex).sort(),
    [hex(wKey), hex(copy.key)].sort(),
    'only the writer and the mapped core are listed'
  )
  await reader.close()
  await stop()
})

test('mappings - cores({ recursive: true }) walks the tables of every core reached', async function (t) {
  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close())

  // b links a, then clears and rewrites the tree: nothing in b's tree points
  // into a any more but b's table still names it, and c builds on b
  const a = await source(store, 50)

  const b = new Bee(store.namespace('b'))
  await b.ready()
  b.move({ key: a.core.key, length: a.core.length })
  {
    const batch = b.write()
    batch.tryPut(key(50), value(50))
    await batch.flush()
  }
  {
    const batch = b.write()
    batch.tryClear()
    for (let i = 0; i < 50; i++) batch.tryPut(key(i), value(100 + i))
    await batch.flush()
  }

  const c = new Bee(store.namespace('c'))
  await c.ready()
  c.move({ key: b.context.local.key, length: b.context.local.length })
  {
    const batch = c.write()
    batch.tryPut(key(50), value(50))
    await batch.flush()
  }

  const direct = (await c.cores({ local: false })).map(hex).sort()
  const reachable = (await c.cores({ local: false, recursive: true })).map(hex).sort()
  t.alike(direct, [hex(b.context.local.key)], "c's table names b only")
  t.alike(
    reachable,
    [hex(a.core.key), hex(b.context.local.key)].sort(),
    'recursively a is reached through b'
  )

  // the flow of a migration: find what is reachable, copy it, map it in one go
  const copy = await copyCore(store, a.core, 'copy')
  await a.core.clear(0, a.core.length)
  c.addMappings([{ from: a.core.key, to: copy.key }])

  t.alike(
    (await c.cores({ local: false, recursive: true })).map(hex).sort(),
    [hex(copy.key), hex(b.context.local.key)].sort(),
    'the walk reports the mapped core'
  )
  for (let i = 0; i < 50; i++) {
    const node = await c.get(key(i))
    if (!node || !b4a.equals(node.value, value(100 + i))) return t.fail('bad value for key ' + i)
  }
  t.alike((await c.get(key(50))).value, value(50), 'every entry still reads')

  await c.close()
  await b.close()
})

function replicateStores(a, b) {
  const s1 = a.replicate(true)
  const s2 = b.replicate(false)
  s1.pipe(s2).pipe(s1)
  s1.on('error', noop)
  s2.on('error', noop)
  return async () => {
    const closed = Promise.all([
      new Promise((resolve) => s1.once('close', resolve)),
      new Promise((resolve) => s2.once('close', resolve))
    ])
    s1.destroy()
    s2.destroy()
    await closed
  }
}

function immediate() {
  return new Promise((resolve) => setImmediate(resolve))
}

function noop() {}
