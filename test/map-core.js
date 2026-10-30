const test = require('brittle')
const b4a = require('b4a')
const Corestore = require('corestore')
const Hypercore = require('hypercore')
const Bee = require('../')

const hex = (k) => b4a.toString(k, 'hex')
const key = (i) => b4a.from('key/' + String(i).padStart(4, '0'))
const value = (i) => b4a.from('value-' + i)

const openBee = (store, opts) => new Bee(store, { mappings: true, ...opts })

async function source(store, n) {
  const a = openBee(store.namespace('a'))
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

// a writer whose tree reaches a, and a copy of a to map it to
async function setup(store, n = 50) {
  const a = await source(store, n)

  const w = openBee(store.namespace('w'))
  await w.ready()
  w.move({ key: a.core.key, length: a.core.length })
  {
    const batch = w.write()
    batch.tryPut(key(n), value(n))
    await batch.flush()
  }

  const copy = await copyCore(store, a.core, 'copy')
  const mappings = [{ from: a.core.key, to: copy.key }]

  return { a, w, copy, mappings }
}

test('mappings - a write with mappings records them, readers read through them', async function (t) {
  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close())

  const { a, w, copy, mappings } = await setup(store)
  const local = w.context.local
  const length = local.length

  await w.write({ mappings }).flush()
  t.is(local.length, length + 1, 'with nothing else to write, one block')
  t.alike(w.head(), { key: local.key, length: local.length }, 'the head moved onto it')

  await w.write({ mappings }).flush()
  t.is(local.length, length + 1, 'mappings already written write nothing')
  await w.close()

  await a.core.clear(0, a.core.length)

  const r = openBee(store.namespace('w'), { writable: false })
  await r.ready()
  await readAll(r, 51, t, 'a reader reads through the mappings of the checkpoint')
  t.alike((await r.cores({ local: false })).map(hex), [hex(copy.key)], 'and lists the copy only')
  await r.close()

  const again = openBee(store.namespace('w'))
  await again.ready()
  await again.write({ mappings }).flush()
  t.is(again.context.local.length, length + 1, 'nor after a reopen')
  await again.close()
})

test('mappings - a write with ops carries the mappings in its checkpoint', async function (t) {
  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close())

  const { a, w, copy, mappings } = await setup(store)
  const length = w.context.local.length

  const batch = w.write({ mappings })
  batch.tryPut(key(51), value(51))
  await batch.flush()
  t.is(w.context.local.length, length + 1, 'one block for both')
  t.absent(
    (await w.cores({ local: false })).some((k) => b4a.equals(k, a.core.key)),
    'the original is not among the cores read'
  )
  await w.close()

  await a.core.clear(0, a.core.length)

  const r = openBee(store.namespace('w'), { writable: false })
  await r.ready()
  await readAll(r, 52, t, 'a reader reads everything through the copy')
  t.ok((await r.cores()).some((k) => b4a.equals(k, copy.key)))
  await r.close()
})

test('mappings - an undo below the record reads through the mappings of the head', async function (t) {
  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close())

  const { a, w, copy, mappings } = await setup(store)
  for (let i = 51; i < 55; i++) {
    const batch = w.write()
    batch.tryPut(key(i), value(i))
    await batch.flush()
  }
  await w.write({ mappings }).flush()
  await w.close()

  await a.core.clear(0, a.core.length)

  const r = openBee(store.namespace('w'), { writable: false })
  await r.ready()
  await readAll(r, 55, t, 'the head reads through the copy')

  const old = r.undo(4)
  await readAll(old, 52, t, 'an undone root reads through the mappings the head carried')
  t.is(await old.get(key(54)), null, 'the undone writes are gone')
  t.alike((await old.cores({ local: false })).map(hex), [hex(copy.key)])
  await old.close()
  await r.close()
})

test('mappings - a mapping met in another writer is recorded locally', async function (t) {
  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close())

  const { a, w, mappings } = await setup(store)
  await w.write({ mappings }).flush()

  await a.core.clear(0, a.core.length)

  // c is never given the mapping and only ever sees w's head
  const c = openBee(store.namespace('c'))
  await c.ready()
  c.move({ key: w.context.local.key, length: w.context.local.length })
  {
    const batch = c.write()
    batch.tryPut(key(51), value(51))
    await batch.flush()
  }
  await readAll(c, 52, t, 'c reads a through the mapping w recorded')
  await c.close()
  await w.close()

  const again = openBee(store.namespace('c'), { writable: false })
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

  const { a, w, copy, mappings } = await setup(origin, 200)
  const wKey = w.context.local.key
  const before = w.context.local.length
  await w.write({ mappings }).flush()
  await w.close()

  await a.core.clear(0, a.core.length)

  const remote = new Corestore(await t.tmp())
  t.teardown(() => remote.close())
  const stop = replicateStores(origin, remote)

  const reader = openBee(remote.session(), { key: wKey, writable: false })
  await reader.ready()
  await reader.core.update({ wait: true })

  let count = 0
  for await (const { key: k, value: v } of reader.createReadStream()) {
    const expected = b4a.equals(k, key(count)) && b4a.equals(v, value(count))
    if (!expected) return t.fail('bad entry ' + count)
    count++
  }
  t.is(count, 201, 'the whole tree was read through the mapping')
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

  // a head from before the record has a table without the mappings
  const other = new Corestore(await t.tmp())
  t.teardown(() => other.close())
  const stopOther = replicateStores(origin, other)
  {
    const core = other.get({ key: wKey })
    await core.update({ wait: true })
    await core.download({ start: 0, end: core.length }).done()
    await core.close()
  }

  const old = openBee(other.session(), { key: wKey, writable: false })
  await old.ready()
  old.move({ length: before })
  await t.exception(
    old.get(key(0), { wait: false }),
    /BLOCK_NOT_AVAILABLE/,
    'before the record the original is read'
  )
  await old.close()
  await stopOther()
})

test('mappings - move, checkout and write onto an original resolve once known', async function (t) {
  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close())

  const { a, w, copy, mappings } = await setup(store)
  await w.write({ mappings }).flush()
  await a.core.clear(0, a.core.length)

  const view = w.checkout({ key: a.core.key, length: a.core.length })
  t.alike(view.head().key, copy.key, 'a checkout onto the original sits on the copy')
  await readAll(view, 50, t, 'and reads through it')
  await view.close()

  const batch = w.write({ key: a.core.key, length: a.core.length })
  batch.tryPut(key(50), value(50))
  await batch.flush()
  await readAll(w, 51, t, 'a batch keyed by the original builds on the copy')

  w.move({ key: a.core.key, length: a.core.length })
  t.alike(w.core.key, copy.key, 'a move onto the original lands on the copy')
  await w.close()
})

test('mappings - a read in flight survives its core being remapped', async function (t) {
  const origin = new Corestore(await t.tmp())
  t.teardown(() => origin.close())

  const { a, w, copy, mappings } = await setup(origin)
  const wKey = w.context.local.key

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

  const reader = openBee(remote.session(), { key: wKey, writable: false })
  await reader.ready()

  const read = reader.get(key(0))
  const deadline = Date.now() + 5000
  while (!reader.context.opened[0]) {
    if (Date.now() > deadline) return t.fail('the read never opened the referenced core')
    await immediate()
  }
  const stale = reader.context.opened[0]
  t.alike(stale.key, a.core.key, 'the read opened the core as referenced')

  // the writer records the mapping, the reader moves onto that head
  await w.write({ mappings }).flush()
  const stop = replicateStores(origin, remote)
  await reader.core.update({ wait: true })
  reader.move({ length: reader.core.length })
  await reader.get(key(50))

  t.alike(reader.context.getCore(1).key, copy.key, 'the next open resolves to the copy')
  t.absent(stale.closing, 'the superseded session is left to the reads holding it')

  t.alike((await read).value, value(0), 'the read issued before the remap still resolved')
  await readAll(reader, 51, t, 'later reads go through the mapping')

  await reader.close()
  await w.close()
  await stop()
})

test('mappings - cores({ recursive: true }) walks the tables of every core reached', async function (t) {
  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close())

  // b links a, then clears and rewrites the tree: nothing in b's tree points
  // into a any more but b's table still names it, and c builds on b
  const a = await source(store, 50)

  const b = openBee(store.namespace('b'))
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

  const c = openBee(store.namespace('c'))
  await c.ready()
  c.move({ key: b.context.local.key, length: b.context.local.length })
  {
    const batch = c.write()
    batch.tryPut(key(50), value(50))
    await batch.flush()
  }

  t.alike(
    (await c.cores({ local: false })).map(hex),
    [hex(b.context.local.key)],
    "c's table names b only"
  )
  t.alike(
    (await c.cores({ local: false, recursive: true })).map(hex).sort(),
    [hex(a.core.key), hex(b.context.local.key)].sort(),
    'recursively a is reached through b'
  )

  // find what is reachable, copy it, record it
  const copy = await copyCore(store, a.core, 'copy')
  await a.core.clear(0, a.core.length)
  await c.write({ mappings: [{ from: a.core.key, to: copy.key }] }).flush()

  t.alike(
    (await c.cores({ local: false, recursive: true })).map(hex).sort(),
    [hex(copy.key), hex(b.context.local.key)].sort(),
    'the walk reports the copy'
  )
  for (let i = 0; i < 50; i++) {
    const node = await c.get(key(i))
    if (!node || !b4a.equals(node.value, value(100 + i))) return t.fail('bad value for key ' + i)
  }
  t.alike((await c.get(key(50))).value, value(50), 'every entry still reads')

  await c.close()
  await b.close()
})

test('mappings - the core table keeps the keys as they were before the mapping', async function (t) {
  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close())

  const { a, w, copy, mappings } = await setup(store)
  await w.write({ mappings }).flush()

  // the tree moves onto the original, so it now reads through the copy
  w.move({ key: a.core.key, length: a.core.length })
  t.alike(w.core.key, copy.key, 'the tree sits on the copy')

  const batch = w.write()
  batch.tryPut(key(60), value(60))
  await batch.flush()

  const table = (keys) => keys.map(hex)
  const local = w.context.getLocalContext()
  t.ok(
    table(local.cores.map((c) => c.key)).includes(hex(a.core.key)),
    'the original is in the table'
  )
  t.absent(table(local.cores.map((c) => c.key)).includes(hex(copy.key)), 'the copy never is')
  await w.close()

  await a.core.clear(0, a.core.length)

  const r = openBee(store.namespace('w'), { writable: false })
  await r.ready()
  await r.cores()
  t.absent(
    table(r.context.cores.map((c) => c.key)).includes(hex(copy.key)),
    'nor in the checkpoint written'
  )
  t.alike((await r.get(key(60))).value, value(60))
  await readAll(r, 50, t, 'and every entry of the original reads through the mapping')
  await r.close()
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

test('mappings - disabled by default, a write ignores the mappings it is given', async function (t) {
  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close())

  const { a, w, copy, mappings } = await setup(store)
  await w.close()

  const off = new Bee(store.namespace('w'))
  await off.ready()
  const length = off.context.local.length

  await off.write({ mappings }).flush()
  t.is(off.context.local.length, length, 'nothing to write, no block')

  const batch = off.write({ mappings })
  batch.tryPut(key(51), value(51))
  await batch.flush()
  t.is(off.context.local.length, length + 1, 'a write with ops still lands')
  t.ok(
    (await off.cores({ local: false })).some((k) => b4a.equals(k, a.core.key)),
    'the original is still read'
  )
  t.absent(
    (await off.cores({ local: false })).some((k) => b4a.equals(k, copy.key)),
    'the copy is not'
  )

  await off.close()

  await a.core.clear(0, a.core.length)
  const r = new Bee(store.namespace('w'), { writable: false })
  await r.ready()
  await t.exception(
    r.get(key(0), { wait: false }),
    /BLOCK_NOT_AVAILABLE/,
    'the mapping was never recorded'
  )
  await r.close()
})

test('mappings - disabled by default, a reader ignores the mappings in the tree', async function (t) {
  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close())

  const { a, w, copy, mappings } = await setup(store)
  await w.write({ mappings }).flush()
  await w.close()

  const off = new Bee(store.namespace('w'), { writable: false })
  await off.ready()
  await readAll(off, 51, t, 'reads the original while it is there')
  t.alike(
    (await off.cores({ local: false })).map(hex),
    [hex(a.core.key)],
    'and lists the original, not the copy'
  )

  await off.close()

  await a.core.clear(0, a.core.length)
  const again = new Bee(store.namespace('w'), { writable: false })
  await again.ready()
  await t.exception(
    again.get(key(0), { wait: false }),
    /BLOCK_NOT_AVAILABLE/,
    'the mapping is not followed'
  )
  await again.close()

  const on = openBee(store.namespace('w'), { writable: false })
  await on.ready()
  await readAll(on, 51, t, 'an opted-in reader of the same tree follows it')
  t.alike((await on.cores({ local: false })).map(hex), [hex(copy.key)])
  await on.close()
})

// a store may hand back a session that only learns its key once open, as one with a preload does
test('two reads of a core not yet open in one tick share the session', async function (t) {
  const dir = await t.tmp()
  const store = new Corestore(dir)
  const { a, w } = await setup(store)
  await w.write().flush()
  const head = { key: w.context.local.key, length: w.context.local.length }
  await w.close()
  await a.close()
  await store.close()

  const cold = new Corestore(dir)
  t.teardown(() => cold.close())
  const ns = cold.namespace('w')
  const bee = openBee(lateKeys(ns), { writable: false })
  await bee.ready()
  const view = bee.checkout(head)
  await view.ready()
  const ctx = view.context
  await ctx.updateMaybe(view.config, 1, null)
  t.ok(ctx.cores.length > 0, 'the head reaches a core besides its own')

  const first = ctx.getCore(1)
  t.is(first.key, null, 'the session it opened does not know its key yet')
  t.is(ctx.getCore(1), first, 'a second read in the same tick reuses it')

  await first.close()
  await view.close()
  await bee.close()
  await ns.close()
})

function lateKeys(store) {
  return new Proxy(store, {
    get(target, prop) {
      if (prop === 'get') return (opts) => target.get({ ...opts, preload: Promise.resolve({}) })
      if (prop === 'session' || prop === 'namespace') {
        return (...args) => lateKeys(target[prop](...args))
      }

      const value = Reflect.get(target, prop)
      return typeof value === 'function' ? value.bind(target) : value
    }
  })
}
