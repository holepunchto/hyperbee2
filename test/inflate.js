const test = require('brittle')
const b4a = require('b4a')
const { create, replicate } = require('./helpers')

test('destroying one stream does not cancel an inflation shared with another', async function (t) {
  const { db, reader, entries } = await createRemote(t)

  const view = reader.checkout({ length: db.head().length })

  const s1 = view.createReadStream()
  const s2 = view.createReadStream()

  const r1 = collect(s1)
  const r2 = collect(s2)

  // let both streams reach the root inflation
  await new Promise((resolve) => setTimeout(resolve, 50))

  s1.destroy()
  await t.exception(r1, /REQUEST_CANCELLED|destroyed/i)

  await replicate(t, db, reader)

  const got = await r2
  t.is(got.length, entries.length)
  for (let i = 0; i < entries.length; i++) t.alike(got[i].key, entries[i].key)

  await view.close()
})

test("shared inflation honours each caller's own timeout", async function (t) {
  const { db, reader, entries } = await createRemote(t)

  const view = reader.checkout({ length: db.head().length })

  const fast = view.get(entries[0].key, { timeout: 50 })
  const slow = view.get(entries[0].key, { timeout: 10000 })

  await t.exception(fast, /REQUEST_TIMEOUT/)

  await replicate(t, db, reader)

  const node = await slow
  t.alike(node.value, entries[0].value)

  await view.close()
})

test('wait: false fails fast while a shared fetch is in flight', async function (t) {
  const { db, reader, entries } = await createRemote(t)

  const view = reader.checkout({ length: db.head().length })

  const pending = view.get(entries[0].key)
  await new Promise((resolve) => setTimeout(resolve, 50))

  await t.exception(view.get(entries[0].key, { wait: false }), /BLOCK_NOT_AVAILABLE/)

  await replicate(t, db, reader)

  const node = await pending
  t.alike(node.value, entries[0].value)

  await view.close()
})

test('each caller sharing an inflation traces its own reads', async function (t) {
  const { db, reader, entries } = await createRemote(t)

  const view = reader.checkout({ length: db.head().length })

  const a = new Set()
  const b = new Set()

  const ga = view.get(entries[0].key, { trace: (core, seq) => a.add(seq) })
  const gb = view.get(entries[0].key, { trace: (core, seq) => b.add(seq) })

  await replicate(t, db, reader)
  await Promise.all([ga, gb])

  t.ok(a.size > 0)
  t.alike(a, b)

  await view.close()
})

test('a late caller reuses the first round and shares the second', async function (t) {
  const { db, reader, entries } = await createRemote(t)

  const view = reader.checkout({ length: db.head().length })
  const root = await view.bootstrap(view.config)

  const first = view.inflate(root, view.config)
  await new Promise((resolve) => setTimeout(resolve, 50))
  t.ok(root.inflating !== null, 'round 0 in flight')

  await replicate(t, db, reader)
  await first

  t.is(root.inflating, null, 'state dropped once inflated')
  t.ok(root.value)

  const again = await view.inflate(root, view.config)
  t.is(again, root.value)

  await view.close()
})

async function createRemote(t) {
  const db = await create(t, { t: 4 })
  const entries = []

  for (let i = 0; i < 100; i++) entries.push({ key: entriesKey(i), value: b4a.from('v' + i) })

  const w = db.write()
  for (const e of entries) w.tryPut(e.key, e.value)
  await w.flush()

  const reader = await create(t, { key: db.core.key, t: 4 })
  await reader.ready()

  return { db, reader, entries }
}

function entriesKey(i) {
  return b4a.from('k' + i.toString().padStart(4, '0'))
}

async function collect(stream) {
  const result = []
  for await (const data of stream) result.push(data)
  return result
}
