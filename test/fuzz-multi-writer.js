const test = require('brittle')
const b4a = require('b4a')
const Corestore = require('corestore')
const Bee = require('../')

// Multi-writer fuzz. Several writers live in one corestore, so every core is
// reachable by key without replication. Each step a random writer bases a
// batch on a head - the current tip, or (occasionally) an older recorded head
// to fork it - via write({ key, length }); the batch always appends to that
// writer's own core, so writers stay independent (no move() on a writer, which
// would repoint its context). Values above the inline limit force value-only
// blocks, so a pointer that lands on the wrong seq shows up as a block without
// a tree node at its offset.
//
// After every flush a reader walks every reachable pointer straight from disk,
// checking each points at a real tree node, and the full key range is compared
// against a model.
//
// FUZZ_SEED and FUZZ_STEPS override the defaults.

const env = (typeof process !== 'undefined' && process.env) || {}

const DEFAULT_SEEDS = [1, 2, 3, 44, 103]
const SEEDS = env.FUZZ_SEED ? [Number(env.FUZZ_SEED)] : DEFAULT_SEEDS
const STEPS = Number(env.FUZZ_STEPS) || 120
const WRITERS = 3
const KEYSPACE = 400

// the 20 key hyperbee1 tree from test/compat.js ('#0'..'#19')
const COMPAT_BATCH = [
  '0a086879706572626565',
  '0a050a030a0101120223301a022330',
  '0a060a040a020102120223311a022331',
  '0a070a050a03010203120223321a022332',
  '0a080a060a0401020304120223331a022333',
  '0a090a070a050102030405120223341a022334',
  '0a0a0a080a06010203040506120223351a022335',
  '0a0b0a090a0701020304050607120223361a022336',
  '0a0c0a0a0a080102030405060708120223371a022337',
  '0a1b0a090a01051204090109020a060a04010203040a060a0406070809120223381a022338',
  '0a140a090a0105120409010a010a070a05060708090a120223391a022339',
  '0a140a090a010512040b010a010a070a0501020b030412032331301a03233130',
  '0a150a090a010512040c010a010a080a0601020b0c030412032331311a03233131',
  '0a160a090a010512040d010a010a090a0701020b0c0d030412032331321a03233132',
  '0a170a090a010512040e010a010a0a0a0801020b0c0d0e030412032331331a03233133',
  '0a1e0a0c0a020d0512060f010f020a010a060a0401020b0c0a060a040e0f030412032331341a03233134',
  '0a170a0c0a020d0512060f0110010a010a070a050e0f10030412032331351a03233135',
  '0a180a0c0a020d0512060f0111010a010a080a060e0f1011030412032331361a03233136',
  '0a190a0c0a020d0512060f0112010a010a090a070e0f101112030412032331371a03233137',
  '0a1a0a0c0a020d0512060f0113010a010a0a0a080e0f10111213030412032331381a03233138',
  '0a210a0f0a030d120512080f01140114020a010a060a040e0f10110a060a041314030412032331391a03233139'
].map((hex) => b4a.from(hex, 'hex'))

for (const seed of SEEDS) {
  test('multi writer fuzz - seed ' + seed, async function (t) {
    await run(t, seed, { compat: false })
  })

  test('multi writer fuzz on a compat root - seed ' + seed, async function (t) {
    await run(t, seed, { compat: true })
  })
}

async function run(t, seed, { compat }) {
  const rng = mulberry32(seed)
  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close())

  const writers = []
  for (let i = 0; i < WRITERS; i++) {
    const w = new Bee(store.namespace('writer-' + i), { t: 5 })
    t.teardown(() => w.close())
    await w.ready()
    writers.push(w)
  }

  // a long lived read-only reader (kept contexts, dropped cached nodes) plus a
  // fresh reader every few steps to also catch anything the cores list masks
  const reader = new Bee(store.namespace('reader'), { writable: false })
  t.teardown(() => reader.close())
  await reader.ready()

  const model = new Map()
  const history = []

  let tip = null

  if (compat) {
    // seed writer 0's core with a hyperbee1 tree so the current tip roots on a
    // compat block; later writers fork it via write({ key, length })
    const w = writers[0]
    await w.core.append(COMPAT_BATCH)
    w.update()
    for (let i = 0; i < 20; i++) model.set('#' + i, b4a.from('#' + i))
    tip = w.head()
    history.push({ head: tip, model: new Map(model) })
  }

  for (let step = 0; step < STEPS; step++) {
    const w = writers[Math.floor(rng() * writers.length)]
    const where = { seed, step, compat, writer: writers.indexOf(w) }

    // pick the head this batch is based on: usually the current tip, sometimes
    // an older head to fork it
    let base = tip
    if (tip !== null && history.length > 1 && rng() < 0.1) {
      const entry = history[Math.floor(rng() * (history.length - 1))]
      base = entry.head
      model.clear()
      for (const [k, v] of entry.model) model.set(k, v)
      where.action = 'fork'
    } else {
      where.action = 'append'
    }

    // base on the chosen head (foreign, own, or older) - the batch still
    // appends to w's own core
    const batch = base === null ? w.write() : w.write({ key: base.key, length: base.length })

    const ops = 1 + Math.floor(rng() * 12)
    const applied = []

    for (let i = 0; i < ops; i++) {
      const key = pickKey(rng)
      if (rng() < 0.7) {
        const value = pickValue(rng)
        batch.tryPut(b4a.from(key), value)
        model.set(key, value)
        applied.push('put ' + key + ' (' + value.byteLength + 'b)')
      } else {
        batch.tryDelete(b4a.from(key))
        model.delete(key)
        applied.push('del ' + key)
      }
    }

    where.ops = applied

    try {
      await batch.flush()
    } catch (err) {
      return fail(t, where, 'flush threw: ' + err.stack)
    }

    tip = w.head()
    history.push({ head: tip, model: new Map(model) })

    // verify from disk with the long lived reader
    reader.cache.empty()
    reader.move(tip)
    const failure = await verify(reader, model)
    if (failure) return fail(t, where, failure)

    // and every so often with a brand new reader
    if (step % 10 === 9) {
      const fresh = new Bee(store.namespace('fresh-' + step), { writable: false })
      await fresh.ready()
      fresh.move(tip)
      const freshFailure = await verify(fresh, model)
      await fresh.close()
      if (freshFailure) return fail(t, where, 'fresh reader: ' + freshFailure)
    }
  }

  t.pass('seed ' + seed + ' ok after ' + STEPS + ' steps')
}

// walks every reachable tree pointer from disk, checking the block it points
// at actually holds a tree node at that offset before inflating it, then
// compares the full range against the model
async function verify(db, model) {
  const root = await db.bootstrap(db.config)
  const seen = new Set()

  if (root !== null) {
    const bad = await walk(db, root, seen)
    if (bad) return bad
  } else if (model.size > 0) {
    return 'empty tree but model has ' + model.size + ' keys'
  }

  const expected = [...model.keys()].sort((a, b) => b4a.compare(b4a.from(a), b4a.from(b)))
  const actual = []

  for await (const data of db.createReadStream()) {
    const key = b4a.toString(data.key)
    const want = model.get(key)
    if (want === undefined) return 'unexpected key in tree: ' + key
    if (!b4a.equals(want, data.value)) return 'wrong value for key ' + key
    actual.push(key)
  }

  if (actual.length !== expected.length) {
    const missing = expected.filter((k) => !actual.includes(k))
    const extra = actual.filter((k) => !expected.includes(k))
    return (
      'tree has ' +
      actual.length +
      ' keys, model has ' +
      expected.length +
      ' (missing: ' +
      missing.join(' ') +
      ', extra: ' +
      extra.join(' ') +
      ')'
    )
  }

  for (let i = 0; i < actual.length; i++) {
    if (actual[i] !== expected[i]) {
      return 'key order mismatch at ' + i + ': ' + actual[i] + ' vs ' + expected[i]
    }
  }

  return null
}

async function walk(db, ptr, seen) {
  const id = describe(ptr)
  if (seen.has(id)) return 'pointer reached twice: ' + id
  seen.add(id)

  let block
  try {
    block = await ptr.context.getBlock(ptr.seq, ptr.core, db.config)
  } catch (err) {
    return 'could not load block for ' + id + ': ' + err.message
  }

  if (!block.tree) {
    return 'pointer into a block without tree nodes: ' + id + ' (type ' + block.type + ')'
  }
  if (ptr.offset >= block.tree.length) {
    return 'pointer offset out of range: ' + id + ' (block has ' + block.tree.length + ' nodes)'
  }

  let v
  try {
    v = ptr.value || (await db.inflate(ptr, db.config))
  } catch (err) {
    return 'inflate failed for ' + id + ': ' + err.stack
  }

  for (let i = 0; i < v.children.length; i++) {
    const bad = await walk(db, v.children.get(i), seen)
    if (bad) return bad
  }

  return null
}

function describe(ptr) {
  let key = '?'
  try {
    key = b4a.toString(ptr.context.getCoreKey(ptr.core), 'hex').slice(0, 8)
  } catch {}
  return key + '@' + ptr.seq + '.' + ptr.offset
}

function fail(t, where, message) {
  t.fail(message)
  t.comment(JSON.stringify(where, null, 2))
}

function pickKey(rng) {
  return 'k' + String(Math.floor(rng() * KEYSPACE)).padStart(4, '0')
}

function pickValue(rng) {
  // ~30% of values exceed the inline limit so batches carry value-only blocks
  const big = rng() < 0.3
  const size = big ? 1200 + Math.floor(rng() * 3000) : 1 + Math.floor(rng() * 32)
  const buf = b4a.allocUnsafe(size)
  const fill = Math.floor(rng() * 256)
  for (let i = 0; i < size; i++) buf[i] = (fill + i) & 0xff
  return buf
}

function mulberry32(seed) {
  let a = seed >>> 0
  return function () {
    a = (a + 0x6d2b79f5) >>> 0
    let t = a
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}
