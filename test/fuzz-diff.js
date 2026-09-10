const test = require('brittle')
const b4a = require('b4a')
const { create, replicate } = require('./helpers')
const { DiffIterator } = require('../lib/diff.js')

const RUNS = Number(process.env.RUNS || 20)
const SEED = Number(process.env.SEED || Date.now() % 1000000)

let skips = 0
const skipSharedChildren = DiffIterator.prototype._skipSharedChildren
DiffIterator.prototype._skipSharedChildren = async function () {
  if (process.env.NOSKIP) return
  const a = this.left.stack[this.left.stack.length - 1]
  const before = a ? a.offset : -1
  await skipSharedChildren.call(this)
  if (a && a.offset !== before) skips++
}

test('fuzz diff - single writer', async function (t) {
  for (let run = 0; run < RUNS; run++) {
    const seed = SEED + run
    const rng = random(seed)
    const opts = { t: pick(rng, [3, 4, 5, 8]), maxCacheSize: pick(rng, [4, 32, 4096]) }
    const db = await create(t, opts)

    const lengths = []
    for (let i = 0; i < 4 + rng(8); i++) {
      await writeBatch(db, rng)
      lengths.push(db.core.length)
    }

    // adjacent versions share most of their subtrees
    const i = rng(lengths.length)
    const j = rng(2) ? Math.min(i + 1, lengths.length - 1) : rng(lengths.length)
    const a = db.checkout({ length: lengths[i] })
    const b = db.checkout({ length: lengths[j] })

    await checkDiffs(t, rng, a, b, `seed ${seed} t=${opts.t} cache=${opts.maxCacheSize}`)

    await a.close()
    await b.close()
  }

  t.ok(skips > 0, `shared children were skipped ${skips} times`)
})

test('fuzz diff - cross writer', async function (t) {
  for (let run = 0; run < RUNS; run++) {
    const seed = SEED + 1000000 + run
    const rng = random(seed)
    const opts = { t: pick(rng, [3, 4, 5]), maxCacheSize: pick(rng, [4, 4096]) }

    const db1 = await create(t, opts)
    for (let i = 0; i < 2 + rng(4); i++) await writeBatch(db1, rng)

    const db2 = await create(t, opts)
    replicate(t, db1, db2)

    const head = db1.head()
    for (let i = 0; i < 1 + rng(3); i++) await writeBatch(db2, rng, i === 0 ? head : undefined)
    for (let i = 0; i < rng(3); i++) await writeBatch(db1, rng)

    await checkDiffs(t, rng, db1, db2, `seed ${seed} t=${opts.t} cache=${opts.maxCacheSize}`)
  }
})

async function checkDiffs(t, rng, a, b, ctx) {
  const [ka, kb] = await Promise.all([readAll(a), readAll(b)])

  for (let i = 0; i < 4; i++) {
    const options = randomOptions(rng)
    const desc =
      ctx + ' ' + JSON.stringify(options, (k, v) => (b4a.isBuffer(v) ? b4a.toString(v) : v))

    check(
      t,
      strip(await collect(a.createDiffStream(b, options))),
      reference(ka, kb, options),
      'a->b ' + desc
    )
    check(
      t,
      strip(await collect(b.createDiffStream(a, options))),
      reference(kb, ka, options),
      'b->a ' + desc
    )
  }
}

function check(t, actual, expected, desc) {
  const same = JSON.stringify(actual) === JSON.stringify(expected)
  if (!same && process.env.VERBOSE) {
    t.comment(
      'actual   ' + actual.map((e) => e.key + (e.left ? 'L' : '') + (e.right ? 'R' : '')).join(' ')
    )
    t.comment(
      'expected ' +
        expected.map((e) => e.key + (e.left ? 'L' : '') + (e.right ? 'R' : '')).join(' ')
    )
  }
  t.ok(same, desc)
}

async function writeBatch(db, rng, head) {
  const w = db.write(head)
  const n = 1 + rng(40)
  for (let i = 0; i < n; i++) {
    const k = key(rng(300))
    if (rng(5) === 0) w.tryDelete(k)
    else w.tryPut(k, b4a.from('v' + rng(1000000)))
  }
  await w.flush()
}

async function readAll(db) {
  const map = new Map()
  for await (const e of db.createReadStream()) map.set(b4a.toString(e.key), e)
  return map
}

function reference(left, right, options) {
  const keys = new Set([...left.keys(), ...right.keys()])
  const out = []

  for (const k of keys) {
    const l = left.get(k) || null
    const r = right.get(k) || null
    if (l && r && samePointer(l, r)) continue
    if (!inRange(b4a.from(k), options)) continue
    out.push({ key: k, left: l && b4a.toString(l.value), right: r && b4a.toString(r.value) })
  }

  out.sort((x, y) => (x.key < y.key ? -1 : x.key > y.key ? 1 : 0))
  if (options.reverse) out.reverse()
  if (options.limit !== undefined && options.limit !== -1) {
    out.length = Math.min(out.length, options.limit)
  }

  return out
}

function strip(entries) {
  return entries.map((e) => ({
    key: b4a.toString((e.left || e.right).key),
    left: e.left && b4a.toString(e.left.value),
    right: e.right && b4a.toString(e.right.value)
  }))
}

function samePointer(a, b) {
  return a.seq === b.seq && a.offset === b.offset && b4a.equals(a.core.key, b.core.key)
}

function inRange(k, { gt, gte, lt, lte }) {
  if (gt && b4a.compare(k, gt) <= 0) return false
  if (gte && b4a.compare(k, gte) < 0) return false
  if (lt && b4a.compare(k, lt) >= 0) return false
  if (lte && b4a.compare(k, lte) > 0) return false
  return true
}

function randomOptions(rng) {
  const options = {}
  if (rng(2)) options.reverse = true
  const lo = rng(3)
  const hi = rng(3)
  if (lo === 1) options.gt = key(rng(300))
  if (lo === 2) options.gte = key(rng(300))
  if (hi === 1) options.lt = key(rng(300))
  if (hi === 2) options.lte = key(rng(300))
  if (rng(3) === 0) options.limit = rng(6)
  return options
}

async function collect(stream) {
  const entries = []
  for await (const e of stream) entries.push(e)
  return entries
}

function key(i) {
  return b4a.from(String(i).padStart(4, '0'))
}

function pick(rng, list) {
  return list[rng(list.length)]
}

function random(seed) {
  let s = seed >>> 0 || 1
  return function (n) {
    s ^= s << 13
    s >>>= 0
    s ^= s >>> 17
    s ^= s << 5
    s >>>= 0
    return s % n
  }
}
