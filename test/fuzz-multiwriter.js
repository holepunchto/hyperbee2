const test = require('brittle')
const b4a = require('b4a')
const { createMultiple } = require('./helpers')

const RUNS = Number(process.env.RUNS || 20)
const SEED = Number(process.env.SEED || Date.now() % 1000000)

test('fuzz multi writer - cross linked writes and moves', async function (t) {
  for (let run = 0; run < RUNS; run++) {
    const seed = SEED + run
    const rng = random(seed)
    const opts = { t: pick(rng, [3, 4, 5]), maxCacheSize: pick(rng, [4, 4096]) }
    const dbs = await createMultiple(t, 2 + rng(3), opts)

    for (const db of dbs) await db.ready()

    // every head we have ever produced, with the exact content it should hold
    const heads = dbs.map((db) => ({ key: db.core.key, length: 0, model: new Map() }))
    const current = new Map(dbs.map((db, i) => [db, heads[i]]))

    const steps = 30 + rng(30)

    for (let step = 0; step < steps; step++) {
      const db = pick(rng, dbs)
      const desc = `seed ${seed} step ${step} writer ${dbs.indexOf(db)} t=${opts.t} cache=${opts.maxCacheSize}`

      if (rng(4) === 0) {
        const head = pick(rng, heads)
        db.move({ key: head.key, length: head.length })
        current.set(db, head)
        if (!(await check(t, db, head, 'move ' + desc))) return
        continue
      }

      const base = rng(2) ? current.get(db) : pick(rng, heads)
      const w =
        base === current.get(db) && rng(2)
          ? db.write()
          : db.write({ key: base.key, length: base.length })
      const model = new Map(base.model)

      const n = 1 + rng(20)
      for (let i = 0; i < n; i++) {
        const k = String(rng(120)).padStart(3, '0')
        if (rng(5) === 0) {
          w.tryDelete(b4a.from(k))
          model.delete(k)
        } else {
          const v = 'v' + rng(1000000)
          w.tryPut(b4a.from(k), b4a.from(v))
          model.set(k, v)
        }
      }

      await w.flush()

      const head = { key: db.head().key, length: db.head().length, model }
      heads.push(head)
      current.set(db, head)

      if (!(await check(t, db, head, 'write ' + desc))) return
    }
  }

  t.pass('finished')
})

async function check(t, db, head, desc) {
  const h = db.head()
  if (!b4a.equals(h.key, head.key) || h.length !== head.length) {
    t.fail('head mismatch ' + desc)
    return false
  }

  if (Math.random() < 0.3) db.cache.empty()

  const actual = []
  for await (const e of db.createReadStream()) {
    actual.push(b4a.toString(e.key) + '=' + b4a.toString(e.value))
  }

  const expected = [...head.model]
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([k, v]) => k + '=' + v)

  if (actual.join(' ') !== expected.join(' ')) {
    t.fail('content mismatch ' + desc)
    t.comment('actual   ' + actual.join(' '))
    t.comment('expected ' + expected.join(' '))
    return false
  }

  return true
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
