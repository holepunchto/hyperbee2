const test = require('brittle')
const b4a = require('b4a')
const { create } = require('./helpers')
const { CompressedArray, DeltaOp, OP_INSERT } = require('../lib/compression.js')

test('move back and flush, disk state stays intact', async function (t) {
  const db = await create(t)

  const pad = (n) => 'key/' + String(n).padStart(5, '0')
  const model = new Map()

  {
    const w = db.write()
    for (let i = 0; i < 400; i++) {
      w.tryPut(b4a.from(pad(i)), b4a.from('v0-' + i))
      model.set(pad(i), 'v0-' + i)
    }
    await w.flush()
  }

  const touched = (f) => {
    const idxs = []
    for (let i = 0; i < 5; i++) idxs.push((f * 37 + i * 11) % 400)
    return idxs
  }

  for (let f = 1; f <= 10; f++) {
    const w = db.write()
    for (const idx of touched(f)) {
      w.tryPut(b4a.from(pad(idx)), b4a.from(`v${f}-${idx}`))
      model.set(pad(idx), `v${f}-${idx}`)
    }
    await w.flush()
  }

  db.move({ length: db.head().length - 1 })

  for (const idx of touched(10)) {
    let val = 'v0-' + idx
    for (let f = 1; f <= 9; f++) {
      if (touched(f).includes(idx)) val = `v${f}-${idx}`
    }
    model.set(pad(idx), val)
  }

  {
    const w = db.write()
    for (let i = 0; i < 5; i++) {
      const idx = (900 + i * 13) % 400
      w.tryPut(b4a.from(pad(idx)), b4a.from('vpost-' + idx))
      model.set(pad(idx), 'vpost-' + idx)
    }
    await w.flush()
  }

  db.cache.empty()

  let bad = 0
  for (const [key, value] of model) {
    const node = await db.get(b4a.from(key))
    const got = node ? b4a.toString(node.value) : null
    if (got !== value) bad++
  }

  t.is(bad, 0)
})

test('compressed array commit without updates keeps the delta', function (t) {
  const arr = new CompressedArray([
    new DeltaOp(false, OP_INSERT, 0, {}),
    new DeltaOp(false, OP_INSERT, 1, {}),
    new DeltaOp(false, OP_INSERT, 2, {})
  ])

  const c = arr.commit()

  t.is(c.delta.length, 3)
  t.is(arr.delta.length, 3)
  t.ok(c.delta !== arr.delta)
  t.is(arr.entries.length, 3)
})
