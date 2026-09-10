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

test('cannot write on a head in the middle of a batch', async function (t) {
  const db = await create(t, { t: 5 })

  // big values force a multi-block batch: value-only blocks then a tail block
  {
    const w = db.write()
    for (let i = 0; i < 3; i++) w.tryPut(b4a.from('k' + i), b4a.alloc(3000, i))
    await w.flush()
  }

  const head = db.head()

  // the tail block carries the tree; earlier seqs are value-only blocks
  const tail = await db.context.getBlock(head.length - 1, 0, db.config)
  t.is(tail.batch.end, 0, 'head points at the batch tail')
  t.ok(tail.tree, 'tail block has a tree')

  const mid = await db.context.getBlock(head.length - 2, 0, db.config)
  t.not(mid.batch.end, 0, 'the previous block is mid-batch')
  t.absent(mid.tree, 'mid-batch block has no tree')

  // writing on the batch tail is fine
  {
    const w = db.write({ key: head.key, length: head.length })
    w.tryPut(b4a.from('z'), b4a.from('z'))
    await w.flush()
  }

  // writing on a mid-batch length throws instead of corrupting the tree
  await t.exception(async function () {
    const w = db.write({ key: head.key, length: head.length - 1 })
    w.tryPut(b4a.from('y'), b4a.from('y'))
    await w.flush()
  }, /middle of a batch/)
})
