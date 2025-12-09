const test = require('brittle')
const b4a = require('b4a')
const { create, replicate } = require('./helpers')

test('basic diff - add one key', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    w.tryPut(b4a.from('c'), b4a.from('3'))
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryPut(b4a.from('d'), b4a.from('4'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  t.is(entries.length, 1)
  t.alike(entries[0].left.key, b4a.from('d'))
  t.alike(entries[0].left.value, b4a.from('4'))
  t.is(entries[0].right, null)
})

test('basic diff - modify one key', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    w.tryPut(b4a.from('c'), b4a.from('3'))
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryPut(b4a.from('b'), b4a.from('modified'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  t.is(entries.length, 1)
  t.alike(entries[0].left.key, b4a.from('b'))
  t.alike(entries[0].left.value, b4a.from('modified'))
  t.alike(entries[0].right.key, b4a.from('b'))
  t.alike(entries[0].right.value, b4a.from('2'))
})

test('basic diff - delete one key', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    w.tryPut(b4a.from('c'), b4a.from('3'))
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryDelete(b4a.from('b'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  t.is(entries.length, 1)
  t.is(entries[0].left, null)
  t.alike(entries[0].right.key, b4a.from('b'))
  t.alike(entries[0].right.value, b4a.from('2'))
})

test('diff with older snap as base', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryPut(b4a.from('c'), b4a.from('3'))
    await w.flush()
  }

  // Diff from snap perspective - c was added in db
  const entries = await collect(snap.createDiffStream(db))

  t.is(entries.length, 1)
  t.is(entries[0].left, null) // not in snap
  t.alike(entries[0].right.key, b4a.from('c')) // present in db
})

test('diff identical trees - no output', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    w.tryPut(b4a.from('c'), b4a.from('3'))
    await w.flush()
  }

  const snap = db.snapshot()
  const entries = await collect(db.createDiffStream(snap))

  t.is(entries.length, 0)
})

test('diff empty trees', async function (t) {
  const db = await create(t)
  const snap = db.snapshot()

  const entries = await collect(db.createDiffStream(snap))
  t.is(entries.length, 0)
})

test('diff empty to populated', async function (t) {
  const db = await create(t)
  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  t.is(entries.length, 2)
  t.alike(entries[0].left.key, b4a.from('a'))
  t.is(entries[0].right, null)
  t.alike(entries[1].left.key, b4a.from('b'))
  t.is(entries[1].right, null)
})

test('diff populated to empty', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryDelete(b4a.from('a'))
    w.tryDelete(b4a.from('b'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  t.is(entries.length, 2)
  t.is(entries[0].left, null)
  t.alike(entries[0].right.key, b4a.from('a'))
  t.is(entries[1].left, null)
  t.alike(entries[1].right.key, b4a.from('b'))
})

test('diff with multiple changes', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    w.tryPut(b4a.from('c'), b4a.from('3'))
    w.tryPut(b4a.from('d'), b4a.from('4'))
    w.tryPut(b4a.from('e'), b4a.from('5'))
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryDelete(b4a.from('a')) // delete
    w.tryPut(b4a.from('b'), b4a.from('modified')) // modify
    // c unchanged
    w.tryDelete(b4a.from('d')) // delete
    w.tryPut(b4a.from('f'), b4a.from('6')) // add
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  t.is(entries.length, 4)

  // a deleted
  t.is(entries[0].left, null)
  t.alike(entries[0].right.key, b4a.from('a'))

  // b modified
  t.alike(entries[1].left.key, b4a.from('b'))
  t.alike(entries[1].left.value, b4a.from('modified'))
  t.alike(entries[1].right.key, b4a.from('b'))
  t.alike(entries[1].right.value, b4a.from('2'))

  // d deleted
  t.is(entries[2].left, null)
  t.alike(entries[2].right.key, b4a.from('d'))

  // f added
  t.alike(entries[3].left.key, b4a.from('f'))
  t.is(entries[3].right, null)
})

test('diff with limit', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryPut(b4a.from('c'), b4a.from('3'))
    w.tryPut(b4a.from('d'), b4a.from('4'))
    w.tryPut(b4a.from('e'), b4a.from('5'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap, { limit: 2 }))

  t.is(entries.length, 2)
  t.alike(entries[0].left.key, b4a.from('c'))
  t.alike(entries[1].left.key, b4a.from('d'))
})

test('diff with range gt', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    w.tryPut(b4a.from('c'), b4a.from('3'))
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1*'))
    w.tryPut(b4a.from('b'), b4a.from('2*'))
    w.tryPut(b4a.from('c'), b4a.from('3*'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap, { gt: b4a.from('a') }))

  t.is(entries.length, 2)
  t.alike(entries[0].left.key, b4a.from('b'))
  t.alike(entries[1].left.key, b4a.from('c'))
})

test('diff with range gte', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    w.tryPut(b4a.from('c'), b4a.from('3'))
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1*'))
    w.tryPut(b4a.from('b'), b4a.from('2*'))
    w.tryPut(b4a.from('c'), b4a.from('3*'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap, { gte: b4a.from('b') }))

  t.is(entries.length, 2)
  t.alike(entries[0].left.key, b4a.from('b'))
  t.alike(entries[1].left.key, b4a.from('c'))
})

test('diff with range lt', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    w.tryPut(b4a.from('c'), b4a.from('3'))
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1*'))
    w.tryPut(b4a.from('b'), b4a.from('2*'))
    w.tryPut(b4a.from('c'), b4a.from('3*'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap, { lt: b4a.from('c') }))

  t.is(entries.length, 2)
  t.alike(entries[0].left.key, b4a.from('a'))
  t.alike(entries[1].left.key, b4a.from('b'))
})

test('diff with range lte', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    w.tryPut(b4a.from('c'), b4a.from('3'))
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1*'))
    w.tryPut(b4a.from('b'), b4a.from('2*'))
    w.tryPut(b4a.from('c'), b4a.from('3*'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap, { lte: b4a.from('b') }))

  t.is(entries.length, 2)
  t.alike(entries[0].left.key, b4a.from('a'))
  t.alike(entries[1].left.key, b4a.from('b'))
})

test('diff with range gt and lt', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    w.tryPut(b4a.from('c'), b4a.from('3'))
    w.tryPut(b4a.from('d'), b4a.from('4'))
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1*'))
    w.tryPut(b4a.from('b'), b4a.from('2*'))
    w.tryPut(b4a.from('c'), b4a.from('3*'))
    w.tryPut(b4a.from('d'), b4a.from('4*'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap, { gt: b4a.from('a'), lt: b4a.from('d') }))

  t.is(entries.length, 2)
  t.alike(entries[0].left.key, b4a.from('b'))
  t.alike(entries[1].left.key, b4a.from('c'))
})

test('diff large tree - performance (subtree skip)', async function (t) {
  const db = await create(t)

  // Insert many keys to create a multi-level tree
  {
    const w = db.write()
    for (let i = 0; i < 100; i++) {
      w.tryPut(b4a.from(String(i).padStart(3, '0')), b4a.from(String(i)))
    }
    await w.flush()
  }

  const snap = db.snapshot()

  // Only change one key
  {
    const w = db.write()
    w.tryPut(b4a.from('050'), b4a.from('modified'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  // Should only report the one changed key
  t.is(entries.length, 1)
  t.alike(entries[0].left.key, b4a.from('050'))
  t.alike(entries[0].left.value, b4a.from('modified'))
  t.alike(entries[0].right.key, b4a.from('050'))
  t.alike(entries[0].right.value, b4a.from('50'))
})

test('diff large tree with multiple sparse changes', async function (t) {
  const db = await create(t)

  // Insert many keys
  {
    const w = db.write()
    for (let i = 0; i < 200; i++) {
      w.tryPut(b4a.from(String(i).padStart(3, '0')), b4a.from(String(i)))
    }
    await w.flush()
  }

  const snap = db.snapshot()

  // Change a few scattered keys
  {
    const w = db.write()
    w.tryPut(b4a.from('010'), b4a.from('mod1'))
    w.tryPut(b4a.from('090'), b4a.from('mod2'))
    w.tryPut(b4a.from('150'), b4a.from('mod3'))
    w.tryDelete(b4a.from('175'))
    w.tryPut(b4a.from('999'), b4a.from('new'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  t.is(entries.length, 5)

  // Verify changes are in key order
  t.alike(entries[0].left.key, b4a.from('010'))
  t.alike(entries[1].left.key, b4a.from('090'))
  t.alike(entries[2].left.key, b4a.from('150'))
  t.is(entries[3].left, null) // deleted 175
  t.alike(entries[3].right.key, b4a.from('175'))
  t.alike(entries[4].left.key, b4a.from('999'))
})

test('diff between two checkouts', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    await w.flush()
  }

  const v1 = db.checkout({ length: db.core.length })

  {
    const w = db.write()
    w.tryPut(b4a.from('b'), b4a.from('2'))
    await w.flush()
  }

  const v2 = db.checkout({ length: db.core.length })

  {
    const w = db.write()
    w.tryPut(b4a.from('c'), b4a.from('3'))
    await w.flush()
  }

  // Diff between v1 and v2 should show 'b' added
  const entries = await collect(v2.createDiffStream(v1))

  t.is(entries.length, 1)
  t.alike(entries[0].left.key, b4a.from('b'))
  t.is(entries[0].right, null)
})

test('diff respects order - entries come in key order', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('z'), b4a.from('1'))
    w.tryPut(b4a.from('a'), b4a.from('2'))
    w.tryPut(b4a.from('m'), b4a.from('3'))
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryPut(b4a.from('z'), b4a.from('1*'))
    w.tryPut(b4a.from('a'), b4a.from('2*'))
    w.tryPut(b4a.from('m'), b4a.from('3*'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  t.is(entries.length, 3)
  // Should be in alphabetical order
  t.alike(entries[0].left.key, b4a.from('a'))
  t.alike(entries[1].left.key, b4a.from('m'))
  t.alike(entries[2].left.key, b4a.from('z'))
})

test('diff with batched writes', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    w.tryPut(b4a.from('c'), b4a.from('3'))
    await w.flush()
  }

  const snap = db.snapshot()

  // Multiple operations in single batch
  {
    const w = db.write()
    w.tryPut(b4a.from('d'), b4a.from('4'))
    w.tryPut(b4a.from('e'), b4a.from('5'))
    w.tryDelete(b4a.from('b'))
    w.tryPut(b4a.from('a'), b4a.from('1*'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  t.is(entries.length, 4)
  t.alike(entries[0].left.key, b4a.from('a'))
  t.alike(entries[0].left.value, b4a.from('1*'))
  t.is(entries[1].left, null) // b deleted
  t.alike(entries[2].left.key, b4a.from('d'))
  t.alike(entries[3].left.key, b4a.from('e'))
})

test('diff oob seek - range outside data', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    w.tryPut(b4a.from('c'), b4a.from('3'))
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1*'))
    await w.flush()
  }

  // Range that doesn't include the changed key
  const entries = await collect(db.createDiffStream(snap, { gt: b4a.from('x'), lt: b4a.from('z') }))

  t.is(entries.length, 0)
})

test('diff single key tree', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('only'), b4a.from('1'))
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryPut(b4a.from('only'), b4a.from('2'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  t.is(entries.length, 1)
  t.alike(entries[0].left.key, b4a.from('only'))
  t.alike(entries[0].left.value, b4a.from('2'))
  t.alike(entries[0].right.key, b4a.from('only'))
  t.alike(entries[0].right.value, b4a.from('1'))
})

test('diff cross-writer scenario', async function (t) {
  const db1 = await create(t)

  {
    const w = db1.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    await w.flush()
  }

  const db2 = await create(t)
  replicate(t, db1, db2)

  // db2 writes on top of db1's state
  {
    const w = db2.write(db1.head())
    w.tryPut(b4a.from('c'), b4a.from('3'))
    await w.flush()
  }

  // Now diff db2 against a snapshot of db1
  const snap1 = db1.snapshot()
  const entries = await collect(db2.createDiffStream(snap1))

  t.is(entries.length, 1)
  t.alike(entries[0].left.key, b4a.from('c'))
  t.is(entries[0].right, null)
})

test('diff cross-writer - divergent writes from common ancestor', async function (t) {
  // Setup: db1 creates initial data, db2 and db3 both fork from db1 with different changes
  const db1 = await create(t)

  // Initial shared state
  {
    const w = db1.write()
    for (let i = 0; i < 20; i++) {
      w.tryPut(b4a.from('key' + String(i).padStart(2, '0')), b4a.from('val' + i))
    }
    await w.flush()
  }

  const db1Head = db1.head()

  const db2 = await create(t)
  const db3 = await create(t)
  replicate(t, db1, db2)
  replicate(t, db1, db3)

  // db2 modifies some keys and adds new ones
  {
    const w = db2.write(db1Head)
    w.tryPut(b4a.from('key05'), b4a.from('modified-by-db2'))
    w.tryPut(b4a.from('key10'), b4a.from('modified-by-db2'))
    w.tryPut(b4a.from('key15'), b4a.from('modified-by-db2'))
    w.tryPut(b4a.from('new-db2-a'), b4a.from('new-a'))
    w.tryPut(b4a.from('new-db2-b'), b4a.from('new-b'))
    w.tryDelete(b4a.from('key03'))
    await w.flush()
  }

  // db3 modifies different keys and adds different new ones
  {
    const w = db3.write(db1Head)
    w.tryPut(b4a.from('key07'), b4a.from('modified-by-db3'))
    w.tryPut(b4a.from('key12'), b4a.from('modified-by-db3'))
    w.tryPut(b4a.from('new-db3-x'), b4a.from('new-x'))
    w.tryDelete(b4a.from('key01'))
    w.tryDelete(b4a.from('key02'))
    await w.flush()
  }

  // Diff db2 against db3 - should show all divergent changes
  const entries = await collect(db2.createDiffStream(db3))

  // Expected differences:
  // - key01: null in db2 (unchanged from db1), deleted in db3
  // - key02: null in db2 (unchanged from db1), deleted in db3
  // - key03: deleted in db2, exists in db3
  // - key05: modified in db2, unchanged in db3
  // - key07: unchanged in db2, modified in db3
  // - key10: modified in db2, unchanged in db3
  // - key12: unchanged in db2, modified in db3
  // - key15: modified in db2, unchanged in db3
  // - new-db2-a: exists in db2, null in db3
  // - new-db2-b: exists in db2, null in db3
  // - new-db3-x: null in db2, exists in db3

  t.is(entries.length, 11)

  // Verify some key differences
  const byKey = new Map(entries.map((e) => [(e.left?.key || e.right?.key).toString(), e]))

  // key01 was deleted in db3
  t.ok(byKey.has('key01'))
  t.alike(byKey.get('key01').left.value, b4a.from('val1'))
  t.is(byKey.get('key01').right, null)

  // key03 was deleted in db2
  t.ok(byKey.has('key03'))
  t.is(byKey.get('key03').left, null)
  t.alike(byKey.get('key03').right.value, b4a.from('val3'))

  // key05 was modified in db2
  t.ok(byKey.has('key05'))
  t.alike(byKey.get('key05').left.value, b4a.from('modified-by-db2'))
  t.alike(byKey.get('key05').right.value, b4a.from('val5'))

  // new-db2-a only in db2
  t.ok(byKey.has('new-db2-a'))
  t.alike(byKey.get('new-db2-a').left.value, b4a.from('new-a'))
  t.is(byKey.get('new-db2-a').right, null)

  // new-db3-x only in db3
  t.ok(byKey.has('new-db3-x'))
  t.is(byKey.get('new-db3-x').left, null)
  t.alike(byKey.get('new-db3-x').right.value, b4a.from('new-x'))
})

test('diff cross-writer - chain of writers building on each other', async function (t) {
  // Setup: db1 -> db2 -> db3 -> db4, each adding/modifying data
  // Then diff various combinations
  const db1 = await create(t)

  // db1: initial data
  {
    const w = db1.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    w.tryPut(b4a.from('c'), b4a.from('3'))
    w.tryPut(b4a.from('d'), b4a.from('4'))
    w.tryPut(b4a.from('e'), b4a.from('5'))
    await w.flush()
  }

  const db2 = await create(t)
  replicate(t, db1, db2)

  // db2: builds on db1, modifies 'b', adds 'f'
  {
    const w = db2.write(db1.head())
    w.tryPut(b4a.from('b'), b4a.from('2-modified'))
    w.tryPut(b4a.from('f'), b4a.from('6'))
    await w.flush()
  }

  const db3 = await create(t)
  replicate(t, db2, db3)

  // db3: builds on db2, deletes 'c', modifies 'f', adds 'g'
  {
    const w = db3.write(db2.head())
    w.tryDelete(b4a.from('c'))
    w.tryPut(b4a.from('f'), b4a.from('6-modified'))
    w.tryPut(b4a.from('g'), b4a.from('7'))
    await w.flush()
  }

  const db4 = await create(t)
  replicate(t, db3, db4)

  // db4: builds on db3, modifies 'a', deletes 'e', adds 'h'
  {
    const w = db4.write(db3.head())
    w.tryPut(b4a.from('a'), b4a.from('1-modified'))
    w.tryDelete(b4a.from('e'))
    w.tryPut(b4a.from('h'), b4a.from('8'))
    await w.flush()
  }

  // Test 1: diff db4 against db1 (should show all cumulative changes)
  {
    const entries = await collect(db4.createDiffStream(db1.snapshot()))
    const byKey = new Map(entries.map((e) => [(e.left?.key || e.right?.key).toString(), e]))

    // Changes from db1 -> db4:
    // a: modified (1 -> 1-modified)
    // b: modified (2 -> 2-modified)
    // c: deleted
    // e: deleted
    // f: added
    // g: added
    // h: added

    t.is(entries.length, 7)

    t.alike(byKey.get('a').left.value, b4a.from('1-modified'))
    t.alike(byKey.get('a').right.value, b4a.from('1'))

    t.alike(byKey.get('b').left.value, b4a.from('2-modified'))
    t.alike(byKey.get('b').right.value, b4a.from('2'))

    t.is(byKey.get('c').left, null)
    t.alike(byKey.get('c').right.value, b4a.from('3'))

    t.is(byKey.get('e').left, null)
    t.alike(byKey.get('e').right.value, b4a.from('5'))

    t.alike(byKey.get('f').left.value, b4a.from('6-modified'))
    t.is(byKey.get('f').right, null)

    t.alike(byKey.get('g').left.value, b4a.from('7'))
    t.is(byKey.get('g').right, null)

    t.alike(byKey.get('h').left.value, b4a.from('8'))
    t.is(byKey.get('h').right, null)
  }

  // Test 2: diff db3 against db2 (one hop)
  {
    const entries = await collect(db3.createDiffStream(db2.snapshot()))
    const byKey = new Map(entries.map((e) => [(e.left?.key || e.right?.key).toString(), e]))

    // Changes from db2 -> db3:
    // c: deleted
    // f: modified (6 -> 6-modified)
    // g: added

    t.is(entries.length, 3)

    t.is(byKey.get('c').left, null)
    t.alike(byKey.get('c').right.value, b4a.from('3'))

    t.alike(byKey.get('f').left.value, b4a.from('6-modified'))
    t.alike(byKey.get('f').right.value, b4a.from('6'))

    t.alike(byKey.get('g').left.value, b4a.from('7'))
    t.is(byKey.get('g').right, null)
  }

  // Test 3: diff db2 against db4 (reverse direction, skip intermediate)
  {
    const entries = await collect(db2.createDiffStream(db4.snapshot()))
    const byKey = new Map(entries.map((e) => [(e.left?.key || e.right?.key).toString(), e]))

    // This is db2's view vs db4's view
    // a: db2 has '1', db4 has '1-modified'
    // c: db2 has '3', db4 has null (deleted)
    // e: db2 has '5', db4 has null (deleted)
    // f: db2 has '6', db4 has '6-modified'
    // g: db2 has null, db4 has '7'
    // h: db2 has null, db4 has '8'

    t.is(entries.length, 6)

    t.alike(byKey.get('a').left.value, b4a.from('1'))
    t.alike(byKey.get('a').right.value, b4a.from('1-modified'))

    t.alike(byKey.get('c').left.value, b4a.from('3'))
    t.is(byKey.get('c').right, null)

    t.alike(byKey.get('e').left.value, b4a.from('5'))
    t.is(byKey.get('e').right, null)

    t.alike(byKey.get('f').left.value, b4a.from('6'))
    t.alike(byKey.get('f').right.value, b4a.from('6-modified'))

    t.is(byKey.get('g').left, null)
    t.alike(byKey.get('g').right.value, b4a.from('7'))

    t.is(byKey.get('h').left, null)
    t.alike(byKey.get('h').right.value, b4a.from('8'))
  }

  // Test 4: diff with range on cross-writer scenario
  {
    const entries = await collect(
      db4.createDiffStream(db1.snapshot(), {
        gte: b4a.from('c'),
        lt: b4a.from('g')
      })
    )
    const byKey = new Map(entries.map((e) => [(e.left?.key || e.right?.key).toString(), e]))

    // Only changes in range [c, g):
    // c: deleted
    // e: deleted
    // f: added

    t.is(entries.length, 3)
    t.ok(byKey.has('c'))
    t.ok(byKey.has('e'))
    t.ok(byKey.has('f'))
    t.ok(!byKey.has('a'))
    t.ok(!byKey.has('g'))
    t.ok(!byKey.has('h'))
  }
})

async function collect(stream) {
  const entries = []
  for await (const entry of stream) {
    entries.push(entry)
  }
  return entries
}
