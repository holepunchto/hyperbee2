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

// === Advanced Test Cases ===

test('diff with interleaved insertions and deletions', async function (t) {
  const db = await create(t)

  // Create initial tree with gaps
  {
    const w = db.write()
    for (let i = 0; i < 50; i += 2) {
      w.tryPut(b4a.from(String(i).padStart(3, '0')), b4a.from('v' + i))
    }
    await w.flush()
  }

  const snap = db.snapshot()

  // Fill in the gaps and remove some existing
  {
    const w = db.write()
    for (let i = 1; i < 50; i += 2) {
      w.tryPut(b4a.from(String(i).padStart(3, '0')), b4a.from('v' + i))
    }
    for (let i = 0; i < 50; i += 4) {
      w.tryDelete(b4a.from(String(i).padStart(3, '0')))
    }
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  // Should have: 25 additions (odd numbers) + ~13 deletions (every 4th even)
  const added = entries.filter((e) => e.right === null)
  const deleted = entries.filter((e) => e.left === null)

  t.is(added.length, 25) // 1,3,5,...,49
  t.is(deleted.length, 13) // 0,4,8,...,48

  // Verify order
  for (let i = 1; i < entries.length; i++) {
    const prevKey = entries[i - 1].left?.key || entries[i - 1].right?.key
    const currKey = entries[i].left?.key || entries[i].right?.key
    t.ok(b4a.compare(prevKey, currKey) < 0, 'entries should be in order')
  }
})

test('diff with complete tree replacement', async function (t) {
  const db = await create(t)

  // Create tree with keys a-z
  {
    const w = db.write()
    for (let i = 0; i < 26; i++) {
      w.tryPut(b4a.from(String.fromCharCode(97 + i)), b4a.from('old-' + i))
    }
    await w.flush()
  }

  const snap = db.snapshot()

  // Delete all and insert completely new keys
  {
    const w = db.write()
    for (let i = 0; i < 26; i++) {
      w.tryDelete(b4a.from(String.fromCharCode(97 + i)))
    }
    for (let i = 0; i < 26; i++) {
      w.tryPut(b4a.from(String.fromCharCode(65 + i)), b4a.from('new-' + i)) // A-Z
    }
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  t.is(entries.length, 52) // 26 deletions + 26 additions

  // All A-Z should be additions (uppercase comes before lowercase in ASCII)
  for (let i = 0; i < 26; i++) {
    t.alike(entries[i].left.key, b4a.from(String.fromCharCode(65 + i)))
    t.is(entries[i].right, null)
  }

  // All a-z should be deletions
  for (let i = 0; i < 26; i++) {
    t.is(entries[26 + i].left, null)
    t.alike(entries[26 + i].right.key, b4a.from(String.fromCharCode(97 + i)))
  }
})

test('diff with deep tree modifications', async function (t) {
  const db = await create(t)

  // Create a large tree to force multiple levels
  {
    const w = db.write()
    for (let i = 0; i < 500; i++) {
      w.tryPut(b4a.from(String(i).padStart(4, '0')), b4a.from('v' + i))
    }
    await w.flush()
  }

  const snap = db.snapshot()

  // Modify keys at various positions to touch different subtrees
  {
    const w = db.write()
    w.tryPut(b4a.from('0001'), b4a.from('modified-start'))
    w.tryPut(b4a.from('0250'), b4a.from('modified-middle'))
    w.tryPut(b4a.from('0499'), b4a.from('modified-end'))
    w.tryDelete(b4a.from('0100'))
    w.tryDelete(b4a.from('0400'))
    w.tryPut(b4a.from('0500'), b4a.from('new-beyond'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  t.is(entries.length, 6)

  const byKey = new Map(entries.map((e) => [(e.left?.key || e.right?.key).toString(), e]))

  t.alike(byKey.get('0001').left.value, b4a.from('modified-start'))
  t.alike(byKey.get('0001').right.value, b4a.from('v1'))
  t.is(byKey.get('0100').left, null)
  t.alike(byKey.get('0250').left.value, b4a.from('modified-middle'))
  t.is(byKey.get('0400').left, null)
  t.alike(byKey.get('0499').left.value, b4a.from('modified-end'))
  t.alike(byKey.get('0500').left.value, b4a.from('new-beyond'))
  t.is(byKey.get('0500').right, null)
})

test('diff with value-only changes (same keys)', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    for (let i = 0; i < 100; i++) {
      w.tryPut(b4a.from('key' + String(i).padStart(3, '0')), b4a.from('value-v1-' + i))
    }
    await w.flush()
  }

  const snap = db.snapshot()

  // Change every value but keep all keys
  {
    const w = db.write()
    for (let i = 0; i < 100; i++) {
      w.tryPut(b4a.from('key' + String(i).padStart(3, '0')), b4a.from('value-v2-' + i))
    }
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  t.is(entries.length, 100)

  for (const e of entries) {
    t.ok(e.left !== null)
    t.ok(e.right !== null)
    t.alike(e.left.key, e.right.key)
    t.ok(e.left.value.toString().includes('v2'))
    t.ok(e.right.value.toString().includes('v1'))
  }
})

test('diff with binary keys', async function (t) {
  const db = await create(t)

  // Use binary keys with null bytes and high bytes
  {
    const w = db.write()
    w.tryPut(Buffer.from([0x00, 0x00, 0x01]), b4a.from('1'))
    w.tryPut(Buffer.from([0x00, 0x00, 0x02]), b4a.from('2'))
    w.tryPut(Buffer.from([0xff, 0xff, 0x01]), b4a.from('3'))
    w.tryPut(Buffer.from([0xff, 0xff, 0x02]), b4a.from('4'))
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryPut(Buffer.from([0x00, 0x00, 0x01]), b4a.from('1-mod'))
    w.tryDelete(Buffer.from([0xff, 0xff, 0x01]))
    w.tryPut(Buffer.from([0x80, 0x00, 0x00]), b4a.from('middle'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  t.is(entries.length, 3)

  // Should be in binary order
  t.alike(entries[0].left.key, Buffer.from([0x00, 0x00, 0x01]))
  t.alike(entries[1].left.key, Buffer.from([0x80, 0x00, 0x00]))
  t.is(entries[1].right, null) // addition
  t.is(entries[2].left, null) // deletion
  t.alike(entries[2].right.key, Buffer.from([0xff, 0xff, 0x01]))
})

test('diff with very long keys', async function (t) {
  const db = await create(t)

  const longKey1 = b4a.from('a'.repeat(1000))
  const longKey2 = b4a.from('b'.repeat(1000))
  const longKey3 = b4a.from('c'.repeat(1000))

  {
    const w = db.write()
    w.tryPut(longKey1, b4a.from('1'))
    w.tryPut(longKey2, b4a.from('2'))
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryPut(longKey2, b4a.from('2-modified'))
    w.tryPut(longKey3, b4a.from('3'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  t.is(entries.length, 2)
  t.alike(entries[0].left.key, longKey2)
  t.alike(entries[0].left.value, b4a.from('2-modified'))
  t.alike(entries[1].left.key, longKey3)
  t.is(entries[1].right, null)
})

test('diff with very long values', async function (t) {
  const db = await create(t)

  const longValue1 = b4a.from('x'.repeat(10000))
  const longValue2 = b4a.from('y'.repeat(10000))

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), longValue1)
    w.tryPut(b4a.from('b'), longValue1)
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), longValue2)
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  t.is(entries.length, 1)
  t.alike(entries[0].left.value, longValue2)
  t.alike(entries[0].right.value, longValue1)
})

test('diff with limit 0', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryPut(b4a.from('b'), b4a.from('2'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap, { limit: 0 }))
  t.is(entries.length, 0)
})

test('diff with limit 1', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1*'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    w.tryPut(b4a.from('c'), b4a.from('3'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap, { limit: 1 }))
  t.is(entries.length, 1)
  t.alike(entries[0].left.key, b4a.from('a'))
})

test('diff cross-writer with shared unchanged subtrees', async function (t) {
  // This tests the subtree skip optimization across writers
  const db1 = await create(t)

  // Create a large tree
  {
    const w = db1.write()
    for (let i = 0; i < 200; i++) {
      w.tryPut(b4a.from(String(i).padStart(4, '0')), b4a.from('v' + i))
    }
    await w.flush()
  }

  const db2 = await create(t)
  replicate(t, db1, db2)

  // db2 only modifies one key - most subtrees should be skipped
  {
    const w = db2.write(db1.head())
    w.tryPut(b4a.from('0100'), b4a.from('modified'))
    await w.flush()
  }

  const entries = await collect(db2.createDiffStream(db1.snapshot()))

  // Should only see the one modification
  t.is(entries.length, 1)
  t.alike(entries[0].left.key, b4a.from('0100'))
  t.alike(entries[0].left.value, b4a.from('modified'))
  t.alike(entries[0].right.value, b4a.from('v100'))
})

test('diff cross-writer three-way divergence', async function (t) {
  // db1 creates base, db2/db3/db4 all fork from db1 with different changes
  const db1 = await create(t)

  {
    const w = db1.write()
    for (let i = 0; i < 10; i++) {
      w.tryPut(b4a.from('k' + i), b4a.from('v' + i))
    }
    await w.flush()
  }

  const db1Head = db1.head()

  const db2 = await create(t)
  const db3 = await create(t)
  const db4 = await create(t)

  replicate(t, db1, db2)
  replicate(t, db1, db3)
  replicate(t, db1, db4)

  // Each writer modifies different keys
  {
    const w = db2.write(db1Head)
    w.tryPut(b4a.from('k0'), b4a.from('db2-k0'))
    w.tryPut(b4a.from('k1'), b4a.from('db2-k1'))
    await w.flush()
  }

  {
    const w = db3.write(db1Head)
    w.tryPut(b4a.from('k3'), b4a.from('db3-k3'))
    w.tryPut(b4a.from('k4'), b4a.from('db3-k4'))
    await w.flush()
  }

  {
    const w = db4.write(db1Head)
    w.tryPut(b4a.from('k6'), b4a.from('db4-k6'))
    w.tryPut(b4a.from('k7'), b4a.from('db4-k7'))
    await w.flush()
  }

  // Diff db2 vs db3
  {
    const entries = await collect(db2.createDiffStream(db3))
    t.is(entries.length, 4) // k0, k1 differ (db2 modified), k3, k4 differ (db3 modified)

    const byKey = new Map(entries.map((e) => [(e.left?.key || e.right?.key).toString(), e]))
    t.alike(byKey.get('k0').left.value, b4a.from('db2-k0'))
    t.alike(byKey.get('k0').right.value, b4a.from('v0'))
    t.alike(byKey.get('k3').left.value, b4a.from('v3'))
    t.alike(byKey.get('k3').right.value, b4a.from('db3-k3'))
  }

  // Diff db2 vs db4
  {
    const entries = await collect(db2.createDiffStream(db4))
    t.is(entries.length, 4) // k0, k1 from db2, k6, k7 from db4

    const byKey = new Map(entries.map((e) => [(e.left?.key || e.right?.key).toString(), e]))
    t.alike(byKey.get('k0').left.value, b4a.from('db2-k0'))
    t.alike(byKey.get('k6').right.value, b4a.from('db4-k6'))
  }

  // Diff db3 vs db4
  {
    const entries = await collect(db3.createDiffStream(db4))
    t.is(entries.length, 4) // k3, k4 from db3, k6, k7 from db4

    const byKey = new Map(entries.map((e) => [(e.left?.key || e.right?.key).toString(), e]))
    t.alike(byKey.get('k3').left.value, b4a.from('db3-k3'))
    t.alike(byKey.get('k7').right.value, b4a.from('db4-k7'))
  }
})

test('diff with multiple sequential batches', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    await w.flush()
  }

  const snap = db.snapshot()

  // Multiple batches after snapshot
  {
    const w = db.write()
    w.tryPut(b4a.from('b'), b4a.from('2'))
    await w.flush()
  }

  {
    const w = db.write()
    w.tryPut(b4a.from('c'), b4a.from('3'))
    await w.flush()
  }

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1*'))
    w.tryDelete(b4a.from('b'))
    await w.flush()
  }

  {
    const w = db.write()
    w.tryPut(b4a.from('d'), b4a.from('4'))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  t.is(entries.length, 3) // a modified, c added, d added (b was added then deleted)
  t.alike(entries[0].left.key, b4a.from('a'))
  t.alike(entries[0].left.value, b4a.from('1*'))
  t.alike(entries[1].left.key, b4a.from('c'))
  t.alike(entries[2].left.key, b4a.from('d'))
})

test('diff range that starts and ends within unchanged regions', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    for (let i = 0; i < 100; i++) {
      w.tryPut(b4a.from(String(i).padStart(3, '0')), b4a.from('v' + i))
    }
    await w.flush()
  }

  const snap = db.snapshot()

  // Only modify keys outside the range we'll query
  {
    const w = db.write()
    w.tryPut(b4a.from('010'), b4a.from('modified'))
    w.tryPut(b4a.from('090'), b4a.from('modified'))
    await w.flush()
  }

  // Query range that has no changes
  const entries = await collect(
    db.createDiffStream(snap, { gte: b4a.from('040'), lt: b4a.from('060') })
  )

  t.is(entries.length, 0)
})

test('diff range that captures only some changes', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    for (let i = 0; i < 100; i++) {
      w.tryPut(b4a.from(String(i).padStart(3, '0')), b4a.from('v' + i))
    }
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryPut(b4a.from('020'), b4a.from('mod1'))
    w.tryPut(b4a.from('050'), b4a.from('mod2'))
    w.tryPut(b4a.from('080'), b4a.from('mod3'))
    await w.flush()
  }

  // Range captures only middle change
  const entries = await collect(
    db.createDiffStream(snap, { gte: b4a.from('040'), lt: b4a.from('060') })
  )

  t.is(entries.length, 1)
  t.alike(entries[0].left.key, b4a.from('050'))
})

test('diff symmetry - a vs b should be inverse of b vs a', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    w.tryPut(b4a.from('c'), b4a.from('3'))
    await w.flush()
  }

  const snap1 = db.snapshot()

  {
    const w = db.write()
    w.tryPut(b4a.from('b'), b4a.from('2*'))
    w.tryDelete(b4a.from('c'))
    w.tryPut(b4a.from('d'), b4a.from('4'))
    await w.flush()
  }

  const snap2 = db.snapshot()

  const forward = await collect(snap2.createDiffStream(snap1))
  const backward = await collect(snap1.createDiffStream(snap2))

  t.is(forward.length, backward.length)

  // Each forward entry should have an inverse backward entry
  for (let i = 0; i < forward.length; i++) {
    const f = forward[i]
    const b = backward[i]

    if (f.left && f.right) {
      // Modification - should be swapped
      t.alike(f.left.key, b.right.key)
      t.alike(f.right.key, b.left.key)
      t.alike(f.left.value, b.right.value)
      t.alike(f.right.value, b.left.value)
    } else if (f.left && !f.right) {
      // Addition in forward = deletion in backward
      t.alike(f.left.key, b.right.key)
      t.is(b.left, null)
    } else {
      // Deletion in forward = addition in backward
      t.alike(f.right.key, b.left.key)
      t.is(b.right, null)
    }
  }
})

test('diff with same key written multiple times', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('x'), b4a.from('initial'))
    await w.flush()
  }

  const snap = db.snapshot()

  // Write same key multiple times in sequence
  for (let i = 0; i < 10; i++) {
    const w = db.write()
    w.tryPut(b4a.from('x'), b4a.from('version-' + i))
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  // Should only see the final diff
  t.is(entries.length, 1)
  t.alike(entries[0].left.value, b4a.from('version-9'))
  t.alike(entries[0].right.value, b4a.from('initial'))
})

test('diff empty range on non-empty trees', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    w.tryPut(b4a.from('mmm'), b4a.from('1'))
    await w.flush()
  }

  const snap = db.snapshot()

  {
    const w = db.write()
    w.tryPut(b4a.from('mmm'), b4a.from('2'))
    await w.flush()
  }

  // Range before all data
  const e1 = await collect(db.createDiffStream(snap, { gte: b4a.from('aaa'), lt: b4a.from('bbb') }))
  t.is(e1.length, 0)

  // Range after all data
  const e2 = await collect(
    db.createDiffStream(snap, { gte: b4a.from('zzz'), lt: b4a.from('zzzz') })
  )
  t.is(e2.length, 0)
})

test('diff cross-writer with no common history', async function (t) {
  // Two completely independent databases
  const db1 = await create(t)
  const db2 = await create(t)

  {
    const w = db1.write()
    w.tryPut(b4a.from('a'), b4a.from('1'))
    w.tryPut(b4a.from('b'), b4a.from('2'))
    await w.flush()
  }

  {
    const w = db2.write()
    w.tryPut(b4a.from('c'), b4a.from('3'))
    w.tryPut(b4a.from('d'), b4a.from('4'))
    await w.flush()
  }

  // Diff independent databases - all keys differ
  const entries = await collect(db1.createDiffStream(db2))

  t.is(entries.length, 4)

  const byKey = new Map(entries.map((e) => [(e.left?.key || e.right?.key).toString(), e]))

  // db1 has a,b - db2 doesn't
  t.alike(byKey.get('a').left.value, b4a.from('1'))
  t.is(byKey.get('a').right, null)
  t.alike(byKey.get('b').left.value, b4a.from('2'))
  t.is(byKey.get('b').right, null)

  // db2 has c,d - db1 doesn't
  t.is(byKey.get('c').left, null)
  t.alike(byKey.get('c').right.value, b4a.from('3'))
  t.is(byKey.get('d').left, null)
  t.alike(byKey.get('d').right.value, b4a.from('4'))
})

test('diff handles deletion of all keys', async function (t) {
  const db = await create(t)

  {
    const w = db.write()
    for (let i = 0; i < 50; i++) {
      w.tryPut(b4a.from('key' + String(i).padStart(2, '0')), b4a.from('val' + i))
    }
    await w.flush()
  }

  const snap = db.snapshot()

  // Delete everything
  {
    const w = db.write()
    for (let i = 0; i < 50; i++) {
      w.tryDelete(b4a.from('key' + String(i).padStart(2, '0')))
    }
    await w.flush()
  }

  const entries = await collect(db.createDiffStream(snap))

  t.is(entries.length, 50)

  for (const e of entries) {
    t.is(e.left, null)
    t.ok(e.right !== null)
  }
})

test('diff cross-writer builds long chain then compares ends', async function (t) {
  // Create a long chain: db1 -> db2 -> db3 -> ... -> db10
  const dbs = [await create(t)]

  // Initialize first db
  {
    const w = dbs[0].write()
    w.tryPut(b4a.from('root'), b4a.from('v0'))
    await w.flush()
  }

  // Build chain of 9 more writers
  for (let i = 1; i < 10; i++) {
    const newDb = await create(t)
    replicate(t, dbs[i - 1], newDb)

    const w = newDb.write(dbs[i - 1].head())
    w.tryPut(b4a.from('key' + i), b4a.from('val' + i))
    await w.flush()

    dbs.push(newDb)
  }

  // Diff first vs last
  const entries = await collect(dbs[9].createDiffStream(dbs[0].snapshot()))

  // Should see all 9 additions
  t.is(entries.length, 9)

  for (let i = 1; i < 10; i++) {
    const found = entries.find((e) => e.left?.key?.toString() === 'key' + i)
    t.ok(found, 'should have key' + i)
    t.is(found.right, null)
  }
})

async function collect(stream) {
  const entries = []
  for await (const entry of stream) {
    entries.push(entry)
  }
  return entries
}
