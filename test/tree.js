const test = require('brittle')
const { TreeNode } = require('../lib/tree.js')
const { CompressedArray, OP_COHORT } = require('../lib/compression.js')
const { create } = require('./helpers/index.js')

test('TreeNodePointer - equivalentTo', async function (t) {
  const db = await create(t)
  await db.ready()

  const ptr = db.context.createTreeNode(0, 0, 0, false, null)
  t.ok(ptr.equivalentTo(ptr), 'equal to self')

  // Create same pointer w/ value
  const ptr2 = db.context.createTreeNode(0, 0, 0, false, new TreeNode(0, [], []))
  t.absent(ptr.equivalentTo(ptr2), 'not equal w/ different values')
})

test('CompressedArray - flush rebases when entries are smaller than the delta', function (t) {
  const a = new CompressedArray([])

  // churn: 6 inserts and 4 deletes leave 2 entries behind a 10 op delta
  for (let i = 0; i < 6; i++) a.insert(i, { changedBy: null })
  for (let i = 0; i < 4; i++) a.delete(0)

  const c = a.commit()

  t.is(c.entries.length, 2)
  t.is(c.delta.length, 10)

  const delta = c.flush(16, 1)

  t.is(delta.length, 1, 'rebased to a single cohort')
  t.is(delta[0].type, OP_COHORT)
  t.is(delta[0].deltas.length, 2, 'cohort holds the raw entries')
  t.is(delta[0].deltas[0].pointer, c.entries[0])
  t.is(delta[0].deltas[1].pointer, c.entries[1])
})

test('CompressedArray - flush keeps a delta not bigger than the entries', function (t) {
  const a = new CompressedArray([])

  for (let i = 0; i < 4; i++) a.insert(i, { changedBy: null })

  const c = a.commit()
  const delta = c.flush(16, 1)

  t.is(delta.length, 4, 'delta kept as is')
})
