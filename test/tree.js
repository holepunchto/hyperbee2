const test = require('brittle')
const { TreeNode } = require('../lib/tree.js')
const { create } = require('./helpers/index.js')

test('TreeNodePointer - equivalentTo', async function (t) {
  const db = await create(t)
  await db.ready()

  const ptr = db.context.createTreeNode(0, 0, 0, false, null)
  t.ok(ptr.equivalentTo(ptr), 'equal to self')

  // Create same pointer w/ value
  const ptr2 = db.context.createTreeNode(0, 0, 0, false, new TreeNode([], []))
  t.absent(ptr.equivalentTo(ptr2), 'not equal')
})
