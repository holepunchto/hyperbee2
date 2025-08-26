const test = require('brittle')
const { create } = require('./helpers')

test('basic', async function (t) {
  const db = await create(t)
  const w = db.write()

  w.tryPut(Buffer.from('hello'), Buffer.from('world'))
  w.tryPut(Buffer.from('hej'), Buffer.from('verden'))
  w.tryPut(Buffer.from('hi'), Buffer.from('ho'))

  await w.flush()

  t.alike((await db.get(Buffer.from('hi'))).value, Buffer.from('ho'))
  t.alike((await db.get(Buffer.from('hej'))).value, Buffer.from('verden'))
  t.alike((await db.get(Buffer.from('hello'))).value, Buffer.from('world'))
})
