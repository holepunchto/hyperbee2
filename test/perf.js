const test = require('brittle')
const b4a = require('b4a')
const { create } = require('./helpers')

test('perf - warm tree does no block reads while writing and reading', async function (t) {
  let reads = 0
  const db = await create(t, { trace: () => reads++ })

  const N = 20000
  for (let i = 0; i < N; i += 100) {
    const w = db.write()
    for (let j = i; j < i + 100; j++) w.tryPut(key(j), b4a.from('v' + j))
    await w.flush()
  }

  for (let i = 0; i < N; i += 7) await db.get(key(i))

  reads = 0
  const start = Date.now()

  for (let r = 0; r < 200; r++) {
    const w = db.write()
    for (let j = 0; j < 20; j++) w.tryPut(key((r * 977 + j * 131) % N), b4a.from('x'))
    await w.flush()

    for (let j = 0; j < 20; j++) await db.get(key((r * 613 + j * 37) % N))

    const co = db.checkout({ length: db.core.length })
    await co.get(key(r))
    await co.close()
  }

  t.comment(`${reads} block reads, ${Date.now() - start}ms`)
  t.is(reads, 0, 'everything served from the node cache')

  function key(i) {
    return b4a.from(String(i).padStart(9, '0'))
  }
})
