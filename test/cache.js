const test = require('brittle')
const NodeCache = require('../lib/cache.js')

const context = { getCore: (core) => ({ id: 'core' + core }) }

function pointer(seq, offset = 0) {
  return { context, core: 0, seq, offset, value: { seq }, prev: null, next: null, retained: 0 }
}

function ring(cache) {
  const nodes = []
  for (const node of cache) nodes.push(node)
  return nodes
}

test('cache - a second pointer for the same node replaces the first', function (t) {
  const cache = new NodeCache(4)
  const a = pointer(1)
  const b = pointer(1)

  cache.bump(a)
  cache.bump(b)

  t.is(cache.size, 1)
  t.alike(ring(cache), [b])
  t.is(cache.get(a), b, 'lookups find the newer pointer')
  t.is(a.value, null, 'the old pointer was evicted')
  t.is(a.prev, null)
  t.ok(b.value, 'the new pointer keeps its value')
})

test('cache - a pointer pinned by a write is not replaced', function (t) {
  const cache = new NodeCache(4)
  const a = pointer(1)
  const b = pointer(1)

  cache.bump(a)
  a.retained = cache.retained + 1
  cache.bump(b)

  t.is(cache.size, 1)
  t.alike(ring(cache), [a])
  t.is(cache.get(b), a)
  t.ok(a.value && b.value, 'both keep their values')
  t.is(b.prev, null, 'the newcomer stayed outside the ring')

  cache.flush()
  cache.bump(b)
  t.alike(ring(cache), [b], 'after the write it is replaced')
  t.is(a.value, null)
})

test('cache - replacing the newest pointer keeps the eviction order', function (t) {
  const cache = new NodeCache(3)
  const a = pointer(1)
  const b = pointer(2)
  const c = pointer(3)
  const c2 = pointer(3)

  cache.bump(a)
  cache.bump(b)
  cache.bump(c)
  cache.bump(c2)

  t.alike(ring(cache), [c2, a, b], 'newest first, then oldest to newest')

  cache.bump(pointer(4))
  cache.gc()
  t.is(a.value, null, 'the oldest pointer went first')
  t.ok(b.value && c2.value)
})

test('cache - gc evicts from the oldest and spares pinned pointers', function (t) {
  const cache = new NodeCache(2)
  const a = pointer(1)
  const b = pointer(2)
  const c = pointer(3)

  cache.bump(a)
  a.retained = cache.retained + 1
  cache.bump(b)
  cache.bump(c)
  cache.gc()

  t.is(cache.size, 3, 'a pinned oldest pointer stops gc')
  t.ok(a.value && b.value && c.value)

  cache.flush()
  t.is(cache.size, 2)
  t.is(a.value, null)
  t.ok(b.value && c.value)
})

test('cache - empty drops everything except pinned pointers', function (t) {
  const cache = new NodeCache(8)
  const nodes = [pointer(1), pointer(2), pointer(3), pointer(4)]
  for (const node of nodes) cache.bump(node)
  nodes[0].retained = cache.retained + 1
  nodes[3].retained = cache.retained + 1

  cache.empty()

  t.alike(ring(cache), [nodes[3], nodes[0]], 'the pinned oldest and newest stay')
  t.is(nodes[1].value, null)
  t.is(nodes[2].value, null)

  cache.flush()
  cache.empty()
  t.is(cache.size, 0)
})
