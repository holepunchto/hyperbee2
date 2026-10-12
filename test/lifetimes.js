const test = require('brittle')
const { Lifetime, SharedLifetime, SharedResource, timeout } = require('../lib/lifetimes.js')

test('ending parent lifetime ends child', function (t) {
  const a = new Lifetime()
  const b = a.fork()
  t.is(a.ended, false)
  t.is(b.ended, false)
  a.end()
  t.is(a.ended, true)
  t.is(b.ended, true)
})

test('ending child lifetime does not end parent', function (t) {
  const a = new Lifetime()
  const b = a.fork()
  t.is(a.ended, false)
  t.is(b.ended, false)
  b.end()
  t.is(a.ended, false)
  t.is(b.ended, true)
})

test('signal waiting callback on lifetime end', function (t) {
  t.plan(1)
  const a = new Lifetime()
  a.wait(() => t.ok(true))
  a.end()
})

test('remove waiting callback from lifetime', function (t) {
  t.plan(0)
  const a = new Lifetime()
  const cb = () => t.ok(false)
  a.wait(cb)
  a.unwait(cb)
  a.end()
})

test('ending nested subtree of lifetimes', function (t) {
  const a = new Lifetime()
  const b = a.fork()

  const ba = b.fork()
  const baa = ba.fork()
  const bab = ba.fork()
  const baba = bab.fork()
  const babb = bab.fork()
  const babc = bab.fork()
  const babca = babc.fork()
  const bac = ba.fork()

  const bb = b.fork()
  const bba = bb.fork()
  const bbb = bb.fork()

  const bc = b.fork()

  let babc_signalled = false
  babc.wait(() => {
    babc_signalled = true
  })

  b.end()

  t.is(a.ended, false)
  t.is(b.ended, true)

  t.is(ba.ended, true)
  t.is(baa.ended, true)
  t.is(bab.ended, true)
  t.is(baba.ended, true)
  t.is(babb.ended, true)
  t.is(babc.ended, true)
  t.is(babca.ended, true)
  t.is(bac.ended, true)
  t.is(bb.ended, true)
  t.is(bba.ended, true)
  t.is(bbb.ended, true)
  t.is(bc.ended, true)

  t.is(babc_signalled, true)
})

test('shared lifetime lives until all attached lifetimes have ended or detached', function (t) {
  const a = new Lifetime()
  const b = new Lifetime()
  const c = new Lifetime()

  const shared = new SharedLifetime(a)
  shared.attach(b)
  shared.attach(c)

  t.is(shared.ended, false)
  b.end()
  t.is(shared.ended, false)
  shared.detach(a)
  t.is(shared.ended, false)
  c.end()
  t.is(shared.ended, true)
})

test('shared resource', function (t) {
  // Example shared object
  const data = {
    state: 1,
    destroy() {
      this.state = -1
    }
  }

  // Create new shared resource from object
  const session1 = new Lifetime()
  const res = new SharedResource(session1, data, () => data.destroy())

  t.is(res.value.state, 1)

  // Attach another session lifetime to this resource
  const session2 = new Lifetime()
  res.attach(session2)

  t.is(res.value.state, 1)

  // End first session and resource persists
  session1.end()
  t.is(res.value.state, 1)

  // End second session and resource is destroyed
  session2.end()
  t.is(res.value.state, -1)
})

test('timeout', function (t) {
  t.plan(6)
  const a = new Lifetime()
  const b = new Lifetime()
  const c = new Lifetime()

  timeout(a, 0)
  const cancel = timeout(b, 200)
  timeout(c, 200)

  setTimeout(() => {
    t.is(a.ended, true)
    t.is(b.ended, false)
    t.is(c.ended, false)
    // clears timeout on b
    cancel()
  }, 100)

  setTimeout(() => {
    t.is(a.ended, true)
    t.is(b.ended, false)
    t.is(c.ended, true)
  }, 300)
})
