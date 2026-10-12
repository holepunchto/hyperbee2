const test = require('brittle')
const { Lifetime, SharedLifetime, SharedResource, timeout } = require('../lib/lifetimes.js')

test('ending parent lifetime ends child', function (t) {
  const a = new Lifetime()
  const b = a.fork()
  t.is(a.isActive(), true)
  t.is(b.isActive(), true)
  a.end()
  t.is(a.isActive(), false)
  t.is(b.isActive(), false)
})

test('ending child lifetime does not end parent', function (t) {
  const a = new Lifetime()
  const b = a.fork()
  t.is(a.isActive(), true)
  t.is(b.isActive(), true)
  b.end()
  t.is(a.isActive(), true)
  t.is(b.isActive(), false)
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

  t.is(a.isActive(), true)
  t.is(b.isActive(), false)

  t.is(ba.isActive(), false)
  t.is(baa.isActive(), false)
  t.is(bab.isActive(), false)
  t.is(baba.isActive(), false)
  t.is(babb.isActive(), false)
  t.is(babc.isActive(), false)
  t.is(babca.isActive(), false)
  t.is(bac.isActive(), false)
  t.is(bb.isActive(), false)
  t.is(bba.isActive(), false)
  t.is(bbb.isActive(), false)
  t.is(bc.isActive(), false)

  t.is(babc_signalled, true)
})

test('shared lifetime lives until all attached lifetimes have ended or detached', function (t) {
  const a = new Lifetime()
  const b = new Lifetime()
  const c = new Lifetime()

  const shared = new SharedLifetime(a)
  shared.attach(b)
  shared.attach(c)

  t.is(shared.isActive(), true)
  b.end()
  t.is(shared.isActive(), true)
  shared.detach(a)
  t.is(shared.isActive(), true)
  c.end()
  t.is(shared.isActive(), false)
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
    t.is(a.isActive(), false)
    t.is(b.isActive(), true)
    t.is(c.isActive(), true)
    // clears timeout on b
    cancel()
  }, 100)

  setTimeout(() => {
    t.is(a.isActive(), false)
    t.is(b.isActive(), true)
    t.is(c.isActive(), false)
  }, 300)
})

test('callbacks unwound in reverse of order added', function (t) {
  const lifetime = new Lifetime()

  const calls = []

  const cb1 = () => {
    calls.push(1)
  }
  const cb2 = () => {
    calls.push(2)
  }
  const cb3 = () => {
    calls.push(3)
  }
  lifetime.wait(cb1)
  lifetime.wait(cb2)
  lifetime.wait(cb3)

  lifetime.end()

  t.alike(calls, [3, 2, 1])
})

test('unwait during signalling', function (t) {
  const lifetime = new Lifetime()

  const calls = []

  const cb1 = () => {
    calls.push(1)
  }
  const cb2 = () => {
    calls.push(2)
  }
  const cb3 = () => {
    calls.push(3)
    lifetime.unwait(cb2)
  }
  lifetime.wait(cb1)
  lifetime.wait(cb2)
  lifetime.wait(cb3)

  lifetime.end()

  t.alike(calls, [3, 1])
})

test('wait during signalling', function (t) {
  const lifetime = new Lifetime()

  const calls = []

  const cb1 = () => {
    calls.push(1)
  }
  const cb2 = () => {
    calls.push(2)
  }
  const cb2_5 = () => {
    calls.push(2.5)
  }
  const cb3 = () => {
    // NOTE: this should not call cb2_2 immediately but wait until this
    // callback has completed and pushed 3 first
    lifetime.wait(cb2_5)
    calls.push(3)
  }
  lifetime.wait(cb1)
  lifetime.wait(cb2)
  lifetime.wait(cb3)

  lifetime.end()

  t.alike(calls, [3, 2.5, 2, 1])
})

test('wait after lifetime has ended', function (t) {
  const lifetime = new Lifetime()
  lifetime.end()

  const calls = []

  const cb1 = () => {
    calls.push(1)
  }
  lifetime.wait(cb1)

  t.alike(calls, [1])
})

test('unwind a nested tree of lifetimes in reverse order', function (t) {
  const a = new Lifetime()

  const calls = []

  const b = a.fork().wait(() => calls.push('b'))
  const ba = b.fork().wait(() => calls.push('ba'))
  const baa = ba.fork().wait(() => calls.push('baa'))
  const bab = ba.fork().wait(() => calls.push('bab'))
  const baba = bab.fork().wait(() => calls.push('baba'))
  const babb = bab.fork().wait(() => calls.push('babb'))
  const babc = bab.fork().wait(() => calls.push('babc'))
  const babca = babc.fork().wait(() => calls.push('babca'))
  const bac = ba.fork().wait(() => calls.push('bac'))
  const bb = b.fork().wait(() => calls.push('bb'))
  const bba = bb.fork().wait(() => calls.push('bba'))
  const bbb = bb.fork().wait(() => calls.push('bbb'))
  const bc = b.fork().wait(() => calls.push('bc'))

  b.end()

  t.alike(calls, [
    'bc',
    'bbb',
    'bba',
    'bb',
    'bac',
    'babca',
    'babc',
    'babb',
    'baba',
    'bab',
    'baa',
    'ba',
    'b'
  ])
})

test('add descendant during signalling', function (t) {
  const calls = []

  const root = new Lifetime().wait(() => calls.push(0))
  root.fork().wait(() => calls.push(1))
  root.fork().wait(() => {
    root.fork().wait(() => calls.push(1.5))
    calls.push(2)
  })

  root.end()

  t.alike(calls, [2, 1.5, 1, 0])
})
