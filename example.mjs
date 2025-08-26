import Hyperbee from './index.js'
import Corestore from 'corestore'

const b = new Hyperbee(new Corestore('./sandbox/store'))

await b.ready()

if (b.core.length === 0) {
  const w = b.write()

  for (let i = 0; i < 100; i++) {
    w.tryPut(Buffer.from('#' + i), Buffer.from('#' + i))
  }

  await w.flush()
}

const c = new Hyperbee(b.store, { core: b.store.get({ name: 'bee2' }) })
await c.ready()

if (c.core.length === 0) {
  const w = c.write({ length: b.core.length, key: b.core.key })

  w.tryPut(Buffer.from('yo'), Buffer.from('yo'))
  await w.flush()
}

const s = c.checkout(b.core.length, b.core.key)

console.log(!!(await c.get(Buffer.from('yo'))))
console.log(!!(await s.get(Buffer.from('yo'))))

for await (const data of c.createReadStream()) {
  console.log(data.key.toString())
}

console.log()

for await (const data of s.createReadStream()) {
  console.log(data.key.toString())
}
