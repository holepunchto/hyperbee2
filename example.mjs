import Hyperbee from './index.js'
import ChangesStream from './lib/changes-stream.js'
import Corestore from 'corestore'

const b = new Hyperbee(new Corestore('./sandbox/store'))

await b.ready()

if (b.core.length === 0) {
  {
    const w = b.write()

    for (let i = 0; i < 10; i++) {
      w.tryPut(Buffer.from('#' + i), Buffer.from('#' + i))
    }

    await w.flush()
  }

  {
    const w = b.write()

    for (let i = 0; i < 10; i++) {
      w.tryPut(Buffer.from('#' + i), Buffer.from('#' + i))
    }

    await w.flush()
  }
}

const c = new ChangesStream(b)

c.on('data', console.log)

// const c = new Hyperbee(b.store, { core: b.store.get({ name: 'bee2' }) })
// await c.ready()

// if (c.core.length === 0) {
//   const w = c.write({ length: b.core.length, key: b.core.key })

//   w.tryPut(Buffer.from('yo'), Buffer.from('yo'))
//   await w.flush()
// }

// const s = c.checkout(b.head())

// console.log(!!(await c.get(Buffer.from('yo'))))
// console.log(!!(await s.get(Buffer.from('yo'))))

// for await (const data of c.createReadStream()) {
//   console.log(data.key.toString())
// }

// console.log()

// for await (const data of s.createReadStream()) {
//   console.log(data.key.toString())
// }
