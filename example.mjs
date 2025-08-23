import Hyperbee from './index.js'
import Hypercore from 'hypercore'

const b = new Hyperbee(new Hypercore('./sandbox/core'))

await b.ready()

if (b.core.length === 0) {
  const w = b.write()

  for (let i = 0; i < 100; i++) {
    w.tryPut(Buffer.from('#' + i), Buffer.from('#' + i))
  }

  await w.flush()
}

// console.log(await b.tmp())

for (let i = 0; i < 100; i++) {
  console.log(await b.get(Buffer.from('#' + i)))
}
// console.log(b.core.length)

// for await (const data of b.createReadStream({ gte: Buffer.from('#33'), lte: Buffer.from('#49') })) {
//   console.log(data.key.toString())
// }
