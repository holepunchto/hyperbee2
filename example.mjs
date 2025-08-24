import Hyperbee from './index.js'
import Hypercore from 'hypercore'

const b = new Hyperbee(new Hypercore('./sandbox/core'))

await b.ready()
const last = await b.peek({ reverse: true })

const n = last ? Number(last.key.toString().slice(1)) + 1 : 0

// const w = b.write()

// for (let i = n; i < n + 50_000; i++) {
//   const k = '#' + i.toString().padStart(7, '0')
//   w.tryPut(Buffer.from(k), Buffer.from('' + i))
// }

// console.time()
// await w.flush()
// console.timeEnd()
// console.log(b.core.length)

// console.log((await b.peek({ reverse: true })).key.toString())

console.time()
// let m = 0
for await (const data of b.createReadStream()) {
  // await b.get(Buffer.from('#' + (m++).toString().padStart(7, '0')))
  // console.log('-->', data.key.toString())
}
console.timeEnd()

// for (let i = 0; i < 100; i++) {
//   console.log(await b.get(Buffer.from('#' + i)))
// }
// console.log(b.core.length)

// for await (const data of b.createReadStream({ gte: Buffer.from('#33'), lte: Buffer.from('#49') })) {
//   console.log(data.key.toString())
// }
