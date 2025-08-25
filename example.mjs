import Hyperbee from './index.js'
import Hypercore from 'hypercore'

const b = new Hyperbee(new Hypercore('./sandbox/core'))

await b.ready()
const last = await b.peek({ reverse: true })

const n = last ? Number(last.key.toString().slice(1)) + 1 : 0
console.log('last', n)

{
  const w = b.write({ truncate: 0 })
  w.tryPut(Buffer.from('#0*'), Buffer.from('#0*'))
  await w.flush()
}

if (b.core.length === 0) {
  const w = b.write()

  for (let i = 0; i < 1000; i++) {
    w.tryPut(Buffer.from('#' + i), Buffer.from('#' + i))
  }
  // w.tryPut(Buffer.from('a'), Buffer.from('a'))
  // w.tryPut(Buffer.from('b'), Buffer.from('b'))
  // w.tryPut(Buffer.from('c'), Buffer.from('c'))
  // w.tryPut(Buffer.from('d'), Buffer.from('d'))
  // w.tryDelete(Buffer.from('#16'))

  // for (let i = n; i < n + 50_000; i++) {
  //   const k = '#' + i.toString().padStart(7, '0')
  //   w.tryPut(Buffer.from(k), Buffer.from('' + i))
  // }

  console.time()
  await w.flush()
  console.timeEnd()
}

console.log('pre get')
console.log(await b.get(Buffer.from('#0')))
console.log('post get')

// console.log(b.core.length)
// console.log(b.root)
// // console.log(await b.get(Buffer.from('a')))

// // console.log((await b.peek({ reverse: true })).key.toString())
// // console.log(b.root)
// // console.time()
let m = 0
console.log('pre createReadStream')
for await (const data of b.createReadStream()) {
//   // console.log(await b.get(Buffer.from('#' + (m++).toString().padStart(7, '0'))))
  console.log(++m, '-->', data.key.toString())
}
// console.timeEnd()

// for (let i = 0; i < 100; i++) {
//   console.log(await b.get(Buffer.from('#' + i)))
// }
// console.log(b.core.length)

// for await (const data of b.createReadStream({ gte: Buffer.from('#33'), lte: Buffer.from('#49') })) {
//   console.log(data.key.toString())
// }
