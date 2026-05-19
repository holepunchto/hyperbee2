import Hyperbee from '../index.js'
import Corestore from 'corestore'
import path from 'node:path'

const count = (values, rereduce) => {
  if (rereduce) {
    let total = 0
    for (const v of values) total += v
    return total
  } else {
    return values.length
  }
}

const reducers = { count }

const storePath = path.resolve(import.meta.dirname, '../sandbox/count-reducer')
const b = new Hyperbee(new Corestore(storePath))

await b.ready()

if (b.core.length === 0) {
  console.log('initial write, no materialized view')
  const w = b.write()

  for (let i = 0; i < 1_000_000; i++) {
    w.tryPut(Buffer.from('#' + i), Buffer.from('#' + i))
  }

  await w.flush()
} else {
  console.log('add one more entry and materialize view')

  const w = b.write()

  w.tryPut(Buffer.from('#500000' + Math.random()), Buffer.from('#500000' + Math.random()))

  await w.flush(reducers)
}

async function timeIt(f) {
  const t = performance.now()
  console.log(await f())
  console.log('Elapsed:', (performance.now() - t).toFixed(3), 'ms')
}

console.log('Time query of count reducer')
await timeIt(async () => await b.reduce('count', count))

console.log('Time query of count reducer over range')
await timeIt(
  async () => await b.reduceRange('count', count, Buffer.from('#250_000'), Buffer.from('#750_000'))
)

console.log('Time query of temporary reducer')
await timeIt(async () => await b.reduce(null, count))
