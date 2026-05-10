import Hyperbee from '../index.js'
import Corestore from 'corestore'
import path from 'node:path'

const total = (values, rereduce) => {
  let total = 0
  for (const v of values) {
    if (rereduce) {
      total += v
    } else {
      total += Number(v.value.toString())
    }
  }
  return total
}

const reducers = { total }

const storePath = path.resolve(import.meta.dirname, '../sandbox/total-reducer')
const b = new Hyperbee(new Corestore(storePath))

await b.ready()

if (b.core.length === 0) {
  console.log('initial write, no materialized view')
  const w = b.write()

  for (let i = 0; i < 1_000_000; i++) {
    w.tryPut(Buffer.from('' + i), Buffer.from('' + i))
  }

  await w.flush()
} else {
  console.log('add one more entry and materialize view')

  const w = b.write()

  w.tryPut(Buffer.from('500000' + Math.random()), Buffer.from('500000' + Math.random()))

  await w.flush(reducers)
}

async function timeIt(f) {
  const t = performance.now()
  console.log(await f())
  console.log('Elapsed:', (performance.now() - t).toFixed(3), 'ms')
}

console.log('Time query of total reducer')
await timeIt(async () => await b.reduce('total', total))

console.log('Time query of total reducer over range')
await timeIt(
  async () => await b.reduceRange('total', total, Buffer.from('250000'), Buffer.from('750000'))
)

console.log('Time query of temporary reducer')
await timeIt(async () => await b.reduce(null, total))
