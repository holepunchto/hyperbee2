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

for await (const data of b.createReadStream()) {
  console.log(data.key, '-->', data.value)
}
