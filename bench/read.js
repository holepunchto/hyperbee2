// Read performance benchmarks
const Hyperbee = require('../index.js')
const Corestore = require('corestore')
const { randomBytes } = require('crypto')
const { bench } = require('./bench.js')
const { resolve } = require('path')
const { clearSandbox } = require('./util.js')
const { openRocksDB } = require('./rocks.js')

const ROCKSDB_PATH = resolve(__dirname, './sandbox/read/rocksdb')
const HB2_PATH = resolve(__dirname, './sandbox/read/hb2')

const random = []
for (let i = 0; i < 100_000; i++) {
  random.push([randomBytes(1024), randomBytes(1024)])
}

const ascending = random.slice()
ascending.sort((a, b) => Buffer.compare(a[0], b[0]))

const descending = ascending.slice()
descending.reverse()

async function populateRocksDB() {
  await clearSandbox(ROCKSDB_PATH)
  const { rocks, db } = await openRocksDB(ROCKSDB_PATH)

  // Write all items to (random order)
  const w = db.write()
  for (const [k, v] of random) {
    w.put(k, v)
  }
  await w.flush()
  w.destroy()

  return { rocks, db }
}

async function populateHyperbee2() {
  await clearSandbox(HB2_PATH)
  const b = new Hyperbee(new Corestore(HB2_PATH))
  await b.ready()

  // Write all items to (random order)
  const w = b.write()
  for (const [k, v] of random) {
    w.tryPut(k, v)
  }
  await w.flush()

  return b
}

async function run() {
  const { rocks, db } = await populateRocksDB()
  const b = await populateHyperbee2()

  await bench({
    name: `RocksDB: get 100K keys`,
    variations: { ascending, descending, random },
    count: 10,
    setup(source) {
      return source
    },
    async cycle(source) {
      const r = db.read()
      for (const [k, _v] of source) {
        const p = r.get(k)
        r.flush()
        await p
      }
    }
  })

  await bench({
    name: `Hyperbee2: get 100K keys`,
    variations: { ascending, descending, random },
    count: 10,
    setup(source) {
      return source
    },
    async cycle(source) {
      for (const [k, _v] of source) {
        const p = b.get(k)
        await p
      }
    }
  })

  await bench({
    name: `RocksDB: iterate over 100K keys`,
    count: 10,
    async cycle() {
      for await (const data of db.iterator()) {
        const _ = data.key
      }
    }
  })

  await bench({
    name: `Hyperbee2: iterate over 100K keys`,
    count: 10,
    async cycle() {
      for await (const data of b.createReadStream()) {
        const _ = data.key
      }
    }
  })

  await rocks.close()
  await b.close()
}

run()
