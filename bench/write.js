// Write performance benchmarks
const Hyperbee = require('../index.js')
const Corestore = require('corestore')
const { randomBytes } = require('crypto')
const { bench } = require('./bench.js')
const { resolve } = require('path')
const { shuffle, clearSandbox, compareResults } = require('./util.js')
const { openRocksDB } = require('./rocks.js')

const PATH = resolve(__dirname, './sandbox/write')

function makeVariations(count, item) {
  const ascending = []
  for (let i = 0; i < count; i++) {
    ascending[i] = item(i)
  }

  const random = ascending.slice()
  shuffle(random)

  const descending = ascending.slice()
  descending.reverse()

  return { ascending, descending, random }
}

async function run() {
  const rocks_small_kv_large_batch = await bench({
    name: `RocksDB: insert 100K small keys and values in one large batch`,
    count: 10,
    variations: makeVariations(100_000, (i) => [Buffer.from('#' + i), Buffer.from('#' + i)]),
    async setup(source) {
      await clearSandbox(PATH)
      const { rocks, db } = await openRocksDB(PATH)
      return { rocks, db, source }
    },
    async cycle({ db, source }) {
      const w = db.write()
      for (const [k, v] of source) {
        w.put(k, v)
      }
      await w.flush()
      w.destroy()
    },
    async teardown({ rocks }) {
      await rocks.close()
    }
  })

  const hb2_small_kv_large_batch = await bench({
    name: `Hyperbee2: insert 100K small keys and values in one large batch`,
    count: 10,
    variations: makeVariations(100_000, (i) => [Buffer.from('#' + i), Buffer.from('#' + i)]),
    async setup(source) {
      await clearSandbox(PATH)
      const b = new Hyperbee(new Corestore(PATH))
      await b.ready()
      return { b, source }
    },
    async cycle({ b, source }) {
      const w = b.write()
      for (const [k, v] of source) {
        w.tryPut(k, v)
      }
      await w.flush()
    },
    async teardown({ b }) {
      await b.close()
    }
  })

  compareResults(rocks_small_kv_large_batch, hb2_small_kv_large_batch)

  const rocks_small_kv_small_batch = await bench({
    name: `RocksDB: insert 10K small keys and values each in a separate batch`,
    count: 10,
    variations: makeVariations(10_000, (i) => [Buffer.from('#' + i), Buffer.from('#' + i)]),
    async setup(source) {
      await clearSandbox(PATH)
      const { rocks, db } = await openRocksDB(PATH)
      return { rocks, db, source }
    },
    async cycle({ db, source }) {
      for (const [k, v] of source) {
        const w = db.write()
        w.put(k, v)
        await w.flush()
        w.destroy()
      }
    },
    async teardown({ rocks }) {
      await rocks.close()
    }
  })

  const hb2_small_kv_small_batch = await bench({
    name: `Hyperbee2: insert 10K small keys and values each in a separate batch`,
    count: 10,
    variations: makeVariations(10_000, (i) => [Buffer.from('#' + i), Buffer.from('#' + i)]),
    async setup(source) {
      await clearSandbox(PATH)
      const b = new Hyperbee(new Corestore(PATH))
      await b.ready()
      return { b, source }
    },
    async cycle({ b, source }) {
      for (const [k, v] of source) {
        const w = b.write()
        w.tryPut(k, v)
        await w.flush()
      }
    },
    async teardown({ b }) {
      await b.close()
    }
  })

  compareResults(rocks_small_kv_small_batch, hb2_small_kv_small_batch)

  const random_large_kv = []
  for (let i = 0; i < 100; i++) {
    random_large_kv.push([randomBytes(1024 * 1024), randomBytes(1024 * 1024)])
  }

  const rocks_large_kv_small_batch = await bench({
    name: `RocksDB: insert 100 large (1MB) keys and values each in a separate batch`,
    count: 10,
    variations: { random: random_large_kv },
    async setup(source) {
      await clearSandbox(PATH)
      const { rocks, db } = await openRocksDB(PATH)
      return { rocks, db, source }
    },
    async cycle({ db, source }) {
      for (const [k, v] of source) {
        const w = db.write()
        w.put(k, v)
        await w.flush()
        w.destroy()
      }
    },
    async teardown({ rocks }) {
      await rocks.close()
    }
  })

  const hb2_large_kv_small_batch = await bench({
    name: `Hyperbee2: insert 100 large (1MB) keys and values each in a separate batch`,
    count: 10,
    variations: { random: random_large_kv },
    async setup(source) {
      await clearSandbox(PATH)
      const b = new Hyperbee(new Corestore(PATH))
      await b.ready()
      return { b, source }
    },
    async cycle({ b, source }) {
      for (const [k, v] of source) {
        const w = b.write()
        w.tryPut(k, v)
        await w.flush()
      }
    },
    async teardown({ b }) {
      await b.close()
    }
  })

  compareResults(rocks_large_kv_small_batch, hb2_large_kv_small_batch)
}

run()
