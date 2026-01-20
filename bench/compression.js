// Compression benchmarks
const Hyperbee = require('../index.js')
const Corestore = require('corestore')
const { randomBytes } = require('crypto')
const { resolve } = require('path')
const { clearSandbox, humanizeBytes } = require('./util.js')

const HB2_PATH = resolve(__dirname, './sandbox/read/hb2')

const SEED = 123456789 // Fixed seed for repeatable random batch sizes

// Acceptable random number generator for block sizes
function mulberry32(seed) {
  return function () {
    let t = (seed += 0x6d2b79f5)
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}
const rng = mulberry32(SEED)

async function runBenchmark({ count, keySize, valueSize, maxBlockSize = 300 }) {
  console.log(
    `Write amplification: key size ${humanizeBytes(keySize)}, value size ${humanizeBytes(valueSize)}`
  )

  // Random data of fixed sizes to insert into b-tree
  const items = []
  for (let i = 0; i < count; i++) {
    items.push([randomBytes(keySize), randomBytes(valueSize)])
  }
  // Sort items to batchs have a better chance of inserting into same block
  items.sort((a, b) => Buffer.compare(a[0], b[0]))

  // Generate batch sizes
  const batch_sizes = []
  let c = 0
  while (c < count) {
    // Non-uniform random distribution, skew to smaller batches
    // with occasional larger batches.
    const beta = Math.sin((rng() * Math.PI) / 2) ** 2
    const beta_left = beta < 0.5 ? 2 * beta : 2 * (1 - beta)
    const size = Math.round(beta_left * Math.min(maxBlockSize, count - c))
    batch_sizes.push(size)
    c += size
  }

  async function populateHyperbee2() {
    await clearSandbox(HB2_PATH)
    const b = new Hyperbee(new Corestore(HB2_PATH))
    await b.ready()

    let i = 0
    for (const size of batch_sizes) {
      const w = b.write()
      for (let j = 0; j < size; j++) {
        const [k, v] = items[i + j]
        w.tryPut(k, v)
      }
      await w.flush()
      i += size
    }

    return b
  }

  const b = await populateHyperbee2()

  const info = await b.core.info()
  const insertedBytes = count * (keySize + valueSize)

  console.table([
    {
      Items: items.length,
      Batches: batch_sizes.length,
      Hypercore: humanizeBytes(info.byteLength),
      Inserted: humanizeBytes(insertedBytes),
      Overhead: humanizeBytes(info.byteLength - insertedBytes),
      Ratio: (info.byteLength / insertedBytes).toPrecision(5)
    }
  ])

  await b.close()
}

async function run() {
  await runBenchmark({
    count: 100_000,
    keySize: 64,
    valueSize: 64
  })
  await runBenchmark({
    count: 100_000,
    keySize: 128,
    valueSize: 128
  })
  await runBenchmark({
    count: 100_000,
    keySize: 256,
    valueSize: 256
  })
  await runBenchmark({
    count: 100_000,
    keySize: 1024,
    valueSize: 1024
  })
  await runBenchmark({
    count: 1_000,
    keySize: 1024 * 1024,
    valueSize: 1024 * 1024,
    maxBlockSize: 5 // (hypercore max suggested block size is 15MB)
  })
}

run()
