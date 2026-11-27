#!/usr/bin/env node

// Import benchmark: the chunked `tryPut` importer (the strategy used before the
// packed loader) against the production `destination.import()` path.
//
//   node test/import/bench.js [--impl batched,import] [--runs 5] [--cases name,name] [--json]
//
// Seeding the source and verifying the destination are not part of the measured
// import time. Every measured run uses a fresh source view and a fresh
// destination in its own temporary corestore.

const b4a = require('b4a')
const crypto = require('crypto')
const os = require('os')
const path = require('path')
const { mkdtemp, rm } = require('fs/promises')
const Corestore = require('corestore')
const Bee = require('../..')

const MAX_ENTRIES = 1024
const MAX_BYTES = 1024 * 1024

const CASES = {
  'sorted-40k': {
    count: 40000,
    key: sortedKey,
    value: oneByteValue
  },
  'sorted-200k': {
    count: 200000,
    key: sortedKey,
    value: oneByteValue
  },
  'mixed-20k': {
    count: 20000,
    key: mixedKey,
    value: mixedValue
  },
  'wide-keys-400': {
    count: 400,
    key: wideKey,
    value: () => null,
    seedEntries: 50
  }
}

function sortedKey(i) {
  const key = b4a.alloc(4)
  key.writeUInt32BE(i, 0)
  return key
}

function oneByteValue(i) {
  return b4a.from([i & 0xff])
}

// Ascending 64 KiB keys with null values. A node holding a full 2t-1 of these
// would exceed the hypercore block limit, so the writer has to bound node blocks
// by the actual key bytes rather than only by the degree.
function wideKey(i) {
  const key = b4a.alloc(64 * 1024)
  key.writeUInt32BE(i, 0)
  return key
}

// The four byte counter prefix keeps keys strictly ascending; the suffix only
// varies their sizes.
function mixedKey(i) {
  const suffix = i % 5 === 0 ? i % 23 : 0
  const key = b4a.alloc(4 + suffix, i & 0xff)
  key.writeUInt32BE(i, 0)
  return key
}

// Includes null, empty, inline, external (above the 1 KiB inline limit) and
// oversized (above the preferred 4 KiB block size) values.
function mixedValue(i) {
  if (i === 12345) return b4a.alloc(2 * 1024 * 1024 + 1, i & 0xff)
  if (i % 4093 === 0) return b4a.alloc(65536 + (i % 1024), i & 0xff)
  if (i % 19 === 0) return null
  if (i % 23 === 0) return b4a.alloc(0)

  switch (i % 8) {
    case 0:
      return b4a.alloc((i % 31) + 1, i & 0xff)
    case 1:
      return b4a.alloc((i % 700) + 1, i & 0xff)
    case 2:
      return b4a.alloc((i % 1024) + 1, i & 0xff)
    case 3:
      return b4a.alloc((i % 4096) + 1, i & 0xff)
    case 4:
      return b4a.alloc((i % 200) + 1, i & 0xff)
    case 5:
      return b4a.alloc((i % 32) + 1, i & 0xff)
    case 6:
      return b4a.alloc((i % 2048) + 1, i & 0xff)
    default:
      return b4a.alloc((i % 64) + 1, i & 0xff)
  }
}

async function main() {
  const impls = option('impl', 'batched,import').split(',')
  const runs = Number(option('runs', 5))
  const filter = option('cases', '')
  const json = process.argv.includes('--json')

  for (const impl of impls) {
    if (impl !== 'batched' && impl !== 'import') {
      throw new Error('Unknown --impl ' + impl + ' (expected batched or import)')
    }
  }

  const names = filter
    ? filter
        .split(',')
        .map((name) => name.trim())
        .filter(Boolean)
    : Object.keys(CASES)

  for (const name of names) {
    if (!CASES[name]) throw new Error('Unknown --case ' + name)
  }

  const results = []

  for (const name of names) {
    const spec = CASES[name]
    const sourceDir = await mkdtemp(path.join(os.tmpdir(), 'hyperbee2-bench-source-'))
    const source = await createSource(sourceDir, spec)
    const view = source.snapshot()
    const digest = await digestBee(view)

    try {
      for (const impl of impls) {
        // One discarded warm up, then the measured runs on fresh destinations.
        if (runs > 1) await measure(view, impl, digest, true)

        const samples = []
        for (let i = 0; i < runs; i++) samples.push(await measure(view, impl, digest, false))

        results.push(summarise(name, impl, samples))
      }
    } finally {
      await view.close()
      await source.close()
      await rm(sourceDir, { recursive: true, force: true })
    }
  }

  if (json) console.log(JSON.stringify(results, null, 2))
  else print(results)

  if (results.some((row) => !row.ok)) process.exitCode = 1

  async function measure(view, impl, expected, warmup) {
    const dir = await mkdtemp(path.join(os.tmpdir(), 'hyperbee2-bench-' + impl + '-'))
    const store = new Corestore(dir)
    const destination = new Bee(store)
    await destination.ready()

    const blocksBefore = destination.core.length
    const bytesBefore = destination.core.byteLength
    const before = process.memoryUsage()
    const peak = {
      heapUsed: before.heapUsed,
      external: before.external,
      arrayBuffers: before.arrayBuffers,
      rss: before.rss
    }

    const sampler = setInterval(() => {
      const used = process.memoryUsage()
      if (used.heapUsed > peak.heapUsed) peak.heapUsed = used.heapUsed
      if (used.external > peak.external) peak.external = used.external
      if (used.arrayBuffers > peak.arrayBuffers) peak.arrayBuffers = used.arrayBuffers
      if (used.rss > peak.rss) peak.rss = used.rss
    }, 2)
    sampler.unref()

    const start = process.hrtime.bigint()

    try {
      if (impl === 'batched') await importBatched(destination, view)
      else await destination.import(view)
    } finally {
      clearInterval(sampler)
      if (warmup) {
        await destination.close()
        await rm(dir, { recursive: true, force: true })
      }
    }

    if (warmup) return null

    const elapsed = Number(process.hrtime.bigint() - start) / 1e6
    const bytes = destination.core.byteLength - bytesBefore
    const blocks = destination.core.length - blocksBefore
    const shape = await measureShape(destination)
    const digest = await digestBee(destination)

    await destination.close()
    await rm(dir, { recursive: true, force: true })

    return {
      impl,
      ms: elapsed,
      bytes,
      blocks,
      peakHeap: Math.max(0, peak.heapUsed - before.heapUsed),
      peakExternal: Math.max(0, peak.external - before.external),
      peakArrayBuffers: Math.max(0, peak.arrayBuffers - before.arrayBuffers),
      peakRss: Math.max(0, peak.rss - before.rss),
      ok: digest === expected,
      ...shape
    }
  }
}

// The chunked importer used before the packed loader. It applies the real
// writer (`destination.write()` + `tryPut`) in the same 1024 entry / 1 MiB
// chunks as `lib/import.js` did before the packed path was evaluated.
async function importBatched(destination, source) {
  let batch = null
  let entries = 0
  let bytes = 0

  try {
    for await (const entry of source.createReadStream()) {
      const size = entry.key.byteLength + (entry.value === null ? 0 : entry.value.byteLength)

      if (batch !== null && (entries >= MAX_ENTRIES || bytes + size > MAX_BYTES)) {
        await flush()
      }

      if (batch === null) batch = destination.write()
      batch.tryPut(entry.key, entry.value)
      entries++
      bytes += size
    }

    if (batch !== null) await flush()
  } finally {
    if (batch !== null && !batch.closed) await batch.close()
  }

  async function flush() {
    const current = batch
    entries = 0
    bytes = 0
    await current.flush()
    if (batch === current) batch = null
  }
}

async function createSource(dir, spec) {
  const store = new Corestore(dir)
  const bee = new Bee(store)
  await bee.ready()

  for (let start = 0; start < spec.count; start += spec.seedEntries || MAX_ENTRIES) {
    const end = Math.min(spec.count, start + (spec.seedEntries || MAX_ENTRIES))
    const batch = bee.write()

    for (let i = start; i < end; i++) {
      batch.tryPut(spec.key(i), spec.value(i))
    }

    await batch.flush()
  }

  return bee
}

async function digestBee(bee) {
  const hash = crypto.createHash('sha256')
  const prefix = b4a.alloc(4)

  for await (const entry of bee.createReadStream()) {
    prefix.writeUInt32BE(entry.key.byteLength, 0)
    hash.update(prefix)
    hash.update(entry.key)

    if (entry.value === null) {
      hash.update(b4a.from([0]))
    } else {
      hash.update(b4a.from([1]))
      prefix.writeUInt32BE(entry.value.byteLength, 0)
      hash.update(prefix)
      hash.update(entry.value)
    }
  }

  return hash.digest('hex')
}

async function measureShape(bee) {
  const root = await bee.bootstrap(bee.config)
  if (root === null) {
    return { levels: 0, nodes: 0, leaves: 0, leafKeys: [] }
  }

  const leafKeys = []
  let levels = 0
  let nodes = 0
  let leaves = 0
  let frontier = [root]

  while (frontier.length) {
    const next = []

    for (const ptr of frontier) {
      const node = ptr.value ? bee.bump(ptr) : await bee.inflate(ptr, bee.config)
      nodes++

      if (node.children.length === 0) {
        leaves++
        leafKeys.push(node.keys.length)
      } else {
        for (let i = 0; i < node.children.length; i++) next.push(node.children.get(i))
      }
    }

    frontier = next
    levels++
  }

  leafKeys.sort((a, b) => a - b)

  return {
    levels,
    nodes,
    leaves,
    leafKeys: leafKeys.length ? [leafKeys[0], median(leafKeys), leafKeys[leafKeys.length - 1]] : []
  }
}

function summarise(name, impl, samples) {
  const first = samples[0]

  return {
    case: name,
    impl,
    runs: samples.length,
    ms: median(samples.map((s) => s.ms)),
    bytes: median(samples.map((s) => s.bytes)),
    blocks: median(samples.map((s) => s.blocks)),
    peakHeap: median(samples.map((s) => s.peakHeap)),
    peakExternal: median(samples.map((s) => s.peakExternal)),
    peakArrayBuffers: median(samples.map((s) => s.peakArrayBuffers)),
    peakRss: median(samples.map((s) => s.peakRss)),
    levels: first.levels,
    nodes: first.nodes,
    leaves: first.leaves,
    leafKeys: first.leafKeys,
    ok: samples.every((s) => s.ok)
  }
}

function median(values) {
  const sorted = values.slice().sort((a, b) => a - b)
  const mid = sorted.length >> 1
  return sorted.length % 2 === 1 ? sorted[mid] : Math.round((sorted[mid - 1] + sorted[mid]) / 2)
}

function print(rows) {
  const header = [
    'case'.padEnd(14),
    'impl'.padEnd(8),
    'ms'.padStart(8),
    'bytes'.padStart(10),
    'blocks'.padStart(7),
    'heap'.padStart(7),
    'ext'.padStart(7),
    'rss'.padStart(7),
    'levels'.padStart(7),
    'leaves'.padStart(7),
    'leaf keys'.padStart(11),
    'ok'.padStart(4)
  ].join('  ')

  console.log(header)
  console.log('-'.repeat(header.length))

  for (const row of rows) {
    console.log(
      [
        row.case.padEnd(14),
        row.impl.padEnd(8),
        String(Math.round(row.ms)).padStart(8),
        String(row.bytes).padStart(10),
        String(row.blocks).padStart(7),
        (row.peakHeap / 1024 / 1024).toFixed(1).padStart(7),
        (row.peakExternal / 1024 / 1024).toFixed(1).padStart(7),
        (row.peakRss / 1024 / 1024).toFixed(1).padStart(7),
        String(row.levels).padStart(7),
        String(row.leaves).padStart(7),
        row.leafKeys.join('/').padStart(11),
        row.ok ? 'ok' : 'FAIL'
      ].join('  ')
    )
  }
}

function option(name, fallback) {
  const index = process.argv.indexOf('--' + name)
  if (index === -1) return fallback
  const value = process.argv[index + 1]
  if (value === undefined || value.startsWith('--')) throw new Error('Missing value for --' + name)
  return value
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
