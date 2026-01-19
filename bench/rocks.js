const RocksDB = require('rocksdb-native')

async function openRocksDB(path) {
  const rocks = new RocksDB(path, { createIfMissing: true })
  // Get an approximation of corestore's rocksdb use
  // See: https://github.com/holepunchto/hypercore-storage/blob/4e8262299837a588a58c160ca943350a4c41bf97/index.js#L1051
  const col = new RocksDB.ColumnFamily('not-corestore', {
    enableBlobFiles: true,
    minBlobSize: 4096,
    blobFileSize: 256 * 1024 * 1024,
    enableBlobGarbageCollection: true,
    tableBlockSize: 8192,
    tableCacheIndexAndFilterBlocks: true,
    tableFormatVersion: 6,
    optimizeFiltersForMemory: false,
    blockCache: true
  })
  const db = rocks.columnFamily(col)
  await db.ready()
  return { rocks, db }
}

module.exports = { openRocksDB }
