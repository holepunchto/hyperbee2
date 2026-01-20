const { rm, mkdir } = require('fs/promises')

// Randomize array in-place using Durstenfeld shuffle algorithm
function shuffle(arr) {
  for (let i = arr.length - 1; i >= 0; i--) {
    const j = Math.floor(Math.random() * (i + 1))
    const tmp = arr[i]
    arr[i] = arr[j]
    arr[j] = tmp
  }
}

function _pickUnit(units, value, divisor = 1000, separator = '') {
  let m = 0
  while (m < units.length - 1 && value >= divisor) {
    value = value / divisor
    m++
  }
  return value.toPrecision(3) + separator + units[m]
}

function humanizeCount(count) {
  return _pickUnit(['', 'K', 'M'], count)
}

function humanizeBytes(bytes) {
  return _pickUnit(['B', 'KB', 'MB', 'GB'], bytes, 1024, ' ')
}

function formatNanoseconds(value) {
  return _pickUnit(['ns', 'µs', 'ms', 's'], value)
}

async function clearSandbox(path) {
  try {
    await rm(path, { recursive: true })
  } catch (e) {
    if (e.code !== 'ENOENT') throw e
  }
  await mkdir(path, { recursive: true })
}

function compareResults(rocks, hb2) {
  const rows = {}
  for (const name of Object.keys(rocks)) {
    rows[name] = {
      'RocksDB (mean)': formatNanoseconds(rocks[name].mean),
      'Hyperbee2 (mean)': formatNanoseconds(hb2[name].mean),
      overhead: Math.round((hb2[name].mean / rocks[name].mean) * 100 - 100) + '%'
    }
  }
  console.table(rows)
  console.log()
}

module.exports = {
  shuffle,
  humanizeCount,
  humanizeBytes,
  formatNanoseconds,
  clearSandbox,
  compareResults
}
