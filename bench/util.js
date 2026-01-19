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

function _pick1KUnit(units, value) {
  let m = 0
  while (m < units.length - 1 && value > 1000) {
    value = value / 1000
    m++
  }
  return value.toPrecision(3) + units[m]
}

function humanizeCount(count) {
  return _pick1KUnit(['', 'K', 'M'], count)
}

function formatNanoseconds(value) {
  return _pick1KUnit(['ns', 'µs', 'ms', 's'], value)
}

async function clearSandbox(path) {
  await rm(path, { recursive: true })
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

module.exports = { shuffle, humanizeCount, formatNanoseconds, clearSandbox, compareResults }
