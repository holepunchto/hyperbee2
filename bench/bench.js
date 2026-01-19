const { createHistogram, timerify, PerformanceObserver } = require('perf_hooks')
const { formatNanoseconds } = require('./util.js')

function noop() {}

// Options:
//   name: string, name of benchmark
//   cycle: function to time, receives return value of setup() as only argument
//
//   [setup]: function to run before each cycle (not timed), receives one argument the input data for the current variation
//   [teardown]: function to run after each cycle (not timed), receives return value of setup() as only argument
//   [count]: number of times to run the cycle function (after warmup)
//   [warmup]: number of times to run cycle function before timing starts
//   [variations]: object mapping names to input data
async function bench(options) {
  const {
    name,
    cycle,
    count = 10,
    warmup = 1,
    setup = noop,
    teardown = noop,
    variations = { '': null }
  } = options

  console.log(name)
  const results = {}
  const histograms = {}

  for (const v of Object.keys(variations)) {
    const input = variations[v]

    // Warmup
    for (let i = 0; i < warmup; i++) {
      const context = await setup(input)
      await cycle(context)
      await teardown(context)
    }

    // Time cycles
    const histogram = createHistogram()
    const wrapped = timerify(options.cycle, { histogram })

    const observer = new PerformanceObserver(() => {})
    observer.observe({ entryTypes: ['function'] })

    for (let i = 0; i < count; i++) {
      const context = await setup(input)
      await wrapped(context)
      await teardown(context)
    }

    results[v] = {
      cycles: histogram.count,
      mean: formatNanoseconds(histogram.mean),
      min: formatNanoseconds(histogram.min),
      max: formatNanoseconds(histogram.max),
      stddev: formatNanoseconds(histogram.stddev)
    }
    histograms[v] = histogram
    observer.disconnect()
  }

  console.table(results)
  console.log()

  return histograms
}

module.exports = { bench }
