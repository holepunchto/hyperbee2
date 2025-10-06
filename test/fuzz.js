const test = require('brittle')
const b4a = require('b4a')
const { create } = require('./helpers')

test('fuzz regression #1', async function (t) {
  const db = await create(t)

  const keys = [
    '1091',
    '1155',
    '1192',
    '1627',
    '1700',
    '1738',
    '1778',
    '2040',
    '2284',
    '2295',
    '2376',
    '2464',
    '2554',
    '2601',
    '2609',
    '2994',
    '3316',
    '3334',
    '3625',
    '4012',
    '402',
    '4022',
    '4048',
    '4234',
    '427',
    '4297',
    '4456',
    '4480',
    '460',
    '4609',
    '5376',
    '5523',
    '5586',
    '5725',
    '5845',
    '605',
    '6294',
    '6320',
    '6506',
    '6531',
    '6629',
    '6929',
    '7037',
    '7501',
    '7554',
    '7742',
    '8196',
    '8323',
    '8542',
    '8677',
    '8753',
    '8804',
    '8811',
    '8826',
    '8895',
    '8935',
    '9068',
    '9422',
    '9513',
    '967',
    '9866',
    '9889'
  ].map((x) => b4a.from(x))

  const expected = [
    '4609',
    '5376',
    '5523',
    '5586',
    '5725',
    '5845',
    '605',
    '6294',
    '6320',
    '6506',
    '6531'
  ]

  const w = db.write()
  for (const k of keys) w.tryPut(k, k)
  await w.flush()

  const query = {
    gte: null,
    gt: b4a.from('460'),
    lte: null,
    lt: b4a.from('6629'),
    reverse: false
  }

  const actual = []

  for await (const data of db.createReadStream(query)) {
    actual.push(b4a.toString(data.key))
  }

  t.alike(actual, expected)
})

test('fuzz regression #2', async function (t) {
  const db = await create(t)

  const keys = [
    '1493',
    '1581',
    '1602',
    '2',
    '2453',
    '2610',
    '2685',
    '2738',
    '2744',
    '2998',
    '3290',
    '3593',
    '3710',
    '3786',
    '3879',
    '3890',
    '3914',
    '3971',
    '4425',
    '4453',
    '4577',
    '460',
    '4728',
    '49',
    '5011',
    '502',
    '5329',
    '5394',
    '5987',
    '6233',
    '6525',
    '6817',
    '6851',
    '7059',
    '7426',
    '7493',
    '7663',
    '7670',
    '7742',
    '806',
    '8203',
    '8207',
    '8413',
    '8640',
    '9201',
    '933',
    '9330',
    '9365',
    '9745',
    '9942'
  ].map((x) => b4a.from(x))

  const expected = ['2685', '2610', '2453', '2', '1602', '1581', '1493']

  const w = db.write()
  for (const k of keys) w.tryPut(k, k)
  await w.flush()

  const query = {
    gte: null,
    gt: null,
    lte: b4a.from('2685'),
    lt: null,
    reverse: true
  }

  const actual = []

  for await (const data of db.createReadStream(query)) {
    actual.push(b4a.toString(data.key))
  }

  t.alike(actual, expected)
})

test('fuzz regression #3', async function (t) {
  const db = await create(t)

  const keys = [
    '1355',
    '1414',
    '1634',
    '1673',
    '1782',
    '1857',
    '2023',
    '2189',
    '2192',
    '2481',
    '2582',
    '2705',
    '2732',
    '2985',
    '313',
    '3300',
    '3514',
    '3532',
    '3611',
    '3650',
    '3769',
    '3881',
    '3964',
    '4048',
    '408',
    '4127',
    '4281',
    '4472',
    '4757',
    '484',
    '5095',
    '5332',
    '5483',
    '5547',
    '5859',
    '5877',
    '599',
    '6138',
    '6271',
    '6464',
    '6732',
    '6812',
    '6897',
    '6943',
    '703',
    '723',
    '7335',
    '7454',
    '7459',
    '7534',
    '772',
    '7886',
    '8184',
    '8747',
    '8762',
    '8782',
    '8924',
    '904',
    '9542',
    '9647',
    '9677',
    '9770',
    '9790',
    '9881',
    '9922',
    '9998'
  ].map((x) => b4a.from(x))

  const expected = [
    '408',
    '4127',
    '4281',
    '4472',
    '4757',
    '484',
    '5095',
    '5332',
    '5483',
    '5547',
    '5859',
    '5877',
    '599',
    '6138',
    '6271',
    '6464',
    '6732',
    '6812',
    '6897',
    '6943',
    '703',
    '723',
    '7335',
    '7454',
    '7459',
    '7534',
    '772',
    '7886',
    '8184',
    '8747',
    '8762',
    '8782',
    '8924',
    '904',
    '9542',
    '9647',
    '9677',
    '9770',
    '9790',
    '9881',
    '9922',
    '9998'
  ]

  const w = db.write()
  for (const k of keys) w.tryPut(k, k)
  await w.flush()

  const query = {
    gte: b4a.from('408'),
    gt: null,
    lte: null,
    lt: null,
    reverse: false
  }

  const actual = []

  for await (const data of db.createReadStream(query)) {
    actual.push(b4a.toString(data.key))
  }

  t.alike(actual, expected)
})

test('fuzz regression #n', async function (t) {
  t.pass('left as a template for next fuzz test')
})

test('random fuzz (2k rounds)', async function (t) {
  const db = await create(t)

  const expected = new Map()

  for (let i = 0; i < 2000; i++) {
    const n = (Math.random() * 10) | 0
    const w = db.write()
    for (let j = 0; j < n; j++) {
      const put = Math.random() < 0.8
      const k = b4a.from('' + ((Math.random() * 10_000) | 0))
      if (put) {
        expected.set(k.toString(), k)
        w.tryPut(k, k)
      } else {
        expected.delete(k.toString())
        w.tryDelete(k)
      }
    }
    await w.flush()
  }

  const sorted = [...expected.values()].sort(b4a.compare)
  const actual = []

  for await (const data of db.createReadStream()) {
    actual.push(data.key)
  }

  if (!alike(actual, sorted)) return

  for (let i = 0; i < 10; i++) {
    const start = sorted[(Math.random() * sorted.length) | 0]
    const end = sorted[(Math.random() * sorted.length) | 0]
    const opts = {}

    if (Math.random() < 0.3) opts.gte = start
    else if (Math.random() < 0.3) opts.gt = start

    if (Math.random() < 0.3) opts.lte = end
    else if (Math.random() < 0.3) opts.lt = end

    if (Math.random() < 0.5) opts.reverse = true

    const actual = []

    for await (const data of db.createReadStream(opts)) {
      actual.push(data.key)
    }

    const expected = []
    for (const s of sorted) {
      const a = b4a.compare(start, s)
      const b = b4a.compare(s, end)

      if (
        (opts.gte ? a <= 0 : opts.gt ? a < 0 : true) &&
        (opts.lte ? b <= 0 : opts.lt ? b < 0 : true)
      ) {
        expected.push(s)
      }
    }

    if (opts.reverse) expected.reverse()

    if (!alike(actual, expected)) return
  }

  function alike(actual, expected, opts) {
    t.alike(actual, expected)

    if (b4a.equals(b4a.concat(expected), b4a.concat(actual))) return

    let s = 'const db = await create(t)\n\n'

    s += 'const keys = ' + format(sorted.map((x) => b4a.toString(x))) + '.map(x => b4a.from(x))\n\n'
    s += 'const expected = ' + format(expected.map((x) => b4a.toString(x))) + '\n'

    s += '\n'
    s += 'const w = db.write()\n'
    s += 'for (const k of keys) w.tryPut(k, k)\n'
    s += 'await w.flush()\n'
    s += '\n'
    s += 'const query = {\n'
    s += '  gte: ' + (opts.gte ? "b4a.from('" + b4a.toString(opts.gte) + "')" : 'null') + ',\n'
    s += '  gt: ' + (opts.gt ? "b4a.from('" + b4a.toString(opts.gt) + "')" : 'null') + ',\n'
    s += '  lte: ' + (opts.lte ? "b4a.from('" + b4a.toString(opts.lte) + "')" : 'null') + ',\n'
    s += '  lt: ' + (opts.lt ? "b4a.from('" + b4a.toString(opts.lt) + "')" : 'null') + ',\n'
    s += '  reverse: ' + !!opts.reverse + '\n'
    s += '}\n\n'

    s += 'const actual = []\n\n'
    s += 'for await (const data of db.createReadStream(query)) {\n'
    s += '  actual.push(b4a.toString(data.key))\n'
    s += '}\n\n'

    s += 't.alike(actual, expected)\n'
    console.log(s)
  }
})

function format(list) {
  let s = '[\n'
  for (let i = 0; i < list.length; i += 8) {
    let l = ' '
    for (let j = i; j < Math.min(list.length, i + 8); j++) {
      l += " '" + list[j] + "',"
    }
    l += '\n'
    s += l
  }
  return s.replace(/,\n$/m, '\n') + ']'
}
