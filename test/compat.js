const test = require('brittle')
const b4a = require('b4a')
const { create } = require('./helpers')

test('basic hyperbee1', async function (t) {
  const compatBatch = [
    b4a.from('0a086879706572626565', 'hex'),
    b4a.from('0a050a030a0101120568656c6c6f1a05776f726c64', 'hex'),
    b4a.from('0a060a040a020201120368656a1a0676657264656e', 'hex'),
    b4a.from('0a070a050a030201031204686f6c611a056d756e646f', 'hex')
  ]

  const db = await create(t)

  await db.core.append(compatBatch)

  const snap = db.checkout({ length: db.core.length })
  const expected = [
    ['hej', 'verden'],
    ['hello', 'world'],
    ['hola', 'mundo']
  ]

  for await (const data of snap.createReadStream()) {
    t.alike([b4a.toString(data.key), b4a.toString(data.value)], expected.shift())
  }

  t.is(expected.length, 0)

  await snap.close()
})

test('bigger hyperbee1', async function (t) {
  const compatBatch = [
    b4a.from('0a086879706572626565', 'hex'),
    b4a.from('0a050a030a0101120223301a022330', 'hex'),
    b4a.from('0a060a040a020102120223311a022331', 'hex'),
    b4a.from('0a070a050a03010203120223321a022332', 'hex'),
    b4a.from('0a080a060a0401020304120223331a022333', 'hex'),
    b4a.from('0a090a070a050102030405120223341a022334', 'hex'),
    b4a.from('0a0a0a080a06010203040506120223351a022335', 'hex'),
    b4a.from('0a0b0a090a0701020304050607120223361a022336', 'hex'),
    b4a.from('0a0c0a0a0a080102030405060708120223371a022337', 'hex'),
    b4a.from('0a1b0a090a01051204090109020a060a04010203040a060a0406070809120223381a022338', 'hex'),
    b4a.from('0a140a090a0105120409010a010a070a05060708090a120223391a022339', 'hex'),
    b4a.from('0a140a090a010512040b010a010a070a0501020b030412032331301a03233130', 'hex'),
    b4a.from('0a150a090a010512040c010a010a080a0601020b0c030412032331311a03233131', 'hex'),
    b4a.from('0a160a090a010512040d010a010a090a0701020b0c0d030412032331321a03233132', 'hex'),
    b4a.from('0a170a090a010512040e010a010a0a0a0801020b0c0d0e030412032331331a03233133', 'hex'),
    b4a.from(
      '0a1e0a0c0a020d0512060f010f020a010a060a0401020b0c0a060a040e0f030412032331341a03233134',
      'hex'
    ),
    b4a.from('0a170a0c0a020d0512060f0110010a010a070a050e0f10030412032331351a03233135', 'hex'),
    b4a.from('0a180a0c0a020d0512060f0111010a010a080a060e0f1011030412032331361a03233136', 'hex'),
    b4a.from('0a190a0c0a020d0512060f0112010a010a090a070e0f101112030412032331371a03233137', 'hex'),
    b4a.from('0a1a0a0c0a020d0512060f0113010a010a0a0a080e0f10111213030412032331381a03233138', 'hex'),
    b4a.from(
      '0a210a0f0a030d120512080f01140114020a010a060a040e0f10110a060a041314030412032331391a03233139',
      'hex'
    )
  ]

  const db = await create(t, { t: 5 })

  await db.core.append(compatBatch)

  const snap = db.checkout({ length: db.core.length })
  const expected = []

  for (let i = 0; i < 20; i++) {
    expected.push(['#' + i, '#' + i])
  }

  expected.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))

  for await (const data of snap.createReadStream()) {
    t.alike([b4a.toString(data.key), b4a.toString(data.value)], expected.shift())
  }

  t.is(expected.length, 0)

  await snap.close()
})

test('encode hyperbee1', async function (t) {
  const db = await create(t, { t: 5 })

  const w = db.write({ compat: true })

  const expected = []

  for (let i = 0; i < 4; i++) {
    w.tryPut(b4a.from('#' + i), b4a.from('#' + i))
    expected.push(['#' + i, '#' + i])
  }

  expected.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))

  await w.flush()

  for await (const data of db.createReadStream()) {
    t.alike([b4a.toString(data.key), b4a.toString(data.value)], expected.shift())
  }

  t.is(expected.length, 0)
})

test('encode bigger hyperbee1', async function (t) {
  const db = await create(t, { t: 5 })

  const w = db.write({ compat: true })

  const expected = []

  for (let i = 0; i < 20; i++) {
    w.tryPut(b4a.from('#' + i), b4a.from('#' + i))
    expected.push(['#' + i, '#' + i])
  }

  expected.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))

  await w.flush()

  db.cache.empty()

  for await (const data of db.createReadStream()) {
    t.alike([b4a.toString(data.key), b4a.toString(data.value)], expected.shift())
  }

  t.is(expected.length, 0)
})

test('basic block 0', async function (t) {
  const block0Batch = [
    b4a.from('000000000601010000000001022330022330', 'hex'),
    b4a.from('0000000007000001020000000001000001022331022331', 'hex'),
    b4a.from('0000000007000101030000000001000002000001022332022332', 'hex'),
    b4a.from('0000000007000201040000000001000002000003000001022333022333', 'hex')
  ]

  const db = await create(t, { t: 5 })

  await db.core.append(block0Batch)

  const snap = db.checkout({ length: db.core.length })
  const expected = []

  for (let i = 0; i < 4; i++) {
    expected.push(['#' + i, '#' + i])
  }

  expected.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))

  for await (const data of snap.createReadStream()) {
    t.alike([b4a.toString(data.key), b4a.toString(data.value)], expected.shift())
  }

  t.is(expected.length, 0)

  await snap.close()
})

test('bigger block 0', async function (t) {
  const block0Batch = [
    b4a.from('000000000601010000000001022330022330', 'hex'),
    b4a.from('0000000007000001020000000001000001022331022331', 'hex'),
    b4a.from('0000000007000101030000000001000002000001022332022332', 'hex'),
    b4a.from('0000000007000201040000000001000002000003000001022333022333', 'hex'),
    b4a.from('0000000007000301050000000001000002000003000004000001022334022334', 'hex'),
    b4a.from('0000000007000401060000000001000002000003000004000005000001022335022335', 'hex'),
    b4a.from('0000000007000501070000000001000002000003000004000005000006000001022336022336', 'hex'),
    b4a.from(
      '0000000007000601080000000001000002000003000004000005000006000007000001022337022337',
      'hex'
    ),
    b4a.from('000000010601040005000006000007000008000001022338022338', 'hex'),
    b4a.from('000001000300070201000400020009010008000400000000010000020000030000', 'hex'),
    b4a.from(
      '00000000070009020100040002000901000a0105000500000600000700000800000a000001022339022339',
      'hex'
    ),
    b4a.from(
      '0000000007000a020100040002000b01000a0105000000000100000b0000020000030000010323313003233130',
      'hex'
    ),
    b4a.from(
      '0000000007000b020100040002000c01000a0106000000000100000b00000c0000020000030000010323313103233131',
      'hex'
    ),
    b4a.from(
      '0000000007000c020100040002000d01000a0107000000000100000b00000c00000d0000020000030000010323313203233132',
      'hex'
    ),
    b4a.from(
      '0000000007000d020100040002000e01000a0108000000000100000b00000c00000d00000e0000020000030000010323313303233133',
      'hex'
    ),
    b4a.from('00000001060104000e00000f0000020000030000010323313403233134', 'hex'),
    b4a.from(
      '0000010007000e020200100000040003001001000f00000a0104000000000100000b00000c0000010323313203233132',
      'hex'
    ),
    b4a.from(
      '00000000070010020200100000040003001001001101000a0105000e00000f0000110000020000030000010323313503233135',
      'hex'
    ),
    b4a.from(
      '00000000070011020200100000040003001001001201000a0106000e00000f0000110000120000020000030000010323313603233136',
      'hex'
    ),
    b4a.from(
      '00000000070012020200100000040003001001001301000a0107000e00000f0000110000120000130000020000030000010323313703233137',
      'hex'
    ),
    b4a.from(
      '00000000070013020200100000040003001001001401000a0108000e00000f0000110000120000130000140000020000030000010323313803233138',
      'hex'
    ),
    b4a.from('0000000106010400140000150000020000030000010323313903233139', 'hex'),
    b4a.from(
      '00000100070014020300100000160000040004001001001601001500000a0104000e00000f0000110000120000010323313703233137',
      'hex'
    )
  ]

  const db = await create(t, { t: 5 })

  await db.core.append(block0Batch)

  const snap = db.checkout({ length: db.core.length })
  const expected = []

  for (let i = 0; i < 20; i++) {
    expected.push(['#' + i, '#' + i])
  }

  expected.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))

  for await (const data of snap.createReadStream()) {
    t.alike([b4a.toString(data.key), b4a.toString(data.value)], expected.shift())
  }

  t.is(expected.length, 0)

  await snap.close()
})

test('encode block0', async function (t) {
  const db = await create(t, { t: 5 })

  const w = db.write({ type: 0 })

  const expected = []

  for (let i = 0; i < 4; i++) {
    w.tryPut(b4a.from('#' + i), b4a.from('#' + i))
    expected.push(['#' + i, '#' + i])
  }

  expected.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))

  await w.flush()

  for await (const data of db.createReadStream()) {
    t.alike([b4a.toString(data.key), b4a.toString(data.value)], expected.shift())
  }

  t.is(expected.length, 0)
})

test('encode bigger block0', async function (t) {
  const db = await create(t, { t: 5 })

  const w = db.write({ type: 0 })

  const expected = []

  for (let i = 0; i < 20; i++) {
    w.tryPut(b4a.from('#' + i), b4a.from('#' + i))
    expected.push(['#' + i, '#' + i])
  }

  expected.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))

  await w.flush()

  db.cache.empty()

  for await (const data of db.createReadStream()) {
    t.alike([b4a.toString(data.key), b4a.toString(data.value)], expected.shift())
  }

  t.is(expected.length, 0)
})
