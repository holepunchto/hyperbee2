const test = require('brittle')
const b4a = require('b4a')
const { create, createMultiple } = require('./helpers')

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

test('multi writer fuzz regression', async function (t) {
  const [a, b] = await createMultiple(t, 2)

  await a.ready()
  await b.ready()

  {
    const k = b4a.from('0')
    const w = b.write({ key: a.core.key, length: a.core.length })

    w.tryPut(k, k)
    await w.flush()
  }

  {
    const k = b4a.from('13')
    const w = a.write()

    w.tryPut(k, k)
    await w.flush()
  }

  {
    const k = b4a.from('17')
    const w = a.write({ key: b.core.key, length: b.core.length })

    w.tryPut(k, k)
    await w.flush()
  }

  {
    const k = b4a.from('16')
    const w = b.write({ key: a.core.key, length: a.core.length })

    w.tryPut(k, k)
    await w.flush()
  }

  {
    const k = b4a.from('11')
    const w = b.write({ key: a.core.key, length: a.core.length })

    w.tryPut(k, k)
    await w.flush()
  }

  {
    const k = b4a.from('3')
    const w = a.write()

    w.tryPut(k, k)
    await w.flush()
  }

  {
    const k = b4a.from('19')
    const w = a.write()

    w.tryPut(k, k)
    await w.flush()
  }

  {
    const k = b4a.from('13')
    const w = a.write()

    w.tryPut(k, k)
    await w.flush()
  }

  {
    const k = b4a.from('18')
    const w = a.write()

    w.tryPut(k, k)
    await w.flush()
  }

  {
    const k = b4a.from('1')
    const w = a.write()

    w.tryPut(k, k)
    await w.flush()
  }

  {
    const k = b4a.from('15')
    const w = b.write({ key: a.core.key, length: a.core.length })

    w.tryPut(k, k)
    await w.flush()
  }

  {
    const k = b4a.from('7')
    const w = b.write({ key: a.core.key, length: a.core.length })

    w.tryPut(k, k)
    await w.flush()
  }

  {
    const k = b4a.from('16')
    const w = b.write()

    w.tryPut(k, k)
    await w.flush()
  }

  {
    const k = b4a.from('10')
    const w = a.write({ key: b.core.key, length: b.core.length })

    w.tryPut(k, k)
    await w.flush()
  }

  {
    const k = b4a.from('8')
    const w = a.write()

    w.tryPut(k, k)
    await w.flush()
  }

  t.pass('finished')
})

test('fuzz regression #4', async function (t) {
  const db = await create(t)

  const expected = new Map()

  {
    const w = db.write()
    expected.set('374', '374')
    w.tryPut(b4a.from('374'), b4a.from('374'))
    expected.set('1913', '1913')
    w.tryPut(b4a.from('1913'), b4a.from('1913'))
    expected.set('4701', '4701')
    w.tryPut(b4a.from('4701'), b4a.from('4701'))
    expected.set('5680', '5680')
    w.tryPut(b4a.from('5680'), b4a.from('5680'))
    expected.set('4840', '4840')
    w.tryPut(b4a.from('4840'), b4a.from('4840'))
    expected.delete('2609')
    w.tryDelete(b4a.from('2609'))
    expected.set('6139', '6139')
    w.tryPut(b4a.from('6139'), b4a.from('6139'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('7220', '7220')
    w.tryPut(b4a.from('7220'), b4a.from('7220'))
    expected.set('9152', '9152')
    w.tryPut(b4a.from('9152'), b4a.from('9152'))
    expected.delete('8844')
    w.tryDelete(b4a.from('8844'))
    expected.set('6625', '6625')
    w.tryPut(b4a.from('6625'), b4a.from('6625'))
    expected.set('776', '776')
    w.tryPut(b4a.from('776'), b4a.from('776'))
    expected.set('2193', '2193')
    w.tryPut(b4a.from('2193'), b4a.from('2193'))
    expected.set('7130', '7130')
    w.tryPut(b4a.from('7130'), b4a.from('7130'))
    expected.set('8932', '8932')
    w.tryPut(b4a.from('8932'), b4a.from('8932'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.delete('5175')
    w.tryDelete(b4a.from('5175'))
    expected.set('3754', '3754')
    w.tryPut(b4a.from('3754'), b4a.from('3754'))
    expected.set('5180', '5180')
    w.tryPut(b4a.from('5180'), b4a.from('5180'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('2890', '2890')
    w.tryPut(b4a.from('2890'), b4a.from('2890'))
    expected.set('1705', '1705')
    w.tryPut(b4a.from('1705'), b4a.from('1705'))
    expected.set('8173', '8173')
    w.tryPut(b4a.from('8173'), b4a.from('8173'))
    expected.set('9623', '9623')
    w.tryPut(b4a.from('9623'), b4a.from('9623'))
    expected.set('734', '734')
    w.tryPut(b4a.from('734'), b4a.from('734'))
    expected.set('1232', '1232')
    w.tryPut(b4a.from('1232'), b4a.from('1232'))
    expected.set('9248', '9248')
    w.tryPut(b4a.from('9248'), b4a.from('9248'))
    expected.set('284', '284')
    w.tryPut(b4a.from('284'), b4a.from('284'))
    expected.delete('9048')
    w.tryDelete(b4a.from('9048'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('4417', '4417')
    w.tryPut(b4a.from('4417'), b4a.from('4417'))
    expected.set('8074', '8074')
    w.tryPut(b4a.from('8074'), b4a.from('8074'))
    expected.set('129', '129')
    w.tryPut(b4a.from('129'), b4a.from('129'))
    expected.set('2861', '2861')
    w.tryPut(b4a.from('2861'), b4a.from('2861'))
    expected.set('5187', '5187')
    w.tryPut(b4a.from('5187'), b4a.from('5187'))
    expected.delete('6891')
    w.tryDelete(b4a.from('6891'))
    expected.set('6345', '6345')
    w.tryPut(b4a.from('6345'), b4a.from('6345'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('7678', '7678')
    w.tryPut(b4a.from('7678'), b4a.from('7678'))
    expected.set('2785', '2785')
    w.tryPut(b4a.from('2785'), b4a.from('2785'))
    expected.delete('5312')
    w.tryDelete(b4a.from('5312'))
    expected.delete('5814')
    w.tryDelete(b4a.from('5814'))
    expected.set('2258', '2258')
    w.tryPut(b4a.from('2258'), b4a.from('2258'))
    expected.delete('7386')
    w.tryDelete(b4a.from('7386'))
    expected.set('1144', '1144')
    w.tryPut(b4a.from('1144'), b4a.from('1144'))
    expected.set('8025', '8025')
    w.tryPut(b4a.from('8025'), b4a.from('8025'))
    expected.set('1617', '1617')
    w.tryPut(b4a.from('1617'), b4a.from('1617'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('2146', '2146')
    w.tryPut(b4a.from('2146'), b4a.from('2146'))
    expected.set('7618', '7618')
    w.tryPut(b4a.from('7618'), b4a.from('7618'))
    expected.set('8774', '8774')
    w.tryPut(b4a.from('8774'), b4a.from('8774'))
    expected.set('4724', '4724')
    w.tryPut(b4a.from('4724'), b4a.from('4724'))
    expected.set('7585', '7585')
    w.tryPut(b4a.from('7585'), b4a.from('7585'))
    expected.set('5150', '5150')
    w.tryPut(b4a.from('5150'), b4a.from('5150'))
    expected.set('9347', '9347')
    w.tryPut(b4a.from('9347'), b4a.from('9347'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('5', '5')
    w.tryPut(b4a.from('5'), b4a.from('5'))
    expected.set('5748', '5748')
    w.tryPut(b4a.from('5748'), b4a.from('5748'))
    expected.set('1674', '1674')
    w.tryPut(b4a.from('1674'), b4a.from('1674'))
    expected.set('6368', '6368')
    w.tryPut(b4a.from('6368'), b4a.from('6368'))
    expected.set('8390', '8390')
    w.tryPut(b4a.from('8390'), b4a.from('8390'))
    expected.set('5020', '5020')
    w.tryPut(b4a.from('5020'), b4a.from('5020'))
    expected.set('4434', '4434')
    w.tryPut(b4a.from('4434'), b4a.from('4434'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('8053', '8053')
    w.tryPut(b4a.from('8053'), b4a.from('8053'))
    expected.set('299', '299')
    w.tryPut(b4a.from('299'), b4a.from('299'))
    expected.set('5620', '5620')
    w.tryPut(b4a.from('5620'), b4a.from('5620'))
    expected.set('6557', '6557')
    w.tryPut(b4a.from('6557'), b4a.from('6557'))
    expected.set('1819', '1819')
    w.tryPut(b4a.from('1819'), b4a.from('1819'))
    expected.set('5653', '5653')
    w.tryPut(b4a.from('5653'), b4a.from('5653'))
    expected.set('6919', '6919')
    w.tryPut(b4a.from('6919'), b4a.from('6919'))
    expected.delete('1902')
    w.tryDelete(b4a.from('1902'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('513', '513')
    w.tryPut(b4a.from('513'), b4a.from('513'))
    expected.set('2899', '2899')
    w.tryPut(b4a.from('2899'), b4a.from('2899'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('797', '797')
    w.tryPut(b4a.from('797'), b4a.from('797'))
    expected.delete('2641')
    w.tryDelete(b4a.from('2641'))
    expected.delete('1695')
    w.tryDelete(b4a.from('1695'))
    expected.set('5655', '5655')
    w.tryPut(b4a.from('5655'), b4a.from('5655'))
    expected.set('8422', '8422')
    w.tryPut(b4a.from('8422'), b4a.from('8422'))
    expected.set('4734', '4734')
    w.tryPut(b4a.from('4734'), b4a.from('4734'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.delete('6523')
    w.tryDelete(b4a.from('6523'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('9778', '9778')
    w.tryPut(b4a.from('9778'), b4a.from('9778'))
    expected.delete('38')
    w.tryDelete(b4a.from('38'))
    await w.flush()
  }

  db.cache.empty()

  {
    const w = db.write()
    expected.set('2197', '2197')
    w.tryPut(b4a.from('2197'), b4a.from('2197'))
    await w.flush()
  }

  const query = {
    gte: null,
    gt: null,
    lte: null,
    lt: null,
    reverse: false
  }

  const actual = []

  for await (const data of db.createReadStream(query)) {
    actual.push(b4a.toString(data.key))
  }

  t.alike(actual, [...expected.keys()].sort())
})

test('fuzz regression #5', async function (t) {
  const db = await create(t)

  const expected = new Map()

  {
    const w = db.write()
    expected.set('862', '862')
    w.tryPut(b4a.from('862'), b4a.from('862'))
    expected.set('3452', '3452')
    w.tryPut(b4a.from('3452'), b4a.from('3452'))
    expected.set('7204', '7204')
    w.tryPut(b4a.from('7204'), b4a.from('7204'))
    expected.set('9260', '9260')
    w.tryPut(b4a.from('9260'), b4a.from('9260'))
    expected.set('5547', '5547')
    w.tryPut(b4a.from('5547'), b4a.from('5547'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('7277', '7277')
    w.tryPut(b4a.from('7277'), b4a.from('7277'))
    expected.delete('6197')
    w.tryDelete(b4a.from('6197'))
    expected.set('5689', '5689')
    w.tryPut(b4a.from('5689'), b4a.from('5689'))
    expected.set('586', '586')
    w.tryPut(b4a.from('586'), b4a.from('586'))
    expected.set('1113', '1113')
    w.tryPut(b4a.from('1113'), b4a.from('1113'))
    expected.set('323', '323')
    w.tryPut(b4a.from('323'), b4a.from('323'))
    expected.delete('861')
    w.tryDelete(b4a.from('861'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('409', '409')
    w.tryPut(b4a.from('409'), b4a.from('409'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('1404', '1404')
    w.tryPut(b4a.from('1404'), b4a.from('1404'))
    expected.set('6690', '6690')
    w.tryPut(b4a.from('6690'), b4a.from('6690'))
    expected.set('2859', '2859')
    w.tryPut(b4a.from('2859'), b4a.from('2859'))
    expected.set('5', '5')
    w.tryPut(b4a.from('5'), b4a.from('5'))
    expected.set('9056', '9056')
    w.tryPut(b4a.from('9056'), b4a.from('9056'))
    expected.set('7865', '7865')
    w.tryPut(b4a.from('7865'), b4a.from('7865'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('5095', '5095')
    w.tryPut(b4a.from('5095'), b4a.from('5095'))
    expected.delete('6262')
    w.tryDelete(b4a.from('6262'))
    expected.set('7012', '7012')
    w.tryPut(b4a.from('7012'), b4a.from('7012'))
    expected.set('749', '749')
    w.tryPut(b4a.from('749'), b4a.from('749'))
    expected.set('9329', '9329')
    w.tryPut(b4a.from('9329'), b4a.from('9329'))
    expected.set('2844', '2844')
    w.tryPut(b4a.from('2844'), b4a.from('2844'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('4662', '4662')
    w.tryPut(b4a.from('4662'), b4a.from('4662'))
    expected.set('9516', '9516')
    w.tryPut(b4a.from('9516'), b4a.from('9516'))
    expected.set('6265', '6265')
    w.tryPut(b4a.from('6265'), b4a.from('6265'))
    expected.set('9669', '9669')
    w.tryPut(b4a.from('9669'), b4a.from('9669'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('1316', '1316')
    w.tryPut(b4a.from('1316'), b4a.from('1316'))
    expected.set('1255', '1255')
    w.tryPut(b4a.from('1255'), b4a.from('1255'))
    expected.set('9454', '9454')
    w.tryPut(b4a.from('9454'), b4a.from('9454'))
    expected.set('5449', '5449')
    w.tryPut(b4a.from('5449'), b4a.from('5449'))
    expected.set('9733', '9733')
    w.tryPut(b4a.from('9733'), b4a.from('9733'))
    expected.set('5386', '5386')
    w.tryPut(b4a.from('5386'), b4a.from('5386'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('374', '374')
    w.tryPut(b4a.from('374'), b4a.from('374'))
    expected.set('4625', '4625')
    w.tryPut(b4a.from('4625'), b4a.from('4625'))
    expected.set('8314', '8314')
    w.tryPut(b4a.from('8314'), b4a.from('8314'))
    expected.set('2268', '2268')
    w.tryPut(b4a.from('2268'), b4a.from('2268'))
    expected.set('5087', '5087')
    w.tryPut(b4a.from('5087'), b4a.from('5087'))
    expected.set('2098', '2098')
    w.tryPut(b4a.from('2098'), b4a.from('2098'))
    expected.set('4736', '4736')
    w.tryPut(b4a.from('4736'), b4a.from('4736'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('9857', '9857')
    w.tryPut(b4a.from('9857'), b4a.from('9857'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('7215', '7215')
    w.tryPut(b4a.from('7215'), b4a.from('7215'))
    expected.set('4762', '4762')
    w.tryPut(b4a.from('4762'), b4a.from('4762'))
    expected.set('3233', '3233')
    w.tryPut(b4a.from('3233'), b4a.from('3233'))
    expected.set('5265', '5265')
    w.tryPut(b4a.from('5265'), b4a.from('5265'))
    expected.delete('5639')
    w.tryDelete(b4a.from('5639'))
    expected.set('3157', '3157')
    w.tryPut(b4a.from('3157'), b4a.from('3157'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('2794', '2794')
    w.tryPut(b4a.from('2794'), b4a.from('2794'))
    expected.delete('2062')
    w.tryDelete(b4a.from('2062'))
    expected.set('4150', '4150')
    w.tryPut(b4a.from('4150'), b4a.from('4150'))
    expected.set('7955', '7955')
    w.tryPut(b4a.from('7955'), b4a.from('7955'))
    expected.set('9211', '9211')
    w.tryPut(b4a.from('9211'), b4a.from('9211'))
    expected.set('4110', '4110')
    w.tryPut(b4a.from('4110'), b4a.from('4110'))
    expected.set('1798', '1798')
    w.tryPut(b4a.from('1798'), b4a.from('1798'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('7960', '7960')
    w.tryPut(b4a.from('7960'), b4a.from('7960'))
    expected.delete('9003')
    w.tryDelete(b4a.from('9003'))
    expected.set('6801', '6801')
    w.tryPut(b4a.from('6801'), b4a.from('6801'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('3196', '3196')
    w.tryPut(b4a.from('3196'), b4a.from('3196'))
    expected.set('7021', '7021')
    w.tryPut(b4a.from('7021'), b4a.from('7021'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.delete('4580')
    w.tryDelete(b4a.from('4580'))
    expected.set('1395', '1395')
    w.tryPut(b4a.from('1395'), b4a.from('1395'))
    expected.set('3982', '3982')
    w.tryPut(b4a.from('3982'), b4a.from('3982'))
    expected.set('3505', '3505')
    w.tryPut(b4a.from('3505'), b4a.from('3505'))
    await w.flush()
  }
  {
    const w = db.write()
    expected.set('949', '949')
    w.tryPut(b4a.from('949'), b4a.from('949'))
    expected.delete('6740')
    w.tryDelete(b4a.from('6740'))
    expected.delete('8769')
    w.tryDelete(b4a.from('8769'))
    expected.set('8301', '8301')
    w.tryPut(b4a.from('8301'), b4a.from('8301'))
    expected.delete('7362')
    w.tryDelete(b4a.from('7362'))
    expected.set('4990', '4990')
    w.tryPut(b4a.from('4990'), b4a.from('4990'))
    expected.set('5961', '5961')
    w.tryPut(b4a.from('5961'), b4a.from('5961'))
    expected.delete('9667')
    w.tryDelete(b4a.from('9667'))
    await w.flush()
  }
  db.cache.empty()
  {
    const w = db.write()
    expected.set('1414', '1414')
    w.tryPut(b4a.from('1414'), b4a.from('1414'))
    await w.flush()
  }
  db.cache.empty()

  const query = {
    gte: null,
    gt: null,
    lte: null,
    lt: null,
    reverse: false
  }

  const actual = []
  const sorted = [...expected.values()].sort()

  for await (const data of db.createReadStream(query)) {
    actual.push(b4a.toString(data.key))
  }

  t.alike(actual, sorted)
})

test('fuzz regression #n', async function (t) {
  t.pass('left blank for next regression')
})

test('random fuzz (2k rounds)', async function (t) {
  t.timeout(120_000)

  const db = await create(t)
  let cnt = 0

  const expected = new Map()
  const log = []

  for (let i = 0; i < 2000; i++) {
    const n = (Math.random() * 10) | 0
    const w = db.write()
    log.push('{')
    log.push('  const w = db.write()')
    for (let j = 0; j < n; j++) {
      cnt++
      const put = Math.random() < 0.8
      const k = b4a.from('' + ((Math.random() * 10_000) | 0))
      if (put) {
        expected.set(k.toString(), k)
        log.push(`  expected.set('${k.toString()}', '${k.toString()}')`)
        log.push(`  w.tryPut(b4a.from('${k.toString()}'), b4a.from('${k.toString()}'))`)
        w.tryPut(k, k)
      } else {
        expected.delete(k.toString())
        log.push(`  expected.delete('${k.toString()}')`)
        log.push(`  w.tryDelete(b4a.from('${k.toString()}'))`)
        w.tryDelete(k)
      }
    }
    log.push('  await w.flush()')
    log.push('}')
    try {
      await w.flush()
    } catch (err) {
      dump()
      // makes it easier to flush if teeing....
      await new Promise((r) => setTimeout(r, 5_000))
      throw err
    }
  }

  const sorted = [...expected.values()].sort(b4a.compare)
  const actual = []

  for await (const data of db.createReadStream()) {
    actual.push(data.key)
  }

  if (!alike(actual, sorted, {})) {
    t.comment('ran ' + cnt + ' total ops')
    return
  }

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

    if (!alike(actual, expected, opts)) return
  }

  function alike(actual, expected, opts) {
    t.alike(actual, expected)

    if (b4a.equals(b4a.concat(expected), b4a.concat(actual))) return
    dump(opts)
  }

  function dump(opts = {}) {
    let s = 'const db = await create(t)\n\n'
    s += 'const expected = new Map()\n'
    s += '\n'
    s += log.join('\n')
    s += '\n'
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
    s += 't.alike(actual, [...expected.values()].sort())\n'
    console.log(s)
  }
})
