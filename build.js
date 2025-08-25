const Hyperschema = require('hyperschema')

const schema = Hyperschema.from('./spec/hyperschema', { versioned: false })
const bee = schema.namespace('bee')

bee.register({
  name: 'pointer',
  compact: true,
  fields: [
    {
      name: 'seq',
      type: 'uint',
      required: true
    },
    {
      name: 'offset',
      type: 'uint',
      required: true
    }
  ]
})

bee.register({
  name: 'tree',
  fields: [
    {
      name: 'keys',
      type: '@bee/pointer',
      array: true,
      required: true
    },
    {
      name: 'children',
      type: '@bee/pointer',
      array: true,
      required: true
    }
  ]
})

bee.register({
  name: 'data',
  compact: true,
  fields: [
    {
      name: 'key',
      type: 'buffer',
      required: true
    },
    {
      name: 'value',
      type: 'buffer',
      required: true
    }
  ]
})

bee.register({
  name: 'block',
  fields: [
    {
      name: 'type',
      type: 'uint',
      required: true
    },
    {
      name: 'batch',
      type: 'uint',
      required: true
    },
    {
      name: 'tree',
      type: '@bee/tree',
      array: true
    },
    {
      name: 'data',
      type: '@bee/data',
      array: true
    }
  ]
})

Hyperschema.toDisk(schema)
