const Hyperschema = require('hyperschema')

const schema = Hyperschema.from('./spec/hyperschema', { versioned: false })
const bee = schema.namespace('bee')

bee.register({
  name: 'tree-pointer',
  compact: true,
  fields: [
    {
      name: 'core',
      type: 'uint',
      required: true
    },
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
  name: 'value-pointer',
  compact: true,
  fields: [
    {
      name: 'core',
      type: 'uint',
      required: true
    },
    {
      name: 'seq',
      type: 'uint',
      required: true
    },
    {
      name: 'offset',
      type: 'uint',
      required: true
    },
    {
      name: 'split',
      type: 'uint'
    }
  ]
})

bee.register({
  name: 'block-pointer',
  compact: true,
  fields: [
    {
      name: 'core',
      type: 'uint',
      required: true
    },
    {
      name: 'seq',
      type: 'uint',
      required: true
    }
  ]
})

bee.register({
  name: 'batch-pointer',
  compact: true,
  fields: [
    {
      name: 'start',
      type: 'uint',
      required: true
    },
    {
      name: 'end',
      type: 'uint',
      required: true
    }
  ]
})

bee.register({
  name: 'tree',
  compact: true,
  fields: [
    {
      name: 'keys',
      type: '@bee/tree-pointer',
      array: true,
      required: true
    },
    {
      name: 'children',
      type: '@bee/tree-pointer',
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
  name: 'key',
  compact: true,
  fields: [
    {
      name: 'key',
      type: 'buffer',
      required: true
    },
    {
      name: 'value',
      type: 'buffer'
    },
    {
      name: 'valuePointer',
      type: '@bee/value-pointer'
    }
  ]
})

bee.register({
  name: 'core',
  compact: true,
  fields: [
    {
      name: 'key',
      type: 'fixed32',
      required: true
    },
    {
      name: 'fork',
      type: 'uint'
    },
    {
      name: 'length',
      type: 'uint'
    },
    {
      name: 'treeHash',
      type: 'fixed32'
    }
  ]
})

bee.register({
  name: 'block-0',
  fields:  [
    {
      name: 'type',
      type: 'uint',
      required: true
    },
    {
      name: 'checkpoint',
      type: 'uint',
      required: true
    },
    {
      name: 'batch',
      type: '@bee/batch-pointer',
      required: true
    },
    {
      name: 'previous',
      type: '@bee/block-pointer'
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
    },
    {
      name: 'cores',
      type: '@bee/core',
      array: true
    }
  ]
})

bee.register({
  name: 'block-1',
  fields: [
    {
      name: 'type',
      type: 'uint',
      required: true
    },
    {
      name: 'checkpoint',
      type: 'uint',
      required: true
    },
    {
      name: 'batch',
      type: '@bee/batch-pointer',
      required: true
    },
    {
      name: 'previous',
      type: '@bee/block-pointer'
    },
    {
      name: 'cores',
      type: '@bee/core',
      array: true
    },
    {
      name: 'tree',
      type: '@bee/tree',
      array: true
    },
    {
      name: 'keys',
      type: '@bee/key',
      array: true
    },
    {
      name: 'values',
      type: 'buffer',
      array: true
    }
  ]
})

Hyperschema.toDisk(schema)
