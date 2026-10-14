const Hypercore = require('hypercore')
const b4a = require('b4a')
const c = require('compact-encoding')
const rrp = require('resolve-reject-promise')
const { getEncoding, getEnum } = require('../spec/hyperschema')
const { inflateValue } = require('./inflate.js')

const DownloadedKey = getEncoding('@bee/downloaded-key')
const { DOWNLOADED } = getEnum('@bee/local-key-type')

const MARKER_VALUE = b4a.from([1])
const MARKER_FLUSH = 4096

class Task {
  constructor(ptr, key, parent) {
    this.ptr = ptr
    this.key = key
    this.parent = parent
    this.pending = 0
    this.partial = false
  }
}

// walks the tree with up to `parallelism` fetches in flight, children in random order.
// fully walked subtrees are marked in local storage and skipped by later downloads.
class Downloader {
  constructor(tree, root, config, range) {
    const { parallelism = 1024, gte, gt, lte, lt } = range

    this.tree = tree
    this.root = root
    this.config = config
    this.parallelism = Math.max(1, parallelism)

    this.gte = gte || null
    this.gt = gte ? null : gt || null
    this.lte = lte || null
    this.lt = lte ? null : lt || null

    this.start = this.gte || this.gt
    this.end = this.lte || this.lt

    this.stack = []
    this.markers = []
    this.active = 0
    this.error = null
    this.resolve = null
    this.reject = null
  }

  async run() {
    const { promise, resolve, reject } = rrp()
    this.resolve = resolve
    this.reject = reject

    const [marked] = await this._marked([this.root])
    if (!marked) this.stack.push(new Task(this.root, null, null))

    this._pump()
    return promise
  }

  _pump() {
    while (this.error === null && this.active < this.parallelism && this.stack.length > 0) {
      const task = this.stack.pop()
      this._runTaskBackground(task.key !== null ? this._value(task) : this._node(task))
    }

    if (this.active > 0) return

    if (this.markers.length > 0) {
      this._runTaskBackground(this._flush())
      return
    }

    if (this.error !== null) this.reject(this.error)
    else this.resolve()
  }

  async _runTaskBackground(task) {
    this.active++

    try {
      await task
    } catch (err) {
      if (this.error === null) {
        this.error = err
        this.stack = []
        Hypercore.destroyRequests(this.config.activeRequests, err)
      }
    }

    this.active--
    this._pump()
  }

  async _value(task) {
    await inflateValue(task.key, this.config)
    this._done(task.parent)
  }

  async _node(task) {
    const ptr = task.ptr
    const v = ptr.value ? this.tree.bump(ptr) : await this.tree.inflate(ptr, this.config)

    const keys = v.keys
    const children = v.children

    const first = this.stack.length

    for (let i = 0; i < keys.length; i++) {
      const k = keys.get(i)
      if (!this._includesKey(k.key)) {
        task.partial = true
        continue
      }
      if (k.value || !k.valuePointer) continue
      this.stack.push(new Task(null, k, task))
      task.pending++
    }

    const ptrs = []

    for (let i = 0; i < children.length; i++) {
      if (!this._includesChild(keys, i)) {
        task.partial = true
        continue
      }
      ptrs.push(children.get(i))
    }

    const marked = await this._marked(ptrs)

    for (let i = 0; i < ptrs.length; i++) {
      if (marked[i]) continue
      this.stack.push(new Task(ptrs[i], null, task))
      task.pending++
    }

    shuffle(this.stack, first, this.stack.length)

    if (task.pending === 0) this._complete(task)
  }

  _done(task) {
    if (--task.pending === 0) this._complete(task)
  }

  _complete(task) {
    if (task.partial) {
      if (task.parent !== null) task.parent.partial = true
    } else {
      this.markers.push(task.ptr)
      if (this.markers.length >= MARKER_FLUSH) this._runTaskBackground(this._flush())
    }

    if (task.parent !== null) this._done(task.parent)
  }

  async _marked(ptrs) {
    const reads = new Map()
    const promises = new Array(ptrs.length)

    for (let i = 0; i < ptrs.length; i++) {
      const storage = await getStorage(ptrs[i])
      let rx = reads.get(storage)
      if (!rx) reads.set(storage, (rx = storage.read()))
      promises[i] = rx.getLocal(markerKey(ptrs[i]))
    }

    for (const rx of reads.values()) rx.tryFlush()

    const values = await Promise.all(promises)
    for (let i = 0; i < values.length; i++) values[i] = values[i] !== null
    return values
  }

  async _flush() {
    const markers = this.markers
    this.markers = []

    const writes = new Map()

    for (const ptr of markers) {
      const storage = await getStorage(ptr)
      let tx = writes.get(storage)
      if (!tx) writes.set(storage, (tx = storage.write()))
      tx.putLocal(markerKey(ptr), MARKER_VALUE)
    }

    for (const tx of writes.values()) await tx.flush()
  }

  _includesKey(key) {
    if (this.gte !== null && b4a.compare(key, this.gte) < 0) return false
    if (this.gt !== null && b4a.compare(key, this.gt) <= 0) return false
    if (this.lte !== null && b4a.compare(key, this.lte) > 0) return false
    if (this.lt !== null && b4a.compare(key, this.lt) >= 0) return false
    return true
  }

  // child i holds the keys between keys[i - 1] and keys[i]
  _includesChild(keys, i) {
    if (this.start !== null && i < keys.length) {
      if (b4a.compare(keys.get(i).key, this.start) <= 0) return false
    }
    if (this.end !== null && i > 0) {
      if (b4a.compare(keys.get(i - 1).key, this.end) >= 0) return false
    }
    return true
  }
}

async function getStorage(ptr) {
  const core = ptr.context.getCore(ptr.core)
  if (!core.opened) await core.ready()
  return core.core.state.storage
}

function markerKey(ptr) {
  return c.encode(DownloadedKey, { type: DOWNLOADED, seq: ptr.seq, offset: ptr.offset })
}

function shuffle(arr, start, end) {
  for (let i = end - 1; i > start; i--) {
    const j = start + Math.floor(Math.random() * (i - start + 1))
    const tmp = arr[i]
    arr[i] = arr[j]
    arr[j] = tmp
  }
}

module.exports = Downloader
