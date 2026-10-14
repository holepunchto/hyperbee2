const Hypercore = require('hypercore')
const b4a = require('b4a')
const rrp = require('resolve-reject-promise')
const { inflateValue } = require('./inflate.js')

// walks the tree with up to `parallelism` fetches in flight, children in random order
class Downloader {
  constructor(tree, root, opts, config) {
    const { parallelism = 1024, gte, gt, lte, lt } = opts

    this.tree = tree
    this.config = config
    this.parallelism = Math.max(1, parallelism)

    this.gte = gte || null
    this.gt = gte ? null : gt || null
    this.lte = lte || null
    this.lt = lte ? null : lt || null

    this.start = this.gte || this.gt
    this.end = this.lte || this.lt

    this.nodes = [root]
    this.values = []
    this.active = 0
    this.error = null
    this.resolve = null
    this.reject = null
  }

  _pump() {
    while (this.error === null && this.active < this.parallelism) {
      let task = null

      if (this.nodes.length > 0) task = this._node(this.nodes.pop())
      else if (this.values.length > 0) task = this._value(this.values.pop())
      else break

      this.active++
      task.then(this._onDone.bind(this), this._onError.bind(this))
    }

    if (this.active > 0) return

    if (this.error !== null) this.reject(this.error)
    else this.resolve()
  }

  _onDone() {
    this.active--
    this._pump()
  }

  _onError(err) {
    this.active--
    if (this.error === null) {
      this.error = err
      this.nodes = []
      this.values = []
      Hypercore.destroyRequests(this.config.activeRequests, err)
    }
    this._pump()
  }

  async _value(key) {
    await inflateValue(key, this.config)
  }

  async _node(ptr) {
    const v = ptr.value ? this.tree.bump(ptr) : await this.tree.inflate(ptr, this.config)

    const keys = v.keys
    const children = v.children

    for (let i = 0; i < keys.length; i++) {
      const k = keys.get(i)
      if (k.value || !k.valuePointer) continue
      if (this._includesKey(k.key)) this.values.push(k)
    }

    const first = this.nodes.length

    for (let i = 0; i < children.length; i++) {
      if (!this._includesChild(keys, i)) continue
      this.nodes.push(children.get(i))
    }

    shuffle(this.nodes, first, this.nodes.length)
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

function shuffle(arr, start, end) {
  for (let i = end - 1; i > start; i--) {
    const j = start + Math.floor(Math.random() * (i - start + 1))
    const tmp = arr[i]
    arr[i] = arr[j]
    arr[j] = tmp
  }
}

module.exports = function download(tree, root, opts, config) {
  const d = new Downloader(tree, root, opts, config)
  const { promise, resolve, reject } = rrp()
  d.resolve = resolve
  d.reject = reject
  d._pump()
  return promise
}
