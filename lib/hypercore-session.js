const Hypercore = require('hypercore')

// Stores Hypercore config and tracks in-flight Hypercore requests. A
// single HypercoreSession may be used with multiple Hypercore instances.
class HypercoreSession {
  constructor({ timeout = 0, wait = true }, parent = null, parentIndex = 0) {
    // If this was forked from another session, store a reference to the
    // original here. This allows the session to be removed from the parent
    // when it is destroyed.
    this.parent = parent
    // Index of this session in the parent's children Array.
    this.parentIndex = parentIndex

    // Child sessions forked from this one. These will be destroyed when
    // this session is destroyed.
    this.children = []

    // Stores in-flight requests being handled by Hypercore so they can be
    // cancelled when this session is destroyed. This Array may be shared
    // across multiple Hypercore instances - with multiple Replicators
    // accessing it at once - and is sometimes referred to as a Hypercore
    // 'session'.
    this.activeRequests = []

    // Base Hypercore.get() options
    this.timeout = timeout
    this.wait = wait
  }

  // Returns a parameterized client to hypercore.
  client(options = {}) {
    const { timeout = this.timeout, wait = this.wait } = options
    return new HypercoreClient(this, { timeout, wait })
  }

  // Cancels in-flight requests for all Hypercores using this session.
  destroy(err = null, cascading = false) {
    // Remove child sessions from their parent (unless parent was already
    // destroyed and this is a cascading destroy).
    if (!cascading && this.parent) {
      if (this.parent.children[this.parentIndex] === this) {
        const last = this.parent.children.pop()
        if (last !== this) {
          this.parent.children[this.parentIndex] = last
          last.parentIndex = this.parentIndex
        }
      }
    }
    // Take children Array and call destroy on them.
    const toDestroy = this.children
    this.children = []
    for (const c of toDestroy) {
      c.destroy(err, true)
    }
    // Cancel Hypercore requests from this instance.
    Hypercore.destroyRequests(this.activeRequests, err)
  }

  // Returns a new child session of this session. It inherits
  // this session's config and will be destroyed when this session
  // is destroyed.
  fork() {
    const child = new HypercoreSession(
      {
        timeout: this.timeout,
        wait: this.wait,
      },
      this,
      this.children.length
    )
    this.children.push(child)
    return child
  }

  // Forks this session, executes a function, then destroys the fork.
  async withFork(fn) {
    const child = this.fork()
    let err = null
    try {
      await fn(child)
    } catch (e) {
      err = e
    } finally {
      child.destroy(err)
    }
  }

  // Returns a new HypercoreSession with the same config options but a new
  // activeRequests Array. The new detached session has its own lifecycle
  // and will not be destroyed when this session is destroyed.
  detach(options) {
    return new HypercoreSession(
      Object.assign(
        {
          timeout: this.timeout,
          wait: this.wait,
        },
        options
      )
    )
  }
}

// A parameterized client to hypercore using the given HypercoreSession. A
// single HypercoreClient can be used with multiple Hypercore instances and
// they will share a list of in-flight requests (activeRequests) and default
// config options.
class HypercoreClient {
  constructor(session, { timeout, wait }) {
    this.session = session
    this.timeout = timeout
    this.wait = wait
  }

  _baseGetOptions() {
    return { timeout: this.timeout, wait: this.wait }
  }

  _makeOptions(...options) {
    const opt = Object.assign(...options)
    opt.activeRequests = this.activeRequests
    return opt
  }

  // Calls same method on the given Hypercore using this sessions config
  // and activeRequests.
  async get(core, index, options = {}) {
    const opts = this._makeOptions(this._baseGetOptions(), options)
    return await core.get(index, opts)
  }

  // Calls same method on the given Hypercore using this session's config
  // and activeRequests.
  download(core, range) {
    return core.download(this._makeOptions(range))
  }
}

exports.HypercoreSession = HypercoreSession
