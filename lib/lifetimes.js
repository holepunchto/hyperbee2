export class Lifetime {
  constructor(parent) {
    this._waiting = []
    this._parent = parent
    this.ended = false
  }

  // Creates a new child lifetime that can be ended independently of
  // this parent but will automatically be ended if the parent is.
  fork() {
    if (this.ended) {
      throw new Error('Execution lifetime has ended')
    }
    // Only allocate children array when required to avoid overhead
    // for leaf lifetimes.
    if (!this._children) {
      this._children = []
    }
    const child = new Lifetime(this)
    this._children.push(child)
    return child
  }

  timeout(ms) {
    if (!this.ended) {
      this.clearTimeout()
      this._timeout = setTimeout(() => this.end(), ms)
    }
    return this
  }

  clearTimeout() {
    if (this._timeout) {
      clearTimeout(this._timeout)
      this._timeout = null
    }
  }

  // Walks all descendants of this lifetime depth first.
  _withDescendants(callback) {
    const indices = []
    let current = this
    let i = 0

    while (true) {
      // Finds first leaf node in subtree
      while (current._children && current._children.length > 0) {
        indices[i] = 0
        current = current._children[0]
        i++
      }

      while (true) {
        // Finished with current
        callback(current)

        // Backtracks up the tree to find next branch
        if (--i < 0) {
          // No more branches to explore
          return
        }
        current = current._parent
        if (++indices[i] < current._children.length) {
          // Found next branch
          break
        }
      }

      // Step into branch
      current = current._children[indices[i]]
      i++
    }
  }

  // Ends all descendants then ends this. The function is idempotent.
  end() {
    // If this is marked ended, it - or one of its ancestors - will have
    // already performed or be in the process of performing clean up.
    if (this.ended) return

    // Detach lifetime from parent (if any)
    if (this._parent) {
      removeFromArray(this._parent._children, this)
      this._parent = null
    }

    // Mark all descendants (and this lifetime itself) as ended
    this._withDescendants((lifetime) => {
      lifetime.ended = true
    })

    // After all relevant lifetimes are marked THEN call any
    // callbacks that are waiting on them. This means two walks of the
    // tree but it ensures we don't run callbacks while descendant
    // lifetimes are in a mix of ended/alive states.
    this._withDescendants((lifetime) => lifetime._signal())
  }

  // Add a function to be called after this lifetime ends. If the lifetime
  // has already ended, the function is called immediately.
  wait(callback) {
    if (this.ended) {
      // TODO: should this be asynchronous like Promise.then()?
      callback(this)
    } else {
      this._waiting.push(callback)
    }
  }

  // Remove a waiting callback.
  unwait(callback) {
    // TODO: must this function maintain the order of waiting callbacks?
    // See NOTE in removeFromArray()
    removeFromArray(this._waiting, callback)
  }

  // Cancels timeout and calls all waiting callbacks.
  _signal() {
    this.clearTimeout()
    for (const callback of this._waiting) {
      callback(this)
    }
  }
}

// Removes an item from an array, mutating the array. The order of
// remaining items is maintained.
function removeFromArray(arr, item) {
  // Performs a simple scan on the assumption there will be a relatively
  // small number of entries.
  for (let i = 0, len = arr.length; i < len; i++) {
    if (arr[i] === item) {
      // NOTE: this maintains entries in the order they were added
      // so that e.g. callbacks are called in a predictable order.
      // But, if required, we could drop that guarantee and do
      // pop + swap here for better performance.
      arr.splice(i, 1)
      return
    }
  }
}

export class SharedLifetime extends Lifetime {
  _attached = new Set()

  constructor(initial) {
    super()

    // Keep reference to this callback so we can unwait() with it later
    this._onLifetimeEnd = this.detach.bind(this)

    if (initial.ended) {
      this.end()
    } else {
      this.attach(initial)
    }
  }

  attach(lifetime) {
    if (this.ended) {
      throw new Error('Shared lifetime has already ended')
    }
    if (lifetime.ended) {
      // Makes no difference this this lifetime
      return
    }
    // Add to _attached first because wait() might call its callback
    // synchronously and _attached needs to be the correct size for
    // ref counting.
    this._attached.add(lifetime)
    // Detach lifetime from resource when it ends.
    lifetime.wait(this._onLifetimeEnd)
  }

  detach(lifetime) {
    if (!lifetime.ended) {
      // Remove callback or it might mess up ref counting
      lifetime.unwait(this._onLifetimeEnd)
    }
    this._attached.delete(lifetime)
    if (this._attached.size === 0) {
      this.end()
    }
  }

  end() {
    // Remove callbacks for all lifetimes referencing this one
    // so it can be garbage collected.
    for (const lifetime of this._attached) {
      lifetime.unwait(this._onLifetimeEnd)
    }
    super.end()
  }
}

export class SharedResource {
  constructor(initialOwner, value, cleanup) {
    this.value = value
    this.lifetime = new SharedLifetime(initialOwner)
    this.lifetime.wait(cleanup)
  }

  attach(lifetime) {
    this.lifetime.attach(lifetime)
    return this
  }

  detach(lifetime) {
    this.lifetime.detach(lifetime)
  }

  destroy() {
    this.lifetime.end()
  }
}
