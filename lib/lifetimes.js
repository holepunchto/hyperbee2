function noop() {}

// Ends a lifetime after a given timeout.
// Returns a function to cancel the timeout.
export function timeout(lifetime, ms) {
  if (!lifetime.ended) {
    let timer = setTimeout(() => lifetime.end(), ms)
    // NOTE: clearTimeout is safe to call with a timeout ID that has already
    // triggered (in this case it does nothing).
    const cb = () => clearTimeout(timer)
    // Clear timeout if lifetime ends.
    lifetime.wait(cb)
    // Return function to cancel the timeout on this lifetime.
    return () => {
      cb()
      lifetime.unwait(cb)
    }
  }
  return noop
}

// The span of time a request should be retried or a resource should live
// for etc.
export class Lifetime {
  constructor(parent, index) {
    this._parent = parent
    this._index = index
    this.ended = false
  }

  // Creates a new child lifetime that can be ended independently of this
  // (parent) lifetime but will automatically be ended if this one is.
  fork() {
    if (this.ended) {
      throw new Error('Execution lifetime has ended')
    }
    // Only allocate children array when required to avoid overhead
    // for leaf lifetimes.
    if (!this._children) {
      // NOTE: _children uses an array but order of child lifetimes is not
      // guaranteed to be stable. Calling parent._removeChild may alter it.
      this._children = []
    }
    const index = this._children.length
    const child = new Lifetime(this, index)
    this._children[index] = child
    return child
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

  // NOTE: this may reorder the _children array so it cannot be done
  // during a walk of the descendant lifetimes tree.
  _removeChild(index) {
    let removed = this._children.pop()
    if (index !== this._children.length) {
      let tmp = removed
      removed = this._children[index]
      this._children[index] = tmp
      tmp._index = index
    }
    removed._parent = null
  }

  // Ends all descendants then ends this. The function is idempotent.
  end() {
    // If this is marked ended, it - or one of its ancestors - will have
    // already performed or be in the process of performing clean up.
    if (this.ended) return

    // Detach lifetime from parent (if any).
    if (this._parent) {
      this._parent._removeChild(this._index)
    }

    // Mark all descendants (and this lifetime itself) as ended.
    // This will prevent _removeChild() running on any descendant
    // lifetimes (see NOTE for _removeChild()).
    this._withDescendants((lifetime) => {
      lifetime.ended = true
    })

    // After all relevant lifetimes are marked THEN call any
    // callbacks that are waiting on them. This means two walks of the
    // tree but it ensures we don't run callbacks while descendant
    // lifetimes are in a mix of ended/alive states.
    this._withDescendants((lifetime) => lifetime._signal())
  }

  // Take waiting callbacks (leaving empty array) and call them.
  _signal() {
    if (this._waiting) {
      const waiting = this._waiting
      this._waiting = null
      for (const callback of waiting) {
        callback(this)
      }
    }
  }

  // Add a function to be called after this lifetime ends. If the lifetime
  // has already ended, the function is called immediately.
  wait(callback) {
    if (this.ended) {
      // TODO: should this be asynchronous like Promise.then()?
      callback(this)
    } else {
      if (!this._waiting) {
        // Only allocate waiting array when required to avoid its overhead
        // for intermediate lifetimes in tree with no waiting callbacks.
        this._waiting = []
      }
      this._waiting.push(callback)
    }
  }

  // Remove a waiting callback.
  unwait(callback) {
    // TODO: is it possible to unwait during a cleanup? will the callback still be called?
    // TODO: must this function maintain the order of waiting callbacks?
    // See NOTE in removeFromArray()
    removeFromArray(this._waiting, callback)
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

// A lifetime that lives at least as long as its attached lifetimes.
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

  // Ensures this shared lifetime lives at least as long as the given lifetime.
  // NOTE: attached lifetimes MUST detach to be garbage collected - either by
  // calling detach(lifetime) or by ending the attached lifetime.
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

  // Removes requirement to live as long as given lifetime.
  detach(lifetime) {
    // If lifetime has already ended, it will be in the process of cleaning up.
    // If not, it may yet live a long time and we should remove the useless callback.
    if (!lifetime.ended) {
      lifetime.unwait(this._onLifetimeEnd)
    }
    this._attached.delete(lifetime)
    if (this._attached.size === 0) {
      this.end()
    }
  }

  // End this lifetime now without waiting for attached lifetimes to end first.
  // WARNING: usually this is not called directly as it may invalidate
  // a resource that is still in use by attached lifetimes.
  end() {
    // Remove callbacks referencing this lifetime from all attached lifetimes
    // so it can be garbage collected.
    const attached = this._attached
    // No more lifetimes can be attached after this one has ended so no
    // need to allocate an empty Set().
    this._attached = null
    for (const lifetime of attached) {
      lifetime.unwait(this._onLifetimeEnd)
    }
    // Call waiting callbacks on this lifetime (and its descendants, if any)
    super.end()
  }
}

// Hold a reference for as long as the attached lifetimes live then call
// its destructor.
export class SharedResource {
  constructor(initialOwner, value, destructor) {
    this.value = value
    this.lifetime = new SharedLifetime(initialOwner)
    this.lifetime.wait(destructor)
  }

  // Resource should live at least as long as given lifetime
  attach(lifetime) {
    this.lifetime.attach(lifetime)
    return this
  }

  // Remove requirement to live at least as long as given lifetime
  detach(lifetime) {
    this.lifetime.detach(lifetime)
  }

  // Calls destructor now without waiting for attached lifetimes to end.
  // WARNING: usually this is not called directly as it may invalidate
  // a resource that is still in use by attached lifetimes.
  destroy() {
    this.lifetime.end()
  }
}
