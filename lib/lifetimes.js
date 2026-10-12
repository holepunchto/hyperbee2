const ACTIVE = 1 // Lifetime is current
const ENDING = 2 // Lifetime has ended and is waiting for descendants to end
const SIGNALLING = 3 // Lifetime has ended and is running its callbacks
const ENDED = 4 // Lifetime has ended and has completed its callbacks

// A Lifetime is a generic way to represent the span of time a request
// should retry, or a resource should be cached, etc.
export class Lifetime {
  constructor(parent) {
    this._parent = parent // Lifetime this was forked from (if any)
    this._state = ACTIVE
  }

  isActive() {
    return this._state === ACTIVE
  }

  // Creates a new child lifetime that can be ended independently of this
  // (parent) lifetime but will automatically be ended if this one is.
  fork() {
    // Only allocate children array when required to avoid overhead
    // for leaf lifetimes.
    if (!this._children) {
      // NOTE: _children uses an array but order of child lifetimes is not
      // guaranteed to be stable. Calling parent._removeChild may alter it.
      this._children = []
    }
    const child = new Lifetime(this)
    this._children.push(child)
    // Forking a lifetime that has already ended results in a new child
    // lifetime that has also ended. NOTE: if state is ENDING, then the child
    // will be ended once the appropiate descendants have been ended. And if
    // the state is SIGNALLING all descendants of this have already been walked
    // and ended, so this one should be ended too.
    if (this._state === SIGNALLING || this._state === ENDED) {
        child._state = ENDED
    }
    return child
  }

  // Walks all descendants of this lifetime depth first.
  // Ends all descendants then ends this.
  end() {
    // If this is marked ended, it - or one of its ancestors - will have
    // already performed or be in the process of performing clean up.
    if (!this.isActive()) return

    this._state = ENDING

    // Detach lifetime from parent (if any).
    if (this._parent) {
      removeLastOf(this._parent._children, this)
      this._parent = null
    }

    let current = this
    while (current) {
      // Finds last leaf node in subtree
      while (current._children && current._children.length > 0) {
        current = current._children.pop()
      }

      while (current) {
        // Finished with current
        current._signal()

        // Backtrack up the tree to find next branch
        current = current._parent
        if (current && current._children && current._children.length > 0) {
          // Found next branch
          break
        }
      }
    }
  }

  // Take waiting callbacks and call them
  _signal() {
    // Currently running callbacks?
    if (this._state === SIGNALLING) return
    if (this._callbacks) {
      this._state = SIGNALLING
      while (true) {
        const cb = this._callbacks.pop()
        if (!cb) break
        cb(this)
      }
    }
    this._state = ENDED
  }

  // Add a function to be called after this lifetime ends.
  // NOTE: currently, calling wait() multiple times with the same callback will
  // call that callback multiple times. It will also require a corresponding number
  // of unwait() calls to completely remove the callback.
  wait(fn) {
    if (!this._callbacks) {
      this._callbacks = []
    }
    this._callbacks.push(fn)
    if (this._state === ENDED) {
      this._signal()
    }
    return this
  }

  // Remove a waiting callback.
  unwait(fn) {
    removeLastOf(this._callbacks, fn)
  }
}

function removeIndex(arr, index) {
  const len = arr.length
  for (let i = index; i < len - 1; i++) {
    arr[i] = arr[i + 1]
  }
  arr.length--
}

// Removes an item from an array, mutating the array. The order of
// remaining items is maintained. NOTE: this stops when the first match is found
// and does not search for duplicates.
function removeLastOf(arr, item) {
  const index = arr.lastIndexOf(item)
  if (index !== -1) {
    removeIndex(arr, index)
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

function noop() {}

// Ends a lifetime after a given timeout.
// Returns a function to cancel the timeout.
export function timeout(lifetime, ms) {
  if (!lifetime.ended) {
    const id = setTimeout(() => lifetime.end(), ms)
    // NOTE: clearTimeout is safe to call with a timeout ID that has already
    // triggered (in this case it does nothing).
    const cb = () => clearTimeout(id)
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

