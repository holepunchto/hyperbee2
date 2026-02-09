# hyperbee2

```
npm install hyperbee2
```

Next major version for [hyperbee](https://github.com/holepunchto/hyperbee).
Will be merged in there and released a new major when fully done.

An append-only B-tree on top of a [Hypercore][hypercore].

## Usage

```js
const Hyperbee2 = require('hyperbee2')

const db = new Hyperbee2(store)

// see tests for more
```

## License

Apache-2.0

## API

#### const db = new Hyperbee(store, [options])

Make a new Hyperbee2 instance. The `store` argument expects a
[Corestore][corestore].

Options include:

```js
{
  key: 'buffer | string',  // Key of Hypercore to load via Corestore
  maxCacheSize: '4096',    // Max number of nodes to keep in NodeCache
  core: 'Hypercore',       // Hypercore within the Corestore to use (defaults to loading key, or using name='bee')
  view: 'boolean',         // Is this a view of an open Hyperbee? (i.e. do not close underlying store)
  writable: 'boolean',     // Is append / truncate allowed on the underlying Hypercore?
  unbatch: 'integer',      // Number of write batches to rollback during bootstrap
  autoUpdate: 'boolean',   // Reload root node when underlying Hypercore is appended to?
  preload: 'function',     // A function called by ready() after the Hypercore is ready. Can be async.
  wait: 'boolean',         // Whether to wait for Hypercore to replicate data
}
```

#### await db.ready()

Ensures the underlying [Hypercore][hypercore] is ready and prepares the Hyperbee
for use. If `autoUpdate` was set to true in the constructor's options,
this will start watching the [Hypercore][hypercore] for new writes.

Calling `get()` or `write()` will call this automatically for you.

#### await db.close()

Fully close the Hyperbee. If it is not a view on another Hyperbee,
this will also close it's [Hypercore][hypercore].

#### db.head()

Returns an object with the following properties:

```
{
  length: 'integer',  // Number of blocks from the start of Hypercore that apply to this tree
  key: 'buffer',      // The key of the underlying Hypercore
}
```

If the Hyperbee is not ready, this will return null.

#### db.cache

Read only. The NodeCache used by this Hyperbee.

#### db.core

Read only. The [Hypercore][hypercore] used by this Hyperbee.

#### db.opening

Read only. A Promise that resolves to `undefined` once the [Corestore][corestore]
is ready.

#### db.closing

Read only. Initially null. When `db.close()` is called, this is set to
a Promise that resolves to `undefined` when the close completes.

#### db.opened

Read only. Boolean indicating whether the [Corestore][corestore] has opened.

#### db.closed

Read only. Boolean indicating whether the [Corestore][corestore] has closed.

#### db.replicate()

Calls `replicate` on underlying [Corestore][corestore].

#### db.checkout([options])

Returns a new Hyperbee as a view on the underlying [Hypercore][hypercore].

Options:

```js
{
  writable: 'boolean',   // Whether the new tree will be writable
  length: 'integer',     // Length of blocks used from the Hypercore
  key: 'null | Buffer',  // Key of the Hypercore
}
```

#### db.move([options])

Replaces the underlying [Hypercore][hypercore] for this Hyperbee.

Options:

```js
{
  writable: 'boolean',  // Whether this tree is writable after the move
  length: 'integer',    // Length of blocks used from the Hypercore
  key: 'null | Buffer', // Key of the Hypercore
}
```

#### db.snapshot()

Returns a new Hyperbee that is a read only view of the current tree.

#### db.undo(n)

Returns a new Hyperbee that is a writable view of the current tree,
with the last `n` write batches ignored.

#### db.write([options])

Returns a WriteBatch object through which the tree can be updated.

Options:

```js
{
  length: 'integer',              // Length of blocks used from the Hypercore
                                  // (i.e. what point in the hypercore is the write
                                  // going to extend when flush() is called?).
  key: 'null | Buffer',           // Key of the Hypercore.
  autoUpdate: 'boolean',          // Whether the Hyperbee should automatically reflect
                                  // updates after each flush().
  compat: 'boolean',              // Write blocks compatible with Hyperbee 1?
  type: 'integer',                // Block format to use (defaults to TYPE_LATEST).
  inlineValueSize: 'integer',     // Values smaller than this byte length are
                                  // written inline in the node. Larger values
                                  // are referenced via a pointer into the block.
  preferredBlockSize: 'integer',  // Try to write blocks of approximately this size
                                  // when flushing updates.
```

Errors:

- If the tree is not writable this will throw an Error

#### db.createReadStream([options])

#### db.createDiffStream(right, [options])

#### db.createChangesStream([options])

#### await db.peek([range])

#### await db.download([range])

#### await db.get(key, [options])

[hypercore]: https://github.com/holepunchto/hypercore
[corestore]: https://github.com/holepunchto/corestore
